package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func nativeWorkflowTimerOwnerActivationForTest(t *testing.T, backend string, recurring bool, delay string, created time.Time, register bool, mode executionmode.Mode, open pipelineDeliveryNativeOpenerForTest) (*PipelineDeliveryNativeFixtureForTest, *PipelineCoordinator, context.Context, *nativePipelineDeliveryBusObservationForTest, string, WorkflowTimerActivation) {
	t.Helper()
	bundle := workflowTimerOwnerBundleWithDelay(t, recurring, delay)
	return nativeWorkflowTimerSourceActivationForTest(t, backend, bundle, created, register, mode, open)
}

func nativeWorkflowTimerSourceActivationForTest(t *testing.T, backend string, bundle *contracts.WorkflowContractBundle, created time.Time, register bool, mode executionmode.Mode, open pipelineDeliveryNativeOpenerForTest) (*PipelineDeliveryNativeFixtureForTest, *PipelineCoordinator, context.Context, *nativePipelineDeliveryBusObservationForTest, string, WorkflowTimerActivation) {
	t.Helper()
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
	ctx = effects.WithExecutionMode(ctx, mode)
	bus := observeNativePipelineDeliveryBusForTest(t, pc)
	// The timer lifecycle captures these ports during construction. Observe
	// them before activation or firing, retaining the original native delegate.
	pc.workflowTimers.publication = bus
	pc.workflowTimers.dispatcher = bus.EngineDispatcher()
	pc.workflowTimers.logger = bus
	entityID := correlation.RunIDFromContext(ctx)
	createdAt := canonicalWorkflowTimerTime(created)
	instance := materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
		InstanceID: entityID, StorageRef: entityID, EntityID: entityID, WorkflowName: ".",
		WorkflowVersion: bundle.WorkflowVersion(), CurrentState: "waiting", EnteredStageAt: createdAt,
		CreatedAt: createdAt, EntityType: "test_entity",
	})
	if err := fixture.Construct(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if register {
		pc.timerScheduler = newWorkflowTimerTestScheduler(t, pc.workOwner)
		if err := pc.workflowTimers.bindScheduler(pc.timerScheduler); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := pc.workflowTimers.stop(join); err != nil {
			t.Error(err)
		}
	})
	if err := reconcileWorkflowTimerForTest(ctx, pc, workflowTimerRootRoute(ctx), entityID, "", "waiting", workflowTimerCause{
		Kind: workflowTimerCauseInitial, OccurredAt: createdAt, ToState: "waiting", ExecutionMode: mode,
	}); err != nil {
		t.Fatalf("activate native workflow timer: %v", err)
	}
	activations := listWorkflowTimerOwnerActivations(t, pc.workflowStore, ctx, entityID, true)
	if len(activations) != 1 {
		t.Fatalf("native active workflow timers = %d, want one: %#v", len(activations), activations)
	}
	return fixture, pc, ctx, bus, entityID, activations[0]
}

func cancelNativeWorkflowTimerForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context, pc *PipelineCoordinator, activation WorkflowTimerActivation) error {
	t.Helper()
	attempt, _, release := fixture.AdmitAttachment(ctx, activation.RunID, activation.Route.InstancePath)
	defer release()
	committed, err := pc.workflowStore.timerActivations.CommitWorkflowTimerReconciliation(ctx, WorkflowTimerReconciliationCommand{
		RunID: activation.RunID, Route: activation.Route, EntityID: activation.EntityID,
		ActivationAttempt: &attempt,
		Plan:              WorkflowLifecycleMutationPlan{Timers: []WorkflowTimerMutation{{Kind: WorkflowTimerMutationCancel, Activation: activation}}},
	})
	if err != nil {
		return err
	}
	return pc.finalizeWorkflowLifecycleMutation(ctx, committed)
}

func VerifyNativeWorkflowTimerFaultsRequireExactActivationOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, faultName := range []string{"missing", "foreign", "malformed", "foreign_route_malformed"} {
			t.Run(backend+"/"+faultName, func(t *testing.T) {
				fixture, pc, ctx, _, _, activation := nativeWorkflowTimerOwnerActivationForTest(t, backend, false, "1h", time.Now(), false, executionmode.Live, open)
				fault := map[string]func(context.Context, string, string) error{
					"missing": fixture.RemoveTimer, "foreign": fixture.SetTimerForeignDeclaration,
					"malformed": fixture.SetTimerMalformedName, "foreign_route_malformed": fixture.SetTimerForeignRouteMalformedName,
				}[faultName]
				for _, key := range [][2]string{{uuid.NewString(), activation.Ref.ActivationID}, {activation.RunID, uuid.NewString()}, {"", activation.Ref.ActivationID}, {activation.RunID, ""}} {
					if err := fault(ctx, key[0], key[1]); err == nil {
						t.Fatalf("fault accepted absent or foreign activation %q/%q", key[0], key[1])
					}
				}
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if err := fault(cancelled, activation.RunID, activation.Ref.ActivationID); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled timer fault error=%v", err)
				}
				observed := loadWorkflowTimerOwnerActivation(t, pc.workflowStore, ctx, activation.Ref.ActivationID)
				if !reflect.DeepEqual(observed, activation) {
					t.Fatalf("refused fault changed native activation: before=%+v after=%+v", activation, observed)
				}
				if fixture.Transactions().Active != 0 {
					t.Fatal("refused timer fault did not join its original transaction")
				}
			})
		}
	}
}
