package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type forkedPipelineBackend struct {
	store          *workflowInstanceStore
	native         WorkflowActivityNativeFixtureForTest
	ctx            context.Context
	runID          string
	continuedRunID string
	frozenAt       time.Time
}

func newForkedPipelineBackend(t *testing.T, backend string, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) forkedPipelineBackend {
	t.Helper()
	runID := uuid.NewString()
	continuedRunID := uuid.NewString()
	native := open(t, backend)
	ctx := effects.WithExecutionMode(runtimecorrelation.WithRunID(native.Context, runID), executionmode.Live)
	for _, run := range []string{runID, continuedRunID} {
		if err := native.RequireRun(ctx, run); err != nil {
			t.Fatal(err)
		}
	}
	frozenAt := time.Now().UTC().Truncate(time.Microsecond)
	return forkedPipelineBackend{
		store: native.Persistence.store, native: native, ctx: ctx,
		runID: runID, continuedRunID: continuedRunID, frozenAt: frozenAt,
	}
}

func (b forkedPipelineBackend) freeze(t *testing.T) {
	t.Helper()
	if _, _, err := b.native.Runs.ForkRunSource(b.ctx, storerunlifecycle.ForkSourceRequest{
		RunID: b.runID, ContinuedAsRunID: b.continuedRunID, EndedAt: b.frozenAt,
	}); err != nil {
		t.Fatal(err)
	}
}

func requireForkedPipelineRefusal(t *testing.T, label string, err error) {
	t.Helper()
	if !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
		t.Fatalf("%s error = %v, want run-not-active", label, err)
	}
}

func VerifyForkedSourceWorkflowInstanceMutationsRefuseAndPreserveReadbackForTest(t *testing.T, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedPipelineBackend(t, backend, open)
			instanceID := uuid.NewString()
			storageRef := "freeze/" + instanceID
			entityID := uuid.NewString()
			instance := WorkflowInstance{
				InstanceID: instanceID, StorageRef: storageRef, EntityID: entityID, WorkflowName: "freeze", WorkflowVersion: "1",
				CurrentState: "active", EnteredStageAt: fixture.frozenAt.Add(-time.Minute),
				Fields:     map[string]any{"marker": "source"},
				EntityType: "test_entity",
			}
			instance = materializedWorkflowInstanceForTest(instance)
			if err := fixture.native.Construct(fixture.ctx, instance); err != nil {
				t.Fatal(err)
			}
			fixture.freeze(t)

			late := instance
			late.CurrentState = "changed"
			requireForkedPipelineRefusal(t, "replace existing workflow", fixture.native.Construct(fixture.ctx, late))
			late.InstanceID = uuid.NewString()
			late.StorageRef = "freeze/" + late.InstanceID
			late.Fields = cloneStringAnyMap(late.Fields)
			requireForkedPipelineRefusal(t, "create workflow", fixture.native.Construct(fixture.ctx, late))
			requireForkedPipelineRefusal(t, "mutate workflow", fixture.store.mutate(fixture.ctx, testRunScopedWorkflowInstanceForRun(fixture.runID, storageRef), func(item *WorkflowInstance) { item.CurrentState = "changed" }))
			requireForkedPipelineRefusal(t, "mutate workflow with error", fixture.store.mutateE(fixture.ctx, testRunScopedWorkflowInstanceForRun(fixture.runID, storageRef), func(item *WorkflowInstance) error {
				item.CurrentState = "changed"
				return nil
			}))
			coordinator := &PipelineCoordinator{workflowStore: fixture.store}
			requireForkedPipelineRefusal(t, "terminate workflow", coordinator.MarkTerminated(fixture.ctx, testRunScopedWorkflowInstanceForRun(fixture.runID, storageRef), identity.NormalizeEntityID(entityID), fixture.frozenAt))

			preserved, ok, err := fixture.store.Load(fixture.ctx, testRunScopedWorkflowInstanceForRun(fixture.runID, storageRef))
			if err != nil || !ok || preserved.CurrentState != "active" {
				t.Fatalf("preserved workflow = %#v found=%v err=%v", preserved, ok, err)
			}
		})
	}
}

func VerifyForkedSourceActivityAttemptMutationsRefuseAndPreserveJournalForTest(t *testing.T, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedPipelineBackend(t, backend, open)
			intent := testNonIdempotentActivityIntent(fixture.runID, uuid.NewString(), uuid.NewString())
			start := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
			started, inserted, err := fixture.store.StartActivityAttempt(fixture.ctx, start)
			if err != nil || !inserted {
				t.Fatalf("start activity before freeze = %#v inserted=%v err=%v", started, inserted, err)
			}
			fixture.freeze(t)

			lateIntent := testNonIdempotentActivityIntent(fixture.runID, uuid.NewString(), uuid.NewString())
			lateStart := activityAttemptStartRecord(lateIntent, activityInputHash(lateIntent.Input))
			_, _, err = fixture.store.StartActivityAttempt(fixture.ctx, lateStart)
			requireForkedPipelineRefusal(t, "start activity", err)
			_, _, err = fixture.store.ClaimActivityAttemptForLoopGeneration(fixture.ctx, lateStart)
			requireForkedPipelineRefusal(t, "claim activity", err)

			success := started.withTerminal(
				ActivityAttemptStatusSucceeded,
				activityResultEventID(intent, intent.SuccessEvent),
				intent.SuccessEvent,
				activitySuccessPayload(intent, map[string]any{"ok": true}),
				nil,
			)
			_, _, err = fixture.store.CompleteActivityAttempt(fixture.ctx, success)
			requireForkedPipelineRefusal(t, "complete activity", err)
			failure := runtimefailures.Normalize(errors.New("provider outcome is unknown"), "pipeline-test", "freeze_activity")
			uncertain := started.withTerminal(
				ActivityAttemptStatusUncertain,
				uuid.NewString(),
				intent.FailureEvent,
				map[string]any{"uncertain": true},
				&failure,
			)
			_, _, err = fixture.store.MarkActivityAttemptUncertain(fixture.ctx, uncertain)
			requireForkedPipelineRefusal(t, "mark activity uncertain", err)

			preserved, ok, err := fixture.store.LoadActivityAttempt(fixture.ctx, started.RequestEventID)
			if err != nil || !ok || preserved.Status != ActivityAttemptStatusStarted {
				t.Fatalf("preserved activity = %#v found=%v err=%v", preserved, ok, err)
			}
		})
	}
}
