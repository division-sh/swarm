package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

type WorkflowTimerCauseReplayStorageForTest struct {
	Timers, ActiveTimers, TimerRevisionFacts, Events int
}

type WorkflowTimerCauseReplayFixtureForTest struct {
	Context            context.Context
	Coordinator        *PipelineCoordinator
	CommitConstruction func(context.Context, flowidentity.RunScopedFlowInstance, WorkflowInstance, time.Time) (DynamicFlowRuntimeActivationAttempt, DynamicFlowRuntimeReadinessPlan)
	Observe            func() WorkflowTimerCauseReplayStorageForTest
	Publish            func(context.Context, events.Event) error
}

type WorkflowTimerCauseReplayFactoryForTest func(*testing.T, string, *contracts.WorkflowContractBundle) WorkflowTimerCauseReplayFixtureForTest

func VerifyWorkflowTimerCauseReplayEngineConsumersOnBothStoresForTest(t *testing.T, factory WorkflowTimerCauseReplayFactoryForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []workflowTimerCauseKind{workflowTimerCauseInitial, workflowTimerCauseEvent, workflowTimerCauseTransition} {
			for _, state := range []string{"active", "fired", "cancelled", "advanced", "advanced_cancelled"} {
				t.Run(backend+"/"+string(kind)+"/"+state, func(t *testing.T) {
					recurring := state == "advanced" || state == "advanced_cancelled"
					initialStage := "waiting"
					if kind == workflowTimerCauseTransition {
						initialStage = "ready"
					}
					files := map[string]string{
						"schema.yaml":   "name: timer-cause-proof\nstages:\n  waiting: {initial: true}\n",
						"entities.yaml": "test_entity: {}\n", "events.yaml": "timer.arm:\ntimer.elapsed:\n",
						"nodes.yaml": "observer:\n  execution_type: system_node\n  event_handlers:\n    timer.arm: {}\n",
					}
					if kind == workflowTimerCauseEvent {
						files["nodes.yaml"] += "  timers:\n    - {id: event.timeout, event: timer.elapsed, start_on: 'event:timer.arm', delay: 1h}\n"
					} else {
						files["schema.yaml"] = "name: timer-cause-proof\nstages:\n"
						if kind == workflowTimerCauseTransition {
							files["schema.yaml"] += "  ready: {initial: true}\n"
							files["nodes.yaml"] = "observer:\n  execution_type: system_node\n  event_handlers:\n    timer.arm: {advances_to: waiting}\n"
						}
						files["schema.yaml"] += fmt.Sprintf("  waiting:\n    initial: %v\n    timers:\n      - {id: waiting.timeout, after: 1h, emit: timer.elapsed}\n", kind == workflowTimerCauseInitial)
					}
					bundle := loadWorkflowTempBundle(t, files)
					bundle.Semantics.Timers[0].Recurring = recurring
					f := factory(t, backend, bundle)
					runID := runtimecorrelation.RunIDFromContext(f.Context)
					entityID := runID
					at := canonicalWorkflowTimerTime(time.Now().Add(-2 * time.Hour))
					identity := testRunScopedWorkflowInstanceFromContext(f.Context, runID)
					instance := workflowTimerMaterializedInstance(f.Context, entityID, runID, WorkflowInstance{
						WorkflowVersion: f.Coordinator.SemanticSource().WorkflowVersion(), EntityType: "test_entity", CurrentState: initialStage,
						CreatedAt: at, EnteredStageAt: at,
					})
					attempt, readiness := f.CommitConstruction(f.Context, identity, instance, at)
					ctx := f.Context
					var admittedEvent events.Event
					if kind != workflowTimerCauseInitial {
						event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "timer.arm", "operator", "", []byte(`{}`), 0, runID,
							events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), eventtest.RootRoutingSource(entityID), at.Add(time.Minute))
						ctx = runtimecorrelation.WithInboundEvent(ctx, event)
						admittedEvent = event
						if err := f.Publish(ctx, event); err != nil {
							t.Fatalf("ordinary accepted-event/transition publication: %v", err)
						}
					}
					rows := listTimerCauseReplayActivationsForTest(t, f.Coordinator.workflowStore.timerActivations, f.Context, entityID)
					if len(rows) != 1 {
						if kind == workflowTimerCauseEvent {
							route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "observer")),
								Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID})}
							proof, err := f.Coordinator.deliveryStore.ProveHandoff(ctx, admittedEvent.ID(), route)
							if err == nil {
								snapshot, err := f.Coordinator.deliveryStore.Snapshot(ctx, proof.DeliveryID())
								t.Logf("missing causal activation delivery %s: %s, failure=%+v, error=%v", snapshot.DeliveryID, snapshot.Status, snapshot.Failure, err)
							} else {
								t.Logf("missing causal activation handoff: %v", err)
							}
						}
						t.Fatalf("causal activation rows = %+v, want one", rows)
					}
					initial := rows[0]
					if initial.Ref.Cause != timeridentity.WorkflowTimerActivationCause(kind) {
						t.Fatalf("persisted cause = %s, want %s", initial.Ref.Cause, kind)
					}
					if state == "fired" || recurring {
						if outcome, err := fireWorkflowTimerTestWakeup(ctx, f.Coordinator, initial); err != nil || outcome != WorkflowTimerFireCommitted {
							t.Fatalf("real cause occurrence: %s, %v", outcome, err)
						}
					}
					if state == "cancelled" || state == "advanced_cancelled" {
						if err := cancelSelectedWorkflowTimerForTest(ctx, f.Coordinator, initial, attempt); err != nil {
							t.Fatal(err)
						}
					}
					before := loadSelectedWorkflowTimerActivationForTest(t, f.Coordinator.workflowStore.timerActivations, ctx, initial.Ref.ActivationID)
					storage := f.Observe()
					for i := 0; i < 2; i++ {
						if kind == workflowTimerCauseInitial {
							if err := f.Coordinator.ReconcileInitialEntryTimersForAttempt(ctx, identity, attempt, readiness); err != nil {
								t.Fatalf("production initial-entry reconciliation: %v", err)
							}
						} else {
							if err := f.Publish(ctx, admittedEvent); err != nil {
								t.Fatalf("ordinary accepted-event replay: %v", err)
							}
							// Event admission may deduplicate before insertion. Exercise the
							// native engine commit separately with the original causal record.
							loaded, found, err := f.Coordinator.workflowStore.Load(ctx, identity)
							if err != nil || !found {
								t.Fatalf("native engine state readback: %v, %v", found, err)
							}
							record, err := workflowEngineStateRecord(identity, loaded, loaded.CurrentState, loaded.Revision,
								WorkflowEngineStateTransitionUpdateStateAndCompanion, time.Now().UTC())
							if err != nil {
								t.Fatal(err)
							}
							committed, err := f.Coordinator.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{
								State: record, Lifecycle: WorkflowLifecycleMutationPlan{Timers: []WorkflowTimerMutation{{Kind: WorkflowTimerMutationInsert, Activation: initial}}},
							})
							if err != nil || !committed.Committed || committed.Validate() != nil || len(committed.Lifecycle.Wakeups) != 1 ||
								committed.Lifecycle.Wakeups[0] != initial.Ref || len(committed.Lifecycle.Cancellations) != 0 {
								t.Fatalf("native engine exact-cause commit: %+v, %v", committed, err)
							}
							if err := f.Coordinator.finalizeWorkflowLifecycleMutation(ctx, committed.Lifecycle); err != nil {
								t.Fatal(err)
							}
						}
						got := listTimerCauseReplayActivationsForTest(t, f.Coordinator.workflowStore.timerActivations, ctx, entityID)
						if len(got) != 1 || !reflect.DeepEqual(got[0], before) || f.Observe() != storage {
							t.Fatalf("exact engine cause replay changed durable history/publication: %+v", got)
						}
					}
					if state != "active" {
						if outcome, err := fireWorkflowTimerTestWakeup(ctx, f.Coordinator, initial); err != nil || outcome != WorkflowTimerFireTerminal || f.Observe() != storage {
							t.Fatalf("old occurrence gained authority after cause replay: %s, %v", outcome, err)
						}
					}
					// A distinct start cause is eligible only after the old activation
					// is terminal; a different cause cannot replace an active timer.
					if kind == workflowTimerCauseEvent && (state == "fired" || state == "cancelled" || state == "advanced_cancelled") {
						t.Run("later_cause", func(t *testing.T) {
							later := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "timer.arm", "operator", "", []byte(`{}`), 0, runID,
								events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), eventtest.RootRoutingSource(entityID), at.Add(2*time.Minute))
							if err := f.Publish(runtimecorrelation.WithInboundEvent(f.Context, later), later); err != nil {
								t.Fatalf("ordinary later-cause publication: %v", err)
							}
							rows := listTimerCauseReplayActivationsForTest(t, f.Coordinator.workflowStore.timerActivations, ctx, entityID)
							want := storage
							want.Events++
							want.Timers++
							want.ActiveTimers++
							want.TimerRevisionFacts++
							if len(rows) != 2 {
								t.Fatalf("later cause produced %d activations, want exactly two", len(rows))
							}
							var old, successor WorkflowTimerActivation
							for _, row := range rows {
								if row.Ref == initial.Ref {
									old = row
								} else {
									successor = row
								}
							}
							if !reflect.DeepEqual(old, before) {
								t.Fatalf("later cause changed old activation authority: %+v", old)
							}
							if successor.Ref.ActivationID == initial.Ref.ActivationID ||
								successor.Ref.DeclarationKey != initial.Ref.DeclarationKey || successor.Ref.DeclarationRevision != initial.Ref.DeclarationRevision ||
								successor.Ref.Cause != timeridentity.WorkflowTimerActivationCauseEvent || successor.Status != workflowTimerStatusActive {
								t.Fatalf("later cause failed to create one active successor of the same declaration: %+v", successor)
							}
							if got := f.Observe(); got != want {
								t.Fatalf("later cause effects: got=%+v want=%+v", got, want)
							}
							if err := f.Publish(runtimecorrelation.WithInboundEvent(f.Context, later), later); err != nil {
								t.Fatalf("ordinary later-cause replay: %v", err)
							}
							if got := listTimerCauseReplayActivationsForTest(t, f.Coordinator.workflowStore.timerActivations, ctx, entityID); !reflect.DeepEqual(got, rows) || f.Observe() != want {
								t.Fatalf("later-cause replay changed exact activations/effects: %+v", got)
							}
						})
					}
				})
			}
		}
	}
}
