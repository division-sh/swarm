package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

// This is a native transaction component proof with compiled carrier evidence,
// not public-launcher or paid-provider qualification.
func TestAuthoredTurnTerminationCommitsWithExactStageBothStores(t *testing.T) {
	for _, mode := range []string{"committed", "late_rollback", "stale_cas", "other_instance", "prelaunch", "prelaunch_restart"} {
		t.Run(mode, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				ctx := providerDrainContext(t, fixture, "authored-termination")
				ctx, turnOwner := runtimeeffects.WithTurnExecution(ctx)
				defer func() { _, _ = turnOwner.Finish() }()
				prelaunch := mode == "prelaunch" || mode == "prelaunch_restart"
				var handle *runtimeeffects.Handle
				if prelaunch {
					var err error
					handle, err = beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("authored-termination"))
					if err != nil {
						t.Fatal(err)
					}
				} else {
					handle = beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "authored-termination")
				}
				_, _, path, _ := fixture.authority.Normal.Identity.Route.Fields()
				owner := flowidentity.RunScopedFlowInstance{RunID: fixture.authority.Target.RunID, Route: flowidentity.RouteForInstancePath(path)}
				entityID := uuid.NewString()
				now := time.Now().UTC().Truncate(time.Microsecond)
				record := seedTurnTerminationHeader(t, fixture, owner, entityID, now)
				if mode == "other_instance" {
					owner.Route = flowidentity.RouteForInstancePath(path + "/other")
					entityID = uuid.NewString()
					record = seedTurnTerminationHeader(t, fixture, owner, entityID, now)
				}
				event := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
				node, err := identity.AdmitExecutableNodeDeclaration(path, "router")
				if err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: owner.Route.ScopeKey, FlowInstance: owner.Route.InstancePath, EntityID: entityID,
				})}
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), event, route)
				if err != nil {
					t.Fatal(err)
				}
				command := turnTerminationCommandForClaim(t, record, event, claimed.Claim, now)
				if mode == "stale_cas" {
					command.State.ExpectedRevision = 2
				}
				if mode == "late_rollback" {
					if _, err := fixture.store.(deliverylifecycle.Store).SettleSuccess(ctx, claimed.Claim, []string{"prior-commit"}, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
				}
				result, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, command)
				wantCommit := mode == "committed" || mode == "other_instance" || prelaunch
				if result.Committed != wantCommit || wantCommit && err != nil || !wantCommit && err == nil {
					t.Fatalf("stage/termination commit: %+v err=%v", result, err)
				}
				if mode == "late_rollback" && !strings.Contains(err.Error(), "settle workflow node delivery") {
					t.Fatalf("rollback proof did not reach the post-intent settlement: %v", err)
				}
				reader := fixture.store.(interface {
					ReadWorkflowHandlerStageReceipts(context.Context, string, flowidentity.RunScopedFlowInstance) ([]pipelineobligation.CommittedStageReceipt, error)
				})
				receipts, err := reader.ReadWorkflowHandlerStageReceipts(ctx, event.ID(), owner)
				if err != nil || wantCommit && (len(receipts) != 1 || receipts[0].Stage() != result.Stage) || !wantCommit && len(receipts) != 0 {
					t.Fatalf("atomic persisted handler stage receipt: %+v err=%v", receipts, err)
				}
				original := flowidentity.RunScopedFlowInstance{RunID: owner.RunID, Route: flowidentity.RouteForInstancePath(path)}
				header, found, err := fixture.store.(pipeline.WorkflowInstancePersistenceReader).LoadWorkflowInstance(ctx, original)
				if err != nil || !found {
					t.Fatalf("exact constructed-header readback: found=%t err=%v", found, err)
				}
				stage, revision := header.CurrentState, header.Revision
				wantStage, wantRevision := "ready", int64(1)
				if mode == "committed" || prelaunch {
					wantStage, wantRevision = "done", 2
				}
				if stage != wantStage || revision != wantRevision {
					t.Fatalf("guarded stage changed incorrectly: %s/%d", stage, revision)
				}
				if mode == "committed" || prelaunch {
					if len(result.Lifecycle.TurnCancellations) != 1 || result.Lifecycle.TurnCancellations[0].ValidateIntent() != nil || !result.Lifecycle.TurnCancellations[0].Origin.Same(handle.Attempt().Origin) || result.Lifecycle.TurnCancellations[0].Reason != deliverylifecycle.CancellationTerminate {
						t.Fatalf("missing exact acknowledged cancellation: %+v", result.Lifecycle)
					}
				} else if len(result.Lifecycle.TurnCancellations) != 0 {
					t.Fatalf("uncommitted/sibling scope returned cancellation: %+v", result.Lifecycle)
				}
				snapshot, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, fixture.origin.DeliveryID())
				if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress {
					t.Fatalf("intent prematurely settled accepted work: %+v err=%v", snapshot, err)
				}
				if !prelaunch {
					requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateResponseObserved)
					if mode == "committed" {
						later := command
						later.DeliverySuccess = nil
						later.Lifecycle = pipeline.WorkflowLifecycleMutationPlan{}
						later.State.ExpectedState, later.State.ExpectedRevision = "done", 2
						later.State.CurrentState = "after"
						later.State.UpdatedAt = command.State.UpdatedAt.Add(time.Second)
						later.State.EnteredStageAt = later.State.UpdatedAt
						advanced, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, later)
						if err != nil || !advanced.Committed || advanced.Stage.Stage != "after" || advanced.Stage.Revision != 3 {
							t.Fatalf("later independent advance: %+v err=%v", advanced, err)
						}
						restored, err := reader.ReadWorkflowHandlerStageReceipts(ctx, event.ID(), owner)
						if err != nil || len(restored) != 1 || restored[0] != receipts[0] {
							t.Fatalf("later header replaced the occurrence's stage: %+v err=%v", restored, err)
						}
						absent, err := reader.ReadWorkflowHandlerStageReceipts(ctx, uuid.NewString(), owner)
						if err != nil || len(absent) != 0 {
							t.Fatalf("unrelated publication inherited a stage receipt: %+v err=%v", absent, err)
						}
					}
					return
				}
				requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateAuthorized)
				if matched, err := turnOwner.RequestCancellation(result.Lifecycle.TurnCancellations[0]); err != nil || !matched || ctx.Err() == nil {
					t.Fatalf("prelaunch carrier did not consume acknowledged intent: matched=%t err=%v", matched, err)
				}
				turn, err := turnOwner.Finish()
				if err != nil || turn.Clock != nil || !turn.Cancellation.Requested || turn.Attempt.AttemptID != handle.Attempt().AttemptID {
					t.Fatalf("prelaunch cancellation fabricated launch or lost origin: %+v err=%v", turn, err)
				}
				cleanupCtx := context.WithoutCancel(ctx)
				if launched, err := fixture.store.MarkExternalAttemptLaunched(cleanupCtx, handle.Attempt(), time.Now().UTC()); err == nil || launched.Committed {
					t.Fatalf("terminate-wins admitted a provider launch: %+v err=%v", launched, err)
				}
				canceled := fixture.store.(runtimeeffects.CanceledTurnStore)
				if result, err := canceled.CommitCanceledTurn(cleanupCtx, runtimeeffects.CanceledTurnCommandForAttempt(handle.Attempt(), nil)); err == nil || result.Acknowledged {
					t.Fatalf("prelaunch cancellation skipped owned physical cleanup: %+v err=%v", result, err)
				}
				failure := runtimefailures.FromError(context.Canceled, "termination-test", "prelaunch_cleanup").Failure
				if err := handle.Settle(cleanupCtx, runtimeeffects.StateTerminalFailure, &failure, map[string]any{"launch_rejected": true}); err != nil {
					t.Fatal(err)
				}
				if mode == "prelaunch_restart" {
					recovered, err := fixture.store.(runtimeeffects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(cleanupCtx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
					if err != nil || len(recovered) != 1 || recovered[0].Clock != nil || !recovered[0].Attempt.Origin.Same(handle.Attempt().Origin) {
						t.Fatalf("prelaunch recovery lost real origin: %+v err=%v", recovered, err)
					}
					turn = recovered[0]
				}
				settled, err := canceled.CommitCanceledTurn(cleanupCtx, runtimeeffects.CanceledTurnCommandForAttempt(turn.Attempt, nil))
				if err != nil || settled.Validate() != nil || settled.Delivery.Status != deliverylifecycle.StatusCanceled || settled.Delivery.ReasonCode != "terminate" || settled.Publication != nil {
					t.Fatalf("prelaunch exact canceled settlement: %+v err=%v", settled, err)
				}
				repeat, err := canceled.CommitCanceledTurn(cleanupCtx, runtimeeffects.CanceledTurnCommandForAttempt(turn.Attempt, nil))
				if err != nil || repeat.Validate() != nil || !repeat.Delivery.SettledAt.Equal(settled.Delivery.SettledAt) {
					t.Fatalf("prelaunch repeat changed outcome: %+v err=%v", repeat, err)
				}
				if _, launched := handle.LogicalTurnClock(); launched {
					t.Fatal("termination or recovery fabricated a launch clock")
				}
			})
		})
	}
}

func turnTerminationCommandForClaim(t *testing.T, record pipeline.WorkflowEngineStateRecord, event events.Event, claim deliverylifecycle.Claim, at time.Time) pipeline.WorkflowEngineMutationCommand {
	t.Helper()
	node, err := identity.ParseExecutableNodeKey(claim.SubscriberID())
	if err != nil {
		t.Fatal(err)
	}
	flow := record.Identity.Route.ScopeKey
	graph := contracts.BuildWorkflowStageTopology(flow, "ready", []string{"ready", "done"}, []string{"done"}, []contracts.HandlerTransitionSemantic{{Node: node, EventType: string(event.Type()), AdvancesTo: "done", Terminate: true}}, nil, nil)
	compiled, err := graph.AdmitTransition(contracts.WorkflowTransitionSite{Node: node, HandlerEvent: string(event.Type()), AdvanceCarrier: contracts.HandlerAdvanceCarrierHandler}, "ready", "done")
	if err != nil {
		t.Fatal(err)
	}
	transition, err := workflowlifecycle.NewCompiledTransition(compiled, handlerselection.NotApplicable(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cause, err := workflowlifecycle.NewAcceptedEvent(record.Identity.Route, identity.NormalizeEntityID(record.EntityID), event.ID(), string(event.Type()), executionmode.Live, at, &transition)
	if err != nil {
		t.Fatal(err)
	}
	cause, err = cause.WithExecutionOccurrence("delivery", claim.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	termination, err := workflowlifecycle.NewTurnTermination(record.Identity, cause)
	if err != nil {
		t.Fatal(err)
	}
	record.CurrentState, record.ExpectedState, record.ExpectedRevision = "done", "ready", 1
	record.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	record.UpdatedAt, record.EnteredStageAt = at.Add(time.Second), at.Add(time.Second)
	return pipeline.WorkflowEngineMutationCommand{State: record, Lifecycle: pipeline.WorkflowLifecycleMutationPlan{TurnTermination: &termination},
		DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
	}
}

func seedTurnTerminationHeader(t *testing.T, fixture completionSettlementFixture, owner flowidentity.RunScopedFlowInstance, entity string, now time.Time) pipeline.WorkflowEngineStateRecord {
	t.Helper()
	mode := "static"
	if owner.Route.InstancePath != owner.Route.ScopeKey {
		mode = "template"
	}
	return commitPreparedWorkflowAggregateFixture(t, testAuthorActivityContext(), fixture.store.(agentFixtureFlowStore), owner.RunID, pipeline.WorkflowInstance{
		InstanceID: owner.Route.InstanceID, StorageRef: owner.Route.InstancePath, EntityID: entity, WorkflowName: owner.Route.ScopeKey, WorkflowVersion: "fixture",
		Mode: mode, StageDefined: true, CurrentState: "ready", Fields: map[string]any{}, CreatedAt: now, EnteredStageAt: now,
	}, now)
}
