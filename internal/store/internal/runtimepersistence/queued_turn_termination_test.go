package runtimepersistence

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestQueuedTurnTerminationBothStores(t *testing.T) {
	for _, mode := range []string{"pending", "retry", "rollback", "isolated", "root", "retained_settled", "retained_open"} {
		t.Run(mode, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				if mode == "root" {
					fixture = newCompletionSettlementFixtureForFlow(t, fixture.store, fixture.db, fixture.sqlite, agentmemory.Plan{}, "")
				}
				ctx := correlation.WithRunID(fixture.contextFor(fixture.authority), fixture.authority.Target.RunID)
				scope, instance, path, err := fixture.authority.BusinessTurnCoordinates()
				if err != nil {
					t.Fatal(err)
				}
				owner := flowidentity.RunScopedFlowInstance{RunID: fixture.authority.Target.RunID, Route: flowidentity.StoredRoute(scope, instance, path)}
				at := time.Now().UTC().Truncate(time.Microsecond)
				entity := uuid.NewString()
				record := seedTurnTerminationHeader(t, fixture, owner, entity, at)
				event := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(fixture.authority.Normal.Identity.AgentID()), AgentIdentity: fixture.authority.Normal.Identity}
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				var retained *effects.Handle
				if mode == "retry" || mode == "retained_settled" || mode == "retained_open" {
					claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), event, route)
					if err != nil {
						t.Fatal(err)
					}
					if mode != "retry" {
						providerCtx := deliverylifecycle.WithClaim(correlation.WithInboundEvent(ctx, event), claimed.Claim)
						retained = beginObservedCompletionForSettlementTest(t, providerCtx, "anthropic_api", mode)
						if mode == "retained_settled" {
							settlement := completionSettlementForTest(t, fixture.authority.Target, fixture, "anthropic_api", "provider-head-current", "retained-head")
							settlement.AgentTurn.TriggerEventID, settlement.AgentTurn.TriggerEventType = event.ID(), string(event.Type())
							payload, _ := completionSuccessorPayload(t, "anthropic_api", fixture, settlement, "agent-frame:v1:"+uuid.NewString())
							if err := effects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte(mode), payload); err != nil {
								t.Fatal(err)
							}
							result, err := retained.SettleCompletion(providerCtx, settlement)
							if err != nil || !result.Committed {
								t.Fatalf("record accepted physical history: %+v %v", result, err)
							}
							requireActiveCompletionContinuation(t, fixture, retained.Attempt().AttemptID, true)
						}
					}
					failure := failures.FromError(errors.New("controlled prior failure"), "queued-proof", "retry").Failure
					_, err = fixture.store.(deliverylifecycle.Store).SettleFailure(ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: &failure, RuleSelection: handlerselection.NotReached()})
					if err != nil {
						t.Fatal(err)
					}
				}
				id, err := deliverylifecycle.DeliveryID(event.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				before, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				beforeOutcomes, err := fixture.store.(deliverylifecycle.Store).Outcomes(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				var physicalBefore map[string][]string
				if retained != nil {
					physicalBefore = snapshotForkHistoricalExecutionTables(t, fixture.db, !fixture.sqlite)
				}
				var siblings []deliverylifecycle.Snapshot
				if mode == "isolated" {
					sibling := mustTestAgentIdentityForRun(owner.RunID, fixture.agentID+"-sibling", path+"/sibling")
					if err := agentfixture.UpsertStatic(t, ctx, fixture.store, agentFixtureStaticRecord(t, sibling)); err != nil {
						t.Fatal(err)
					}
					siblingRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(sibling.AgentID()), AgentIdentity: sibling}
					siblingEvent := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
					if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, siblingEvent, []events.DeliveryRoute{siblingRoute}); err != nil {
						t.Fatal(err)
					}
					siblings = append(siblings, loadDeliverySnapshotFixture(t, ctx, fixture.store.(deliveryFixtureStore), siblingEvent.ID(), siblingRoute))
					other := newCompletionSettlementFixture(t, fixture.store, fixture.db, fixture.sqlite)
					otherEvent := managedCompletionTestEventWithIdentity(other.authority, uuid.NewString(), "completion.test.requested")
					otherRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(other.agentID), AgentIdentity: other.authority.Normal.Identity}
					if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, otherEvent, []events.DeliveryRoute{otherRoute}); err != nil {
						t.Fatal(err)
					}
					siblings = append(siblings, loadDeliverySnapshotFixture(t, ctx, fixture.store.(deliveryFixtureStore), otherEvent.ID(), otherRoute))
				}
				cause := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
				node, err := identity.AdmitExecutableNodeDeclaration(owner.Route.ScopeKey, "router")
				if err != nil {
					t.Fatal(err)
				}
				nodeRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: owner.Route.ScopeKey, FlowInstance: path, EntityID: entity})}
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, cause, []events.DeliveryRoute{nodeRoute}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), cause, nodeRoute)
				if err != nil {
					t.Fatal(err)
				}
				command := turnTerminationCommandForClaim(t, record, cause, claimed.Claim, at)
				headerBefore, found, err := fixture.store.(pipeline.WorkflowInstancePersistenceReader).LoadWorkflowInstance(ctx, owner)
				if err != nil || !found {
					t.Fatalf("read queued origin header: found=%t %v", found, err)
				}
				if mode == "rollback" {
					if _, err := fixture.store.(deliverylifecycle.Store).SettleSuccess(ctx, claimed.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
				}
				result, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, command)
				wantCommit := mode != "rollback" && mode != "retained_open"
				if wantCommit && (err != nil || !result.Committed || len(result.Lifecycle.QueuedCancellations) != 1) || !wantCommit && (err == nil || result.Committed || len(result.Lifecycle.QueuedCancellations) != 0) {
					t.Fatalf("queued/stage atomic commit: %+v err=%v", result, err)
				}
				if mode == "retained_open" && !strings.Contains(err.Error(), "queued terminate origin still owns physical provider work") {
					t.Fatalf("retained-tail refusal reached the wrong gate: %v", err)
				}
				after, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, id)
				if err != nil || after.RetryCount != before.RetryCount || after.ClaimVersion != before.ClaimVersion {
					t.Fatalf("cancellation invented an attempt or changed retry history: %+v before=%+v err=%v", after, before, err)
				}
				for _, before := range siblings {
					after, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, before.DeliveryID)
					if err != nil || !reflect.DeepEqual(after, before) {
						t.Fatalf("termination changed another instance/run's queue: before=%+v after=%+v err=%v", before, after, err)
					}
				}
				afterOutcomes, err := fixture.store.(deliverylifecycle.Store).Outcomes(ctx, id)
				if err != nil || !reflect.DeepEqual(beforeOutcomes, afterOutcomes) {
					t.Fatalf("queued cancellation rewrote prior claim outcomes: before=%+v after=%+v err=%v", beforeOutcomes, afterOutcomes, err)
				}
				if retained != nil {
					physicalAfter := snapshotForkHistoricalExecutionTables(t, fixture.db, !fixture.sqlite)
					for _, family := range []string{"runtime_external_effect_operations", "agent_turns", "spend_ledger"} {
						if !reflect.DeepEqual(physicalBefore[family], physicalAfter[family]) {
							t.Fatalf("queued cancellation rewrote retained %s", family)
						}
					}
					state := effects.StateSettled
					if mode == "retained_open" {
						state = effects.StateResponseObserved
					}
					requireExternalAttemptState(t, fixture.db, fixture.sqlite, retained.Attempt().AttemptID, state)
					if mode == "retained_settled" {
						requireActiveCompletionContinuation(t, fixture, retained.Attempt().AttemptID, false)
					}
				}
				if !wantCommit {
					headerAfter, found, err := fixture.store.(pipeline.WorkflowInstancePersistenceReader).LoadWorkflowInstance(ctx, owner)
					if err != nil || !found || !reflect.DeepEqual(headerBefore, headerAfter) {
						t.Fatalf("refused queue settlement changed its guarded header: %+v %v", headerAfter, err)
					}
					if after.Status != before.Status || !after.UpdatedAt.Equal(before.UpdatedAt) {
						t.Fatal("rollback retained queued cancellation")
					}
				} else if after.Status != deliverylifecycle.StatusCanceled || after.ReasonCode != "terminate" || after.Failure != nil || after.FinalSelection.Present() || deliverylifecycle.ValidateCanceledSnapshot(after) != nil {
					t.Fatalf("queued cancellation did not retain exact terminal evidence: %+v", after)
				}
				if wantCommit {
					claim, err := fixture.store.(deliverylifecycle.Store).ClaimDelivery(ctx, before.Authority, event, route)
					if _, acquired := claim.Acquired(); err != nil || acquired {
						t.Fatalf("canceled queue admitted new execution: %+v err=%v", claim, err)
					}
				}
			})
		})
	}
}
