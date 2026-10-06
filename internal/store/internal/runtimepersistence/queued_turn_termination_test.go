package runtimepersistence

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestQueuedTurnTerminationBothStores(t *testing.T) {
	for _, mode := range []string{"pending", "retry", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				ctx := providerDrainContext(t, fixture, "queued-termination")
				_, _, path, _ := fixture.authority.Normal.Identity.Route.Fields()
				owner := flowidentity.RunScopedFlowInstance{RunID: fixture.authority.Target.RunID, Route: flowidentity.RouteForInstancePath(path)}
				at := time.Now().UTC().Truncate(time.Microsecond)
				entity := uuid.NewString()
				record := seedTurnTerminationHeader(t, fixture, owner, entity, at)
				event := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(fixture.authority.Normal.Identity.AgentID()), AgentIdentity: fixture.authority.Normal.Identity}
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				if mode == "retry" {
					claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), event, route)
					if err != nil {
						t.Fatal(err)
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
				cause := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
				node, err := identity.AdmitExecutableNodeDeclaration(path, "router")
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
				if mode == "rollback" {
					if _, err := fixture.store.(deliverylifecycle.Store).SettleSuccess(ctx, claimed.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
				}
				result, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, command)
				if mode != "rollback" && (err != nil || !result.Committed || len(result.Lifecycle.QueuedCancellations) != 1) || mode == "rollback" && (err == nil || result.Committed || len(result.Lifecycle.QueuedCancellations) != 0) {
					t.Fatalf("queued/stage atomic commit: %+v err=%v", result, err)
				}
				after, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, id)
				if err != nil || after.RetryCount != before.RetryCount || after.ClaimVersion != before.ClaimVersion {
					t.Fatalf("cancellation invented an attempt or changed retry history: %+v before=%+v err=%v", after, before, err)
				}
				if mode == "rollback" {
					if after.Status != before.Status || !after.UpdatedAt.Equal(before.UpdatedAt) {
						t.Fatal("rollback retained queued cancellation")
					}
				} else if after.Status != deliverylifecycle.StatusCanceled || after.ReasonCode != "terminate" || after.Failure != nil || after.FinalSelection.Present() || deliverylifecycle.ValidateCanceledSnapshot(after) != nil {
					t.Fatalf("queued cancellation did not retain exact terminal evidence: %+v", after)
				}
				if mode != "rollback" {
					claim, err := fixture.store.(deliverylifecycle.Store).ClaimDelivery(ctx, before.Authority, event, route)
					if _, acquired := claim.Acquired(); err != nil || acquired {
						t.Fatalf("canceled queue admitted new execution: %+v err=%v", claim, err)
					}
				}
			})
		})
	}
}
