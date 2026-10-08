package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestUnstartedClaimedTurnTerminationBothStores(t *testing.T) {
	for _, mode := range []string{"waiting", "waiting_restart", "rollback", "authorization_wins"} {
		t.Run(mode, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				ctx, carrier := effects.WithTurnExecution(providerDrainContext(t, fixture, "unstarted-termination"))
				defer func() { _, _ = carrier.Finish() }()
				var physical *effects.Handle
				if mode == "authorization_wins" {
					var err error
					physical, err = beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("unstarted-termination"))
					if err != nil {
						t.Fatal(err)
					}
				}
				scope, instance, path, err := fixture.authority.BusinessTurnCoordinates()
				if err != nil {
					t.Fatal(err)
				}
				owner := flowidentity.RunScopedFlowInstance{RunID: fixture.authority.Target.RunID, Route: flowidentity.StoredRoute(scope, instance, path)}
				at := time.Now().UTC().Truncate(time.Microsecond)
				entity := uuid.NewString()
				record := seedTurnTerminationHeader(t, fixture, owner, entity, at)
				cause := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
				node, err := identity.AdmitExecutableNodeDeclaration(scope, "router")
				if err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: scope, FlowInstance: path, EntityID: entity})}
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, cause, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), cause, route)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "rollback" {
					if _, err := fixture.store.(deliverylifecycle.Store).SettleSuccess(ctx, claimed.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
				}
				committed, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, turnTerminationCommandForClaim(t, record, cause, claimed.Claim, at))
				if mode == "rollback" {
					if err == nil || committed.Committed || len(committed.Lifecycle.TurnCancellations) != 0 {
						t.Fatalf("rolled-back stage retained cancellation intent: %+v err=%v", committed, err)
					}
					if _, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("after-rollback")); err != nil {
						t.Fatalf("rollback fenced the original work: %v", err)
					}
					return
				}
				if err != nil || !committed.Committed || len(committed.Lifecycle.TurnCancellations) != 1 {
					t.Fatalf("claimed work was omitted before authorization: %+v err=%v", committed, err)
				}
				intent := committed.Lifecycle.TurnCancellations[0]
				if matched, err := carrier.RequestCancellation(intent); err != nil || !matched || ctx.Err() == nil {
					t.Fatalf("exact claimed carrier did not receive termination: matched=%v err=%v", matched, err)
				}
				joined, err := carrier.Finish()
				if err != nil || joined.Clock != nil {
					t.Fatalf("unlaunched work invented a provider clock: %+v err=%v", joined, err)
				}
				if mode == "authorization_wins" {
					if physical == nil || joined.Attempt.AttemptID != physical.Attempt().AttemptID {
						t.Fatal("authorization winner lost its actual attempt")
					}
					return
				}
				cleanup := context.WithoutCancel(ctx)
				if mode == "waiting_restart" {
					recovered, err := fixture.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(cleanup, liveExternalEffectRecoveryRequest(time.Now().UTC()))
					if err != nil || len(recovered) != 1 || recovered[0].Clock != nil || recovered[0].Attempt.AttemptID != "" || !recovered[0].Cancellation.Origin.Same(intent.Origin) {
						t.Fatalf("unstarted recovery fabricated a provider attempt or changed its origin: %+v err=%v", recovered, err)
					}
					intent = recovered[0].Cancellation
				}
				if _, err := beginManagedCompletionForTest(t, cleanup, "anthropic_api", []byte("after-termination")); err == nil {
					t.Fatal("termination winner admitted a provider attempt")
				}
				command := effects.CanceledTurnCommand{Origin: intent.Origin}
				result, err := fixture.store.(effects.CanceledTurnStore).CommitCanceledTurn(cleanup, command)
				if err != nil || result.Validate() != nil || result.Delivery.Status != deliverylifecycle.StatusCanceled || result.Delivery.ReasonCode != "terminate" {
					t.Fatalf("unstarted exact origin did not settle: %+v err=%v", result, err)
				}
				repeat, err := fixture.store.(effects.CanceledTurnStore).CommitCanceledTurn(cleanup, command)
				if err != nil || repeat.Validate() != nil || !repeat.Delivery.SettledAt.Equal(result.Delivery.SettledAt) {
					t.Fatalf("unstarted retry changed outcome: %+v err=%v", repeat, err)
				}
			})
		})
	}
}
