package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestUnstartedDirectiveTurnRecoveryBothStores(t *testing.T) {
	for _, flow := range []string{"", "completion"} {
		label := flow
		if label == "" {
			label = "root"
		}
		t.Run(label, func(t *testing.T) {
			for _, disposition := range []string{"restart", "rollback"} {
				t.Run(disposition, func(t *testing.T) {
					eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
						store := selected.selected.(completionSettlementTestStore)
						if selected.postgres {
							store = admitTestPostgresStore(t, selected.db)
						}
						fixture := newCompletionSettlementFixtureForFlow(t, store, selected.db, !selected.postgres, agentmemory.Plan{}, flow)
						// Complete the fixture's independent event origin first. The
						// only live work under test is the actual admitted directive.
						base := fixture.contextFor(fixture.authority)
						claim, found := deliverylifecycle.ClaimFromContext(base)
						if !found {
							t.Fatal("fixture lost its real delivery claim")
						}
						if _, err := store.(deliverylifecycle.Store).SettleSuccess(base, claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
							t.Fatal(err)
						}
						origin, _, event := admitProviderDirectiveOrigin(t, fixture, requireProviderDirectiveStore(t, fixture), "unstarted")
						ctx, carrier := effects.WithTurnExecution(providerDirectiveContext(t, fixture, origin, event, "unstarted-directive"))
						defer func() { _, _ = carrier.Finish() }()
						scope, instance, path, err := fixture.authority.BusinessTurnCoordinates()
						if err != nil {
							t.Fatal(err)
						}
						owner := flowidentity.RunScopedFlowInstance{RunID: fixture.authority.Target.RunID, Route: flowidentity.StoredRoute(scope, instance, path)}
						committed, err := commitUnstartedDirectiveTermination(t, ctx, fixture, owner, disposition == "rollback")
						if disposition == "rollback" {
							if err == nil || committed.Committed || len(committed.Lifecycle.TurnCancellations) != 0 {
								t.Fatalf("rolled-back stage retained directive intent: %+v err=%v", committed, err)
							}
							if _, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("after-directive-rollback")); err != nil {
								t.Fatalf("rollback fenced admitted directive: %v", err)
							}
							return
						}
						if err != nil || !committed.Committed || len(committed.Lifecycle.TurnCancellations) != 1 {
							t.Fatalf("unstarted directive omitted from termination: %+v err=%v", committed, err)
						}
						intent := committed.Lifecycle.TurnCancellations[0]
						if matched, err := carrier.RequestCancellation(intent); err != nil || !matched || ctx.Err() == nil {
							t.Fatalf("directive execution missed intent: matched=%t err=%v", matched, err)
						}
						joined, err := carrier.Finish()
						if err != nil || joined.Attempt.AttemptID != "" || joined.Clock != nil {
							t.Fatalf("unstarted directive manufactured physical evidence: %+v err=%v", joined, err)
						}
						cleanup := context.WithoutCancel(ctx)
						if _, err := beginManagedCompletionForTest(t, cleanup, "anthropic_api", []byte("after-directive-termination")); err == nil {
							t.Fatal("termination winner admitted provider work")
						}
						canceled := store.(effects.CanceledTurnStore)
						foreign := intent.Origin
						foreign.Directive.ExecutionOwnerID = uuid.NewString()
						if result, err := canceled.CommitCanceledTurn(cleanup, effects.CanceledTurnCommand{Origin: foreign}); err == nil || result.Acknowledged {
							t.Fatalf("foreign directive owner settled cancellation: %+v err=%v", result, err)
						}
						turns, err := store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(cleanup, liveExternalEffectRecoveryRequest(time.Now().UTC()))
						if err != nil || len(turns) != 1 || turns[0].Clock != nil || turns[0].Attempt.AttemptID != "" || !turns[0].Cancellation.Origin.Same(intent.Origin) {
							t.Fatalf("restart lost exact unstarted directive: %+v err=%v", turns, err)
						}
						command := effects.CanceledTurnCommand{Origin: turns[0].Cancellation.Origin}
						result, err := canceled.CommitCanceledTurn(cleanup, command)
						if err != nil || result.Validate() != nil || result.Directive.State != agentcontrol.DirectiveOperationCanceled || result.Directive.ExecutionOwnerID != origin.ExecutionOwnerID {
							t.Fatalf("unstarted directive did not settle exactly: %+v err=%v", result, err)
						}
						again, err := canceled.CommitCanceledTurn(cleanup, command)
						if err != nil || again.Validate() != nil || !again.Directive.CompletedAt.Equal(result.Directive.CompletedAt) {
							t.Fatalf("retry changed directive outcome: %+v err=%v", again, err)
						}
						remaining, err := store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(cleanup, liveExternalEffectRecoveryRequest(time.Now().UTC()))
						if err != nil || len(remaining) != 0 {
							t.Fatalf("settled directive remained recoverable: %+v err=%v", remaining, err)
						}
					})
				})
			}
		})
	}
}

func commitUnstartedDirectiveTermination(t *testing.T, ctx context.Context, fixture completionSettlementFixture, owner flowidentity.RunScopedFlowInstance, rollback bool) (pipeline.CommittedWorkflowEngineMutation, error) {
	t.Helper()
	at, entity := time.Now().UTC().Truncate(time.Microsecond), uuid.NewString()
	record := seedTurnTerminationHeader(t, fixture, owner, entity, at)
	cause := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
	node, err := identity.AdmitExecutableNodeDeclaration(owner.Route.ScopeKey, "router")
	if err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: owner.Route.ScopeKey, FlowInstance: owner.Route.InstancePath, EntityID: entity})}
	if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, cause, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), cause, route)
	if err != nil {
		t.Fatal(err)
	}
	if rollback {
		if _, err := fixture.store.(deliverylifecycle.Store).SettleSuccess(ctx, claimed.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
			t.Fatal(err)
		}
	}
	return fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, turnTerminationCommandForClaim(t, record, cause, claimed.Claim, at))
}
