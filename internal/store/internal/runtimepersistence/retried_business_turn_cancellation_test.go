package runtimepersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestRetriedBusinessTurnTerminationAndTimeoutRecoveryBothStores(t *testing.T) {
	for _, mode := range []string{"unstarted", "authorized", "launched", "timeout-recovery"} {
		t.Run(mode, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				ctx := correlation.WithRunID(fixture.contextFor(fixture.authority), fixture.authority.Target.RunID)
				scope, instance, path, err := fixture.authority.BusinessTurnCoordinates()
				if err != nil {
					t.Fatal(err)
				}
				owner := flowidentity.RunScopedFlowInstance{RunID: fixture.authority.Target.RunID, Route: flowidentity.StoredRoute(scope, instance, path)}
				at, entity := time.Now().UTC().Truncate(time.Microsecond), uuid.NewString()
				record := seedTurnTerminationHeader(t, fixture, owner, entity, at)
				event := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(fixture.agentID), AgentIdentity: fixture.authority.Normal.Identity}
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				first, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), event, route)
				if err != nil {
					t.Fatal(err)
				}
				providerCtx := deliverylifecycle.WithClaim(correlation.WithInboundEvent(ctx, event), first.Claim)
				providerCtx = effects.WithTurnTimeout(providerCtx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
				providerCtx = withManagedCompletionTestSurface(t, providerCtx, fixture.authority, "claude_cli")
				handle, err := beginManagedCompletionForTest(t, providerCtx, "claude_cli", []byte("review-first-authorized"))
				if err != nil {
					t.Fatal(err)
				}
				failure := failures.Normalize(failures.New(failures.ClassDependencyUnavailable, "claude_cli_process_start_failed", "reviewer", "start", map[string]any{"launch_rejected": true}), "reviewer", "start")
				if err := handle.Settle(providerCtx, effects.StateTerminalFailure, &failure, map[string]any{"launch_rejected": true}); err != nil {
					t.Fatal(err)
				}
				_, err = fixture.store.(deliverylifecycle.Store).SettleFailure(ctx, first.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: &failure, RetryBase: time.Nanosecond, RuleSelection: handlerselection.NotReached()})
				if err != nil {
					t.Fatal(err)
				}
				second, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), event, route)
				if err != nil {
					t.Fatal(err)
				}
				if second.Claim.Version() <= first.Claim.Version() {
					t.Fatal("did not reclaim origin")
				}
				if mode != "unstarted" {
					retryCtx := deliverylifecycle.WithClaim(correlation.WithInboundEvent(ctx, event), second.Claim)
					retryCtx = effects.WithTurnTimeout(retryCtx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
					retryCtx = withManagedCompletionTestSurface(t, retryCtx, fixture.authority, "claude_cli")
					retry, err := beginManagedCompletionForTest(t, retryCtx, "claude_cli", []byte("review-first-authorized"))
					if err != nil {
						t.Fatal(err)
					}
					if mode == "launched" || mode == "timeout-recovery" {
						launch, err := fixture.store.MarkExternalAttemptLaunched(retryCtx, retry.Attempt(), at.Add(time.Second))
						if err != nil {
							t.Fatal(err)
						}
						if mode == "timeout-recovery" {
							intent, err := fixture.store.(effects.TurnLifetimeStore).RequestTurnTimeout(retryCtx, retry.Attempt(), launch.Turn.DeadlineAt)
							if err != nil || !intent.Committed || !intent.Requested {
								t.Fatalf("current retry timeout: %+v %v", intent, err)
							}
							if err := retry.Settle(retryCtx, effects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
								t.Fatal(err)
							}
							turns, err := fixture.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
							if err != nil || len(turns) != 1 || !turns[0].Attempt.Origin.Delivery.Same(second.Claim) {
								t.Fatalf("current retry timeout recovery: %+v %v", turns, err)
							}
							return
						}
					}
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
				result, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, turnTerminationCommandForClaim(t, record, cause, claimed.Claim, at))
				if err != nil || !result.Committed {
					t.Fatalf("terminate current retried origin: committed=%t err=%v", result.Committed, err)
				}
				if len(result.Lifecycle.TurnCancellations) != 1 || !result.Lifecycle.TurnCancellations[0].Origin.Delivery.Same(second.Claim) {
					t.Fatalf("cancellation did not bind current claim: %+v", result.Lifecycle.TurnCancellations)
				}
			})
		})
	}
}
