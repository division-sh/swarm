package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestRootTurnTerminationUsesConstructedRunOwnerBothStores(t *testing.T) {
	for _, phase := range []string{"prelaunch", "launched"} {
		t.Run(phase, func(t *testing.T) {
			eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
				store := selected.selected.(completionSettlementTestStore)
				if selected.postgres {
					store = admitTestPostgresStore(t, selected.db)
				}
				fixture := newCompletionSettlementFixtureForFlow(t, store, selected.db, !selected.postgres, agentmemory.Plan{}, "")
				ctx := withManagedCompletionTestSurface(t, fixture.contextFor(fixture.authority), fixture.authority, "anthropic_api")
				handle, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("root-termination"))
				if err != nil {
					t.Fatal(err)
				}
				if phase == "launched" {
					if err := handle.MarkLaunched(ctx); err != nil {
						t.Fatal(err)
					}
				}
				runID := fixture.authority.Target.RunID
				owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute(".", runID, runID)}
				at := time.Now().UTC().Truncate(time.Microsecond)
				entity := uuid.NewString()
				record := seedTurnTerminationHeader(t, fixture, owner, entity, at)
				cause := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
				node, err := identity.AdmitExecutableNodeDeclaration(".", "router")
				if err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entity})}
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, cause, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), cause, route)
				if err != nil {
					t.Fatal(err)
				}
				result, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, turnTerminationCommandForClaim(t, record, cause, claimed.Claim, at))
				if err != nil || !result.Committed || len(result.Lifecycle.TurnCancellations) != 1 || !result.Lifecycle.TurnCancellations[0].Origin.Same(handle.Attempt().Origin) {
					t.Fatalf("root termination missed its exact admitted origin: %+v err=%v", result, err)
				}
				if fixture.authority.Target.FlowInstance != "" || !owner.MatchesAgentRoute(fixture.authority.Target.AgentIdentity) {
					t.Fatal("root usage target or declaration identity was rewritten")
				}
				canceled := store.(runtimeeffects.CanceledTurnStore)
				command := runtimeeffects.CanceledTurnCommandForAttempt(handle.Attempt(), nil)
				if result, err := canceled.CommitCanceledTurn(ctx, command); err == nil || result.Acknowledged {
					t.Fatalf("root origin skipped its physical cleanup: %+v err=%v", result, err)
				}
				state := runtimeeffects.StateTerminalFailure
				if phase == "launched" {
					state = runtimeeffects.StateOutcomeUncertain
				}
				failure := failures.FromError(context.Canceled, "root-termination-test", "physical_cleanup").Failure
				if err := handle.Settle(ctx, state, &failure, map[string]any{"launch_rejected": phase == "prelaunch", "physical_joined": true}); err != nil {
					t.Fatal(err)
				}
				recovered, err := store.(runtimeeffects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
				if err != nil || len(recovered) != 1 || !recovered[0].Attempt.Origin.Same(handle.Attempt().Origin) || (recovered[0].Clock != nil) != (phase == "launched") {
					t.Fatalf("root recovery lost origin or launch evidence: %+v err=%v", recovered, err)
				}
				command = runtimeeffects.CanceledTurnCommandForAttempt(recovered[0].Attempt, nil)
				settled, err := canceled.CommitCanceledTurn(ctx, command)
				if err != nil || settled.Validate() != nil || settled.Delivery.Status != deliverylifecycle.StatusCanceled || settled.Delivery.ReasonCode != "terminate" {
					t.Fatalf("root recovery did not settle its exact origin: %+v err=%v", settled, err)
				}
				repeat, err := canceled.CommitCanceledTurn(ctx, command)
				if err != nil || repeat.Validate() != nil || !repeat.Delivery.SettledAt.Equal(settled.Delivery.SettledAt) {
					t.Fatalf("root retry changed settlement: %+v err=%v", repeat, err)
				}
			})
		})
	}
}

func TestRootTurnTimeoutRecoveryKeepsExactReactionBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		store := selected.selected.(completionSettlementTestStore)
		if selected.postgres {
			store = admitTestPostgresStore(t, selected.db)
		}
		fixture := newCompletionSettlementFixtureForFlow(t, store, selected.db, !selected.postgres, agentmemory.Plan{}, "")
		ctx := runtimeeffects.WithTurnTimeout(fixture.contextFor(fixture.authority), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
		ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "anthropic_api")
		handle, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("root-timeout"))
		if err != nil {
			t.Fatal(err)
		}
		if err := handle.MarkLaunched(ctx); err != nil {
			t.Fatal(err)
		}
		clock, found := handle.LogicalTurnClock()
		if !found {
			t.Fatal("root provider launch omitted its logical clock")
		}
		intent, err := store.(runtimeeffects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
		if err != nil || intent.ValidateIntent() != nil {
			t.Fatalf("root timeout intent: %+v err=%v", intent, err)
		}
		failure := failures.FromError(context.Canceled, "root-timeout-test", "physical_cleanup").Failure
		if err := handle.Settle(ctx, runtimeeffects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
			t.Fatal(err)
		}
		turns, err := store.(runtimeeffects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
		if err != nil || len(turns) != 1 || turns[0].Clock == nil || turns[0].Clock.TimeoutEvent != clock.TimeoutEvent {
			t.Fatalf("root timeout recovery lost the exact clock: %+v err=%v", turns, err)
		}
		if !fixture.sqlite {
			registerTestAuthorActivityCatalogForContext(t, fixture.store.(testAuthorActivityCatalogRegistrar), ctx)
		}
		bus, err := newStoreTestEventBus(t, fixture.store.(storeTestDurableEventBusStore))
		if err != nil {
			t.Fatal(err)
		}
		plan, err := bus.PrepareTurnTimeoutReaction(ctx, turns[0])
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := bus.ReleaseTurnTimeoutReaction(ctx, plan); err != nil {
				t.Error(err)
			}
		}()
		command := runtimeeffects.CanceledTurnCommandForAttempt(turns[0].Attempt, plan)
		canceled := store.(runtimeeffects.CanceledTurnStore)
		result, err := canceled.CommitCanceledTurn(ctx, command)
		if err != nil || result.Validate() != nil || result.Delivery.ReasonCode != "turn_timeout" || result.Publication == nil {
			t.Fatalf("root timeout reaction/origin atomic settlement: %+v err=%v", result, err)
		}
		assertCanceledReactionCount(t, ctx, fixture, clock.TimeoutEvent, 1)
		repeat, err := canceled.CommitCanceledTurn(ctx, command)
		if err != nil || repeat.Validate() != nil || !repeat.Delivery.SettledAt.Equal(result.Delivery.SettledAt) {
			t.Fatalf("root timeout retry changed origin: %+v err=%v", repeat, err)
		}
		assertCanceledReactionCount(t, ctx, fixture, clock.TimeoutEvent, 1)
	})
}
