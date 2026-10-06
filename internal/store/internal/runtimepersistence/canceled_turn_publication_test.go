package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestCanceledTurnReactionCommitsAtomicallyBothStores(t *testing.T) {
	for _, kind := range []string{"delivery", "directive"} {
		t.Run(kind, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				ctx := providerDrainContext(t, fixture, "atomic-timeout")
				if kind == "directive" {
					origin, _, event := admitProviderDirectiveOrigin(t, fixture, requireProviderDirectiveStore(t, fixture), "atomic-timeout")
					ctx = providerDirectiveContext(t, fixture, origin, event, "atomic-timeout")
				}
				ctx = runtimeeffects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "atomic-timeout")
				clock, found := handle.LogicalTurnClock()
				if !found {
					t.Fatal("launched turn has no exact clock")
				}
				intent, err := fixture.store.(runtimeeffects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
				if err != nil || intent.ValidateIntent() != nil {
					t.Fatalf("timeout intent: %+v err=%v", intent, err)
				}
				store, ok := fixture.store.(runtimeeffects.CanceledTurnStore)
				if !ok {
					t.Fatal("cancellation owner has no atomic reaction commit")
				}
				command := runtimeeffects.CanceledTurnCommand{Attempt: handle.Attempt()}
				if result, err := store.CommitCanceledTurn(ctx, command); err == nil || result.Acknowledged {
					t.Fatalf("missing reaction or open physical tail accepted: %+v err=%v", result, err)
				}
				failure := runtimefailures.FromError(context.Canceled, "provider-test", "physical_join").Failure
				if err := handle.Settle(ctx, runtimeeffects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
					t.Fatal(err)
				}
				if result, err := store.CommitCanceledTurn(ctx, command); err == nil || result.Acknowledged {
					t.Fatalf("timeout origin settled without declared reaction: %+v err=%v", result, err)
				}
				command.Publication = prepareCanceledReactionForTest(t, ctx, fixture, clock, intent.RequestedAt)
				installCanceledReactionCut(t, ctx, fixture)
				result, err := store.CommitCanceledTurn(ctx, command)
				if err == nil || result.Acknowledged || result.Publication != nil {
					t.Fatalf("reaction failure acknowledged partial cancellation: %+v err=%v", result, err)
				}
				assertCanceledReactionCount(t, ctx, fixture, clock.TimeoutEvent, 0)
				if kind == "delivery" {
					snapshot, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, handle.Attempt().Origin.Delivery.DeliveryID())
					if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || !snapshot.SettledAt.IsZero() {
						t.Fatalf("reaction rollback changed delivery: %+v err=%v", snapshot, err)
					}
				} else {
					op, found, err := requireProviderDirectiveStore(t, fixture).LoadDirectiveOperation(ctx, handle.Attempt().Origin.Directive.OperationID)
					if err != nil || !found || op.State != agentcontrol.DirectiveOperationExecuting || !op.CompletedAt.IsZero() {
						t.Fatalf("reaction rollback changed directive: %+v err=%v", op, err)
					}
				}
				removeCanceledReactionCut(t, ctx, fixture)
				result, err = store.CommitCanceledTurn(ctx, command)
				if err != nil || !result.Acknowledged || result.Publication == nil || result.Publication.ValidateCommittedDurablePublication() != nil {
					t.Fatalf("atomic canceled reaction: %+v err=%v", result, err)
				}
				if !result.Origin.Same(handle.Attempt().Origin) || result.Publication.CommittedDurablePublicationEventID() != clock.TimeoutEvent {
					t.Fatal("commit substituted the origin or reaction")
				}
				if kind == "delivery" && result.Delivery.Status != deliverylifecycle.StatusCanceled ||
					kind == "directive" && result.Directive.State != agentcontrol.DirectiveOperationCanceled {
					t.Fatalf("atomic commit did not settle exact canceled origin: %+v", result)
				}
				var before string
				if err := fixture.db.QueryRowContext(ctx, `SELECT CAST(payload AS TEXT) FROM events WHERE event_id=$1`, clock.TimeoutEvent).Scan(&before); err != nil {
					t.Fatal(err)
				}
				repeated, err := store.CommitCanceledTurn(ctx, command)
				if err != nil || !repeated.Acknowledged || repeated.Publication == nil || repeated.Publication.ValidateCommittedDurablePublication() != nil {
					t.Fatalf("exact acknowledgment retry: %+v err=%v", repeated, err)
				}
				assertCanceledReactionCount(t, ctx, fixture, clock.TimeoutEvent, 1)
				var after string
				if err := fixture.db.QueryRowContext(ctx, `SELECT CAST(payload AS TEXT) FROM events WHERE event_id=$1`, clock.TimeoutEvent).Scan(&after); err != nil || after != before {
					t.Fatalf("exact retry changed reaction payload: %q err=%v", after, err)
				}
			})
		})
	}
}

func prepareCanceledReactionForTest(t *testing.T, ctx context.Context, fixture completionSettlementFixture, clock runtimeeffects.LogicalTurnClock, requestedAt time.Time) runtimeeffects.TurnReactionPlan {
	t.Helper()
	flowID, _, path, present := fixture.authority.Normal.Identity.Route.Fields()
	if !present {
		t.Fatal("turn reaction needs the exact agent route")
	}
	source, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: path})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RuntimeControlWithRoutingSource(clock.TimeoutEvent, events.EventType(clock.Timeout.Emit), runtimeeffects.TurnTimeoutProducerID(), "", []byte(`{}`), 0, fixture.authority.Target.RunID, "", events.EventEnvelope{}, source, requestedAt)
	if !fixture.sqlite {
		registerTestAuthorActivityCatalogForContext(t, fixture.store.(testAuthorActivityCatalogRegistrar), ctx)
	}
	bus, err := newStoreTestEventBus(t, fixture.store.(storeTestDurableEventBusStore))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := bus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
	if err != nil || len(plans) != 1 {
		t.Fatalf("prepare reaction: count=%d err=%v", len(plans), err)
	}
	return plans[0]
}

func assertCanceledReactionCount(t *testing.T, ctx context.Context, fixture completionSettlementFixture, eventID string, want int) {
	t.Helper()
	var count int
	if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1`, eventID).Scan(&count); err != nil || count != want {
		t.Fatalf("reaction count=%d want=%d err=%v", count, want, err)
	}
}

func installCanceledReactionCut(t *testing.T, ctx context.Context, fixture completionSettlementFixture) {
	t.Helper()
	statements := []string{`CREATE TRIGGER canceled_reaction_cut BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'canceled_reaction_cut'); END`}
	if !fixture.sqlite {
		statements = []string{`CREATE FUNCTION canceled_reaction_cut_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'canceled_reaction_cut'; END $$`, `CREATE TRIGGER canceled_reaction_cut BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION canceled_reaction_cut_fn()`}
	}
	for _, statement := range statements {
		if _, err := fixture.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func removeCanceledReactionCut(t *testing.T, ctx context.Context, fixture completionSettlementFixture) {
	t.Helper()
	statements := []string{`DROP TRIGGER canceled_reaction_cut`}
	if !fixture.sqlite {
		statements = []string{`DROP TRIGGER canceled_reaction_cut ON events`, `DROP FUNCTION canceled_reaction_cut_fn()`}
	}
	for _, statement := range statements {
		if _, err := fixture.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}
