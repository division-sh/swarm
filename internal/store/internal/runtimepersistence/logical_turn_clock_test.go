package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestLogicalTurnClockStartsAtFirstLaunchBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		store := selected.selected.(completionSettlementTestStore)
		if selected.postgres {
			store = admitTestPostgresStore(t, selected.db)
		}
		fixture := newCompletionSettlementFixture(t, store, selected.db, !selected.postgres)
		bound := &timeridentity.TurnTimeout{After: 7 * time.Minute, Emit: "investigation.aborted"}
		authority := fixture.authority
		authority.BudgetScopes = nil
		ctx := runtimeeffects.WithTurnTimeout(fixture.contextFor(authority), bound)
		ctx = withManagedCompletionTestSurface(t, ctx, authority, "claude_cli")
		first := beginLogicalClockCompletion(t, ctx, "first-tool-round")
		var launched, deadline any
		if err := selected.db.QueryRowContext(ctx, `SELECT first_launched_at,deadline_at FROM runtime_agent_turn_lifetimes`).Scan(&launched, &deadline); err != nil {
			t.Fatal(err)
		}
		if launched != nil || deadline != nil {
			t.Fatal("authorization started the provider clock")
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		launch, err := store.MarkExternalAttemptLaunched(ctx, first.Attempt(), now)
		if err != nil || !launch.Committed || launch.Turn == nil {
			t.Fatalf("first launch: result=%+v err=%v", launch, err)
		}
		clock := *launch.Turn
		if err := clock.Validate(); err != nil || clock.FirstAttempt != first.Attempt().AttemptID || !clock.LaunchedAt.Equal(now) || !clock.DeadlineAt.Equal(now.Add(bound.After)) {
			t.Fatalf("clock not measured from first launch: %+v err=%v", clock, err)
		}
		second := beginLogicalClockCompletion(t, ctx, "second-tool-round")
		later, err := store.MarkExternalAttemptLaunched(ctx, second.Attempt(), now.Add(time.Minute))
		if err != nil || !later.Committed || later.Turn == nil || later.Turn.FirstAttempt != clock.FirstAttempt || !later.Turn.LaunchedAt.Equal(clock.LaunchedAt) || !later.Turn.DeadlineAt.Equal(clock.DeadlineAt) || later.Turn.TimeoutEvent != clock.TimeoutEvent {
			t.Fatalf("tool round reset the clock: first=%+v later=%+v err=%v", clock, later, err)
		}
		var rows int
		if err := selected.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_agent_turn_lifetimes`).Scan(&rows); err != nil || rows != 1 {
			t.Fatalf("one origin acquired multiple clocks: rows=%d err=%v", rows, err)
		}
		changed := runtimeeffects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Hour, Emit: bound.Emit})
		if _, err := beginManagedCompletionForTest(t, runtimeeffects.WithLogicalOperationIdentity(changed, "changed-bound"), "claude_cli", []byte("changed-bound")); err == nil {
			t.Fatal("same business turn accepted a changed bound")
		}
		unbounded := runtimeeffects.WithTurnTimeout(ctx, nil)
		if _, err := beginManagedCompletionForTest(t, runtimeeffects.WithLogicalOperationIdentity(unbounded, "removed-bound"), "claude_cli", []byte("removed-bound")); err == nil {
			t.Fatal("same business turn accepted removal of its bound")
		}
	})
}

func TestLogicalTurnClockRejectsSubstitutedOriginBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		store := selected.selected.(completionSettlementTestStore)
		if selected.postgres {
			store = admitTestPostgresStore(t, selected.db)
		}
		fixture := newCompletionSettlementFixture(t, store, selected.db, !selected.postgres)
		authority := fixture.authority
		authority.BudgetScopes = nil
		ctx := withManagedCompletionTestSurface(t, fixture.contextFor(authority), authority, "claude_cli")
		first := beginLogicalClockCompletion(t, ctx, "exact-origin")
		other := newCompletionSettlementFixture(t, store, selected.db, !selected.postgres)
		otherAuthority := other.authority
		otherAuthority.BudgetScopes = nil
		otherCtx := withManagedCompletionTestSurface(t, other.contextFor(otherAuthority), otherAuthority, "claude_cli")
		_ = beginLogicalClockCompletion(t, otherCtx, "foreign-origin")
		altered := first.Attempt()
		var err error
		altered.Origin, err = runtimeeffects.DeliveryCompletionOrigin(other.origin)
		if err != nil {
			t.Fatal(err)
		}
		launch, err := store.MarkExternalAttemptLaunched(ctx, altered, time.Now().UTC())
		if err == nil || launch.Committed {
			t.Fatalf("foreign origin admitted launch: result=%+v err=%v", launch, err)
		}
		requireExternalAttemptState(t, selected.db, !selected.postgres, first.Attempt().AttemptID, runtimeeffects.StateAuthorized)
		var launched sql.NullString
		if err := selected.db.QueryRowContext(ctx, `SELECT CAST(first_attempt_id AS TEXT) FROM runtime_agent_turn_lifetimes WHERE run_id=$1`, fixture.authority.Target.RunID).Scan(&launched); err != nil || launched.Valid {
			t.Fatalf("refused launch changed clock: %v err=%v", launched, err)
		}
	})
}

func beginLogicalClockCompletion(t *testing.T, ctx context.Context, key string) *runtimeeffects.Handle {
	t.Helper()
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, key)
	handle, err := beginManagedCompletionForTest(t, ctx, "claude_cli", []byte(key))
	if err != nil {
		t.Fatalf("authorize %s: %v", key, err)
	}
	return handle
}

func TestLogicalTurnTimeoutAdmissionOrdersBothStores(t *testing.T) {
	for _, order := range []string{"timeout_first", "launch_first"} {
		t.Run(order, func(t *testing.T) {
			eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
				store := selected.selected.(completionSettlementTestStore)
				if selected.postgres {
					store = admitTestPostgresStore(t, selected.db)
				}
				lifetime := store.(runtimeeffects.TurnLifetimeStore)
				fixture := newCompletionSettlementFixture(t, store, selected.db, !selected.postgres)
				authority := fixture.authority
				authority.BudgetScopes = nil
				bound := &timeridentity.TurnTimeout{After: time.Minute, Emit: "investigation.aborted"}
				ctx := runtimeeffects.WithTurnTimeout(fixture.contextFor(authority), bound)
				ctx = withManagedCompletionTestSurface(t, ctx, authority, "claude_cli")
				first := beginLogicalClockCompletion(t, ctx, "initial-provider-round")
				now := time.Now().UTC().Truncate(time.Microsecond)
				before, err := lifetime.RequestTurnTimeout(ctx, first.Attempt(), now.Add(time.Hour))
				if err != nil || !before.Committed || before.Requested {
					t.Fatalf("unlaunched turn acquired timeout intent: %+v err=%v", before, err)
				}
				launch, err := store.MarkExternalAttemptLaunched(ctx, first.Attempt(), now)
				if err != nil || !launch.Committed || launch.Turn == nil {
					t.Fatalf("first launch: %+v err=%v", launch, err)
				}
				second := beginLogicalClockCompletion(t, ctx, "pending-provider-round")
				early, err := lifetime.RequestTurnTimeout(ctx, first.Attempt(), now.Add(time.Second))
				if err != nil || !early.Committed || early.Requested {
					t.Fatalf("unexpired turn acquired timeout intent: %+v err=%v", early, err)
				}
				if order == "launch_first" {
					result, err := store.MarkExternalAttemptLaunched(ctx, second.Attempt(), now.Add(2*time.Second))
					if err != nil || !result.Committed {
						t.Fatalf("launch before timeout: %+v err=%v", result, err)
					}
				}
				cancellation, err := lifetime.RequestTurnTimeout(ctx, first.Attempt(), launch.Turn.DeadlineAt)
				if err != nil || !cancellation.Committed || !cancellation.Requested || cancellation.Reason != deliverylifecycle.CancellationTurnTimeout || cancellation.CauseEvent != launch.Turn.TimeoutEvent || !cancellation.Origin.Same(first.Attempt().Origin) {
					t.Fatalf("timeout lost exact intent: %+v err=%v", cancellation, err)
				}
				repeat, err := lifetime.RequestTurnTimeout(ctx, first.Attempt(), launch.Turn.DeadlineAt.Add(time.Hour))
				if err != nil || !repeat.Committed || repeat.CauseEvent != cancellation.CauseEvent || !repeat.RequestedAt.Equal(cancellation.RequestedAt) {
					t.Fatalf("timeout retry changed intent: first=%+v repeat=%+v err=%v", cancellation, repeat, err)
				}
				if order == "timeout_first" {
					refused, err := store.MarkExternalAttemptLaunched(ctx, second.Attempt(), launch.Turn.DeadlineAt)
					if err == nil || refused.Committed {
						t.Fatalf("timeout intent admitted another provider launch: %+v err=%v", refused, err)
					}
					requireExternalAttemptState(t, selected.db, !selected.postgres, second.Attempt().AttemptID, runtimeeffects.StateAuthorized)
				} else {
					requireExternalAttemptState(t, selected.db, !selected.postgres, second.Attempt().AttemptID, runtimeeffects.StateLaunched)
				}
				// Intent cannot counterfeit the physical tail or origin's settlement.
				requireExternalAttemptState(t, selected.db, !selected.postgres, first.Attempt().AttemptID, runtimeeffects.StateLaunched)
				var settled any
				if err := selected.db.QueryRowContext(ctx, `SELECT settled_at FROM runtime_agent_turn_lifetimes`).Scan(&settled); err != nil || settled != nil {
					t.Fatalf("timeout intent prematurely settled provider work: %v err=%v", settled, err)
				}
			})
		})
	}
}

func TestLogicalTurnDirectiveClockDoesNotCreateDeliveryBothStores(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		store := requireProviderDirectiveStore(t, fixture)
		origin, _, event := admitProviderDirectiveOrigin(t, fixture, store, "bounded-directive")
		before := providerDirectiveDeliveryCount(t, fixture)
		ctx := providerDirectiveContext(t, fixture, origin, event, "bounded-directive")
		ctx = runtimeeffects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "investigation.aborted"})
		handle, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("bounded-directive"))
		if err != nil {
			t.Fatal(err)
		}
		launch, err := store.MarkExternalAttemptLaunched(ctx, handle.Attempt(), time.Now().UTC())
		if err != nil || !launch.Committed || launch.Turn == nil || launch.Turn.Origin.Kind != runtimeeffects.CompletionOriginDirective {
			t.Fatalf("directive launch: %+v err=%v", launch, err)
		}
		cancellation, err := store.(runtimeeffects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), launch.Turn.DeadlineAt)
		if err != nil || !cancellation.Committed || !cancellation.Requested || !cancellation.Origin.Same(handle.Attempt().Origin) {
			t.Fatalf("directive timeout: %+v err=%v", cancellation, err)
		}
		requireProviderDirectiveDeliveryCount(t, fixture, before)
	})
}
