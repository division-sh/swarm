package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func compoundFixtureEvent(t *testing.T, runID, eventID, payload string, at time.Time) (events.AdmittedEvent, events.RouteSettlement, []events.DeliveryRoute) {
	t.Helper()
	event := eventtest.ExistingRunRootIngressWithRoutingSource(eventID, "fixture.ready", "fixture", "", []byte(payload), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at)
	bound, err := bindSemanticEventFixturePayload(event)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPublish(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	routes := []events.DeliveryRoute{{
		Recipient:     events.MustAgentDeliveryRecipient("fixture-agent"),
		AgentIdentity: mustTestAgentIdentityForRun(runID, "fixture-agent", "fixture/agent"),
	}}
	return admitted, testRouteSettlement(admitted.Event(), routes), routes
}

func compoundFixtureCounts(t *testing.T, db *sql.DB, runID, eventID string) [5]int {
	t.Helper()
	var counts [5]int
	for i, table := range []string{"events", "event_deliveries", "committed_replay_scopes", "event_receipts", "run_fork_revisions"} {
		column, id := "event_id", eventID
		if table == "events" {
			column = "event_id"
		}
		if table == "run_fork_revisions" {
			column, id = "run_id", runID
		}
		if err := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE "+column+"=$1", id).Scan(&counts[i]); err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

func TestCompoundEventFixturesAtomicReplayAndRevisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, revisioned := range []bool{false, true} {
			name := "unrevisioned"
			if revisioned {
				name = "revisioned"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				selected, runID := decisionCardTestStore(t, backend)
				db, _ := decisionCardStoreDB(t, selected)
				ctx := testAuthorActivityContext()
				at := time.Now().UTC().Truncate(time.Microsecond)
				commit := func(event events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition) (bool, error) {
					if revisioned {
						return CommitRevisionedSemanticEventFixtureForTest(ctx, selected, event, settlement, routes, scope, disposition)
					}
					return CommitSemanticEventFixtureForTest(ctx, selected, event, settlement, routes, scope, disposition)
				}
				eventID := uuid.NewString()
				admitted, settlement, routes := compoundFixtureEvent(t, runID, eventID, `{"value":1}`, at)
				before := compoundFixtureCounts(t, db, runID, eventID)
				// Scope validation occurs after the event and delivery writes.
				if inserted, err := commit(admitted, settlement, routes, runtimepipelineobligation.CommittedScope("invalid"), nil); err == nil || inserted {
					t.Fatalf("invalid late scope: inserted=%v err=%v", inserted, err)
				}
				if after := compoundFixtureCounts(t, db, runID, eventID); after != before {
					t.Fatalf("late scope failure leaked facts: before=%v after=%v", before, after)
				}
				invalidDisposition := runtimepipelineobligation.Disposition{}
				if inserted, err := commit(admitted, settlement, routes, runtimepipelineobligation.ScopeSubscribed, &invalidDisposition); err == nil || inserted {
					t.Fatalf("invalid late disposition: inserted=%v err=%v", inserted, err)
				}
				if after := compoundFixtureCounts(t, db, runID, eventID); after != before {
					t.Fatalf("late disposition failure leaked facts: before=%v after=%v", before, after)
				}
				ack := runtimepipelineobligation.Acknowledged("pipeline_persisted")
				if inserted, err := commit(admitted, settlement, routes, runtimepipelineobligation.ScopeSubscribed, &ack); err != nil || !inserted {
					t.Fatalf("first commit: inserted=%v err=%v", inserted, err)
				}
				after := compoundFixtureCounts(t, db, runID, eventID)
				if after[0] != 1 || after[1] != 1 || after[2] != 1 || after[3] != 1 || (after[4] > before[4]) != revisioned {
					t.Fatalf("compound facts/revision: before=%v after=%v revisioned=%v", before, after, revisioned)
				}
				deliveryID, err := runtimedelivery.DeliveryID(eventID, routes[0])
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := selected.(interface {
					Snapshot(context.Context, string) (runtimedelivery.Snapshot, error)
				}).Snapshot(ctx, deliveryID)
				if err != nil || snapshot.ContinuationHandoffAt.IsZero() || snapshot.Status != runtimedelivery.StatusPending {
					t.Fatalf("canonical continuation handoff: %+v err=%v", snapshot, err)
				}
				if inserted, err := commit(admitted, settlement, routes, runtimepipelineobligation.ScopeSubscribed, &ack); err != nil || inserted {
					t.Fatalf("exact replay: inserted=%v err=%v", inserted, err)
				}
				changed, changedSettlement, changedRoutes := compoundFixtureEvent(t, runID, eventID, `{"value":2}`, at)
				if inserted, err := commit(changed, changedSettlement, changedRoutes, runtimepipelineobligation.ScopeSubscribed, &ack); err == nil || inserted {
					t.Fatalf("changed-record replay: inserted=%v err=%v", inserted, err)
				}
				if replayed := compoundFixtureCounts(t, db, runID, eventID); replayed != after {
					t.Fatalf("replay changed compound facts: want=%v got=%v", after, replayed)
				}
			})
		}
	}
}

func TestCompoundEventFixturesUseExactSQLiteAdmission(t *testing.T) {
	for _, revisioned := range []bool{false, true} {
		name := "unrevisioned"
		if revisioned {
			name = "revisioned"
		}
		t.Run(name, func(t *testing.T) {
			owner, runID := decisionCardTestStore(t, "sqlite")
			selected := owner.(*SQLiteRuntimeStore)
			db, _ := decisionCardStoreDB(t, owner)
			ctx, stop := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
			defer stop()
			eventID := uuid.NewString()
			admitted, settlement, routes := compoundFixtureEvent(t, runID, eventID, `{"value":1}`, time.Now().UTC())
			before := compoundFixtureCounts(t, db, runID, eventID)
			entered, release, heldDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			go func() {
				heldDone <- selected.backend.RunTransaction(ctx, "hold exact fixture admission", func(context.Context, *sql.Tx) error {
					close(entered)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}()
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer func() {
				unblock()
				if err := <-heldDone; err != nil {
					t.Error(err)
				}
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			queued, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			invoke := func(ctx context.Context) (bool, error) {
				if revisioned {
					return CommitRevisionedSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, runtimepipelineobligation.ScopeSubscribed, nil)
				}
				return CommitSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, runtimepipelineobligation.ScopeSubscribed, nil)
			}
			go func() { _, err := invoke(queued); done <- err }()
			joined := false
			defer func() {
				cancel()
				unblock()
				if !joined {
					<-done
				}
			}()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for selected.backend.QueuedWritersForTest() != 1 {
				select {
				case err := <-done:
					joined = true
					t.Fatalf("fixture bypassed the held original coordinator: %v", err)
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			cancel()
			err := <-done
			joined = true
			if !errors.Is(err, context.Canceled) || selected.backend.QueuedWritersForTest() != 0 {
				t.Fatalf("queued cancellation: %v", err)
			}
			if after := compoundFixtureCounts(t, db, runID, eventID); after != before {
				t.Fatalf("cancelled fixture mutated facts: before=%v after=%v", before, after)
			}
			unblock()
			if inserted, err := invoke(ctx); err != nil || !inserted {
				t.Fatalf("fixture after holder release: inserted=%v err=%v", inserted, err)
			}
		})
	}
}

func TestCompoundEventFixturesRejectMissingSelectedOwner(t *testing.T) {
	admitted, settlement, routes := compoundFixtureEvent(t, uuid.NewString(), uuid.NewString(), `{}`, time.Now().UTC())
	for _, selected := range []any{nil, (*SQLiteRuntimeStore)(nil), (*PostgresStore)(nil), &SQLiteRuntimeStore{}, &PostgresStore{}, &sql.DB{}} {
		for _, revisioned := range []bool{false, true} {
			inserted, err := commitSelectedSemanticEventFixtureForTest(context.Background(), selected, admitted, settlement, routes, runtimepipelineobligation.ScopeSubscribed, nil, revisioned, false)
			if inserted || err == nil {
				t.Fatalf("missing owner %T admitted: inserted=%v err=%v", selected, inserted, err)
			}
		}
	}
}
