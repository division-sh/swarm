package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestDeliverySingletonSnapshotIndependentParityBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			adapter := m29DeliveryAdapter(t, backend.name == "postgres")
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			now, err := adapter.CaptureSnapshotTime(ctx, fixture.db)
			if err != nil {
				t.Fatal(err)
			}
			now = now.Truncate(time.Microsecond)
			states := []runtimedelivery.State{runtimedelivery.StateExhausted, runtimedelivery.StateDelivered, runtimedelivery.StateLaunching, runtimedelivery.StateRetrying, runtimedelivery.StateQueued, runtimedelivery.StateLaunching}
			routes := make([]events.DeliveryRoute, len(states))
			for i := range routes {
				routes[i] = testEntitylessNodeDeliveryRoute(fmt.Sprintf("singleton-%d", len(states)-i))
			}
			event := eventtest.ExistingRunRootIngress("abcdef01-abcd-4abc-8abc-abcdef012345", "singleton.parity", "gateway", "", []byte(`{"independent":true}`), 0, runID, events.EventEnvelope{}, now.Add(-3*time.Hour))
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, routes); err != nil {
				t.Fatal(err)
			}
			failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "singleton_failure", map[string]any{"proof": "full fields"})
			ids := make([]string, len(routes))
			created := make(map[string]time.Time, len(routes))
			for i, route := range routes {
				snapshot := seedDeliveryStateFixture(t, ctx, fixture.store, event, route, states[i], &failure)
				ids[i] = snapshot.DeliveryID
				// Tied creation times plus reversed groups make query ordering observable.
				at := now.Add(-2*time.Hour + time.Duration(1-i/3)*time.Second)
				transition := at.Add(time.Minute)
				if i == 5 {
					transition = now.Add(time.Hour)
				}
				if backend.name == "postgres" {
					setPostgresDeliveryFixtureTimes(t, ctx, fixture.db, snapshot, at, transition)
				} else {
					setSQLiteDeliveryFixtureTimes(t, ctx, fixture.db, snapshot, at, transition)
				}
				created[ids[i]] = at
			}
			sort.Slice(ids, func(i, j int) bool {
				if created[ids[i]].Equal(created[ids[j]]) {
					return ids[i] < ids[j]
				}
				return created[ids[i]].Before(created[ids[j]])
			})
			want := make([]runtimedelivery.Snapshot, len(ids))
			reclaimable, held, retries := 0, 0, 0
			for i, id := range ids {
				// This oracle is the individual canonical row reader, NOT either
				// event-wide method (which now share the same batch implementation).
				want[i], err = adapter.Snapshot(ctx, fixture.db, id)
				if err != nil {
					t.Fatal(err)
				}
				if want[i].ClaimReclaimable {
					reclaimable++
				} else if want[i].Status == runtimedelivery.StatusInProgress {
					held++
				}
				if want[i].RetryScheduled {
					retries++
				}
			}
			if reclaimable != 1 || held != 1 || retries != 1 {
				t.Fatalf("time/state controls: reclaimable=%d held=%d retries=%d", reclaimable, held, retries)
			}
			for _, input := range []string{event.ID(), "  " + event.ID() + "  ", strings.ToUpper(event.ID()), strings.ReplaceAll(event.ID(), "-", "")} {
				q := &m29BatchQueryCounter{eventReadQueryer: fixture.db}
				got, err := adapter.SnapshotsForEvent(ctx, q, input)
				expected := want
				if backend.name == "sqlite" && strings.TrimSpace(input) != event.ID() {
					expected = []runtimedelivery.Snapshot{}
				}
				if err != nil || !reflect.DeepEqual(got, expected) {
					t.Fatalf("singleton %q differs from individual canonical rows:\n got=%#v\nwant=%#v\nerr=%v", input, got, expected, err)
				}
				if q.rows != 1 || q.lists != 2 || q.maxArgs != 1 {
					t.Fatalf("singleton should sample DB time once and use two bounded reads: rows=%d lists=%d maxArgs=%d", q.rows, q.lists, q.maxArgs)
				}
			}
			// Noncanonical UUID spelling is a physical SQLite identity, not a lookup
			// alias. PostgreSQL stores the corresponding canonical UUID instead.
			for _, spelling := range []string{"ABCDEF02-ABCD-4ABC-8ABC-ABCDEF012345", "abcdef03abcd4abc8abcabcdef012345"} {
				e := eventtest.ExistingRunRootIngress(spelling, "singleton.spelling", "gateway", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, now)
				route := testEntitylessNodeDeliveryRoute("spelled")
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, e, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				stored := spelling
				if backend.name == "postgres" {
					stored = uuid.MustParse(spelling).String()
				}
				var id string
				if err := fixture.db.QueryRowContext(ctx, `SELECT delivery_id FROM event_deliveries WHERE event_id=$1`, stored).Scan(&id); err != nil {
					t.Fatal(err)
				}
				individual, err := adapter.Snapshot(ctx, fixture.db, id)
				if err != nil || individual.EventID != stored {
					t.Fatalf("stored event spelling: event=%q want=%q err=%v", individual.EventID, stored, err)
				}
				got, err := adapter.SnapshotsForEvent(ctx, fixture.db, spelling)
				if err != nil || !reflect.DeepEqual(got, []runtimedelivery.Snapshot{individual}) {
					t.Fatalf("exact stored spelling read %q: got=%+v err=%v", spelling, got, err)
				}
				got, err = adapter.SnapshotsForEvent(ctx, fixture.db, uuid.MustParse(spelling).String())
				if backend.name == "sqlite" {
					if err != nil || got == nil || len(got) != 0 {
						t.Fatalf("SQLite promoted canonical alias for exact text key: %+v err=%v", got, err)
					}
				} else if err != nil || !reflect.DeepEqual(got, []runtimedelivery.Snapshot{individual}) {
					t.Fatalf("PostgreSQL canonical alias: %+v err=%v", got, err)
				}
			}
			empty := eventtest.ExistingRunRootIngress(uuid.NewString(), "singleton.empty", "gateway", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, now)
			if err := commitSemanticEventFixture(ctx, fixture.store, empty); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{empty.ID(), uuid.NewString()} {
				got, err := adapter.SnapshotsForEvent(ctx, fixture.db, id)
				if err != nil || got == nil || len(got) != 0 {
					t.Fatalf("zero direct membership must be non-nil empty, not event-existence error: %+v err=%v", got, err)
				}
			}
			for _, input := range []string{"", " ", "not-a-uuid"} {
				q := &m29BatchQueryCounter{eventReadQueryer: fixture.db}
				got, err := adapter.SnapshotsForEvent(ctx, q, input)
				if err == nil || got != nil || q.rows+q.lists != 0 {
					t.Fatalf("malformed ID %q reached database or returned partial data: rows=%d lists=%d got=%+v err=%v", input, q.rows, q.lists, got, err)
				}
			}
		})
	}
}

func TestDeliverySingletonSnapshotMembershipRefusalBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			adapter := m29DeliveryAdapter(t, backend.name == "postgres")
			runID, foreignRun := uuid.NewString(), uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			seedAuthorActivityReceiptRun(t, fixture, ctx, foreignRun)
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "singleton.hostile", "gateway", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{testEntitylessNodeDeliveryRoute("first"), testEntitylessNodeDeliveryRoute("second")}); err != nil {
				t.Fatal(err)
			}
			rows, err := fixture.db.QueryContext(ctx, `SELECT delivery_id FROM event_deliveries WHERE event_id=$1 ORDER BY created_at, delivery_id`, event.ID())
			if err != nil {
				t.Fatal(err)
			}
			var ids []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			readErr, closeErr := rows.Err(), rows.Close()
			if readErr != nil || closeErr != nil || len(ids) != 2 {
				t.Fatalf("direct fixture membership: ids=%v read=%v close=%v", ids, readErr, closeErr)
			}
			for _, fault := range []string{"route_identity", "failure_type", "cross_run_join", "deleted_after_membership"} {
				t.Run(fault, func(t *testing.T) {
					tx, err := fixture.db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					// The shadow table permits hostile admission states without changing
					// production constraints; rollback also removes the shadow itself.
					if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE event_deliveries AS SELECT * FROM event_deliveries`); err != nil {
						t.Fatal(err)
					}
					sentinel := runtimedelivery.ErrConflict
					query := `UPDATE event_deliveries SET route_identity=$1 WHERE delivery_id=$2`
					value := "delivery-route-v2:sha256:" + strings.Repeat("f", 64)
					switch fault {
					case "failure_type":
						query, value = `UPDATE event_deliveries SET failure=$1 WHERE delivery_id=$2`, `{"class":[]}`
					case "cross_run_join":
						query, value, sentinel = `UPDATE event_deliveries SET run_id=$1 WHERE delivery_id=$2`, foreignRun, runtimedelivery.ErrNotFound
					case "deleted_after_membership":
						sentinel = runtimedelivery.ErrNotFound
					}
					q := &singletonSnapshotMembershipQueryer{eventReadQueryer: tx}
					if fault == "deleted_after_membership" {
						q.beforeHydration = func() error {
							result, err := tx.ExecContext(ctx, `DELETE FROM event_deliveries WHERE delivery_id=$1`, ids[1])
							if err != nil {
								return err
							}
							count, err := result.RowsAffected()
							if err != nil || count != 1 {
								return fmt.Errorf("delete selected member: count=%d err=%v", count, err)
							}
							return nil
						}
					} else if _, err := tx.ExecContext(ctx, query, value, ids[1]); err != nil {
						t.Fatal(err)
					}
					got, err := adapter.SnapshotsForEvent(ctx, q, event.ID())
					if got != nil || !errors.Is(err, sentinel) || q.lists != 2 {
						t.Fatalf("singleton admitted hostile direct member or partial data: got=%+v lists=%d err=%v want=%v", got, q.lists, err, sentinel)
					}
					if _, err := adapter.Snapshot(ctx, tx, ids[1]); !errors.Is(err, sentinel) {
						t.Fatalf("individual canonical row must independently refuse same member: %v want=%v", err, sentinel)
					}
					if _, err := adapter.Snapshot(ctx, tx, ids[0]); err != nil {
						t.Fatalf("unmodified sibling must remain independently valid: %v", err)
					}
				})
			}
			fresh, err := adapter.SnapshotsForEvent(ctx, fixture.db, event.ID())
			if err != nil || len(fresh) != len(ids) {
				t.Fatalf("rollback/fresh read membership: %+v err=%v", fresh, err)
			}
			for i, id := range ids {
				want, err := adapter.Snapshot(ctx, fixture.db, id)
				if err != nil || !reflect.DeepEqual(fresh[i], want) {
					t.Fatalf("rollback independent row/order equality %s: err=%v", id, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := adapter.SnapshotsForEvent(cancelled, fixture.db, event.ID()); got != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled singleton: got=%+v err=%v", got, err)
			}
		})
	}
}

type singletonSnapshotMembershipQueryer struct {
	eventReadQueryer
	lists           int
	beforeHydration func() error
}

func (q *singletonSnapshotMembershipQueryer) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.lists++
	if q.lists == 2 && q.beforeHydration != nil {
		if err := q.beforeHydration(); err != nil {
			return nil, err
		}
	}
	return q.eventReadQueryer.QueryContext(ctx, query, args...)
}
