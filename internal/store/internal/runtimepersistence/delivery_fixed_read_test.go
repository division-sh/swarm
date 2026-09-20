package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
	"modernc.org/sqlite"
)

func deliveryFixedReadFixture(t *testing.T) (*SQLiteRuntimeStore, *sql.DB, string, []string) {
	t.Helper()
	fixture := openSQLiteAuthorActivityReceiptFixture(t)
	s := fixture.store.(*SQLiteRuntimeStore)
	db := fixture.db
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	states := []runtimedelivery.State{runtimedelivery.StateQueued, runtimedelivery.StateLaunching, runtimedelivery.StateRetrying, runtimedelivery.StateDelivered, runtimedelivery.StateExhausted}
	routes := make([]events.DeliveryRoute, len(states))
	for i := range routes {
		routes[i] = testEntitylessNodeDeliveryRoute(fmt.Sprintf("fixed-%d", i))
	}
	e := eventtest.ExistingRunRootIngress(uuid.NewString(), "fixed.read", "gateway", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixtureWithRoutes(ctx, s, e, routes); err != nil {
		t.Fatal(err)
	}
	failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "fixed_failure", map[string]any{"proof": "fresh"})
	ids := make([]string, len(routes))
	for i, route := range routes {
		snapshot := seedDeliveryStateFixture(t, ctx, s, e, route, states[i], &failure)
		ids[i] = snapshot.DeliveryID
		at := time.Now().UTC().Add(-time.Hour)
		setSQLiteDeliveryFixtureTimes(t, ctx, db, snapshot, at, at.Add(time.Minute))
	}
	return s, db, e.ID(), ids
}

func equalDeliveryReadErrors(t *testing.T, raw, prepared error) {
	t.Helper()
	if fmt.Sprint(raw) != fmt.Sprint(prepared) || reflect.TypeOf(raw) != reflect.TypeOf(prepared) {
		t.Fatalf("raw/prepared errors differ: %T %v / %T %v", raw, raw, prepared, prepared)
	}
	for _, sentinel := range []error{runtimedelivery.ErrConflict, runtimedelivery.ErrNotFound, context.Canceled, context.DeadlineExceeded, sql.ErrNoRows} {
		if errors.Is(raw, sentinel) != errors.Is(prepared, sentinel) {
			t.Fatalf("sentinel %v differs: %v / %v", sentinel, raw, prepared)
		}
	}
	var r, p *sqlite.Error
	if errors.As(raw, &r) != errors.As(prepared, &p) || r != nil && r.Code() != p.Code() {
		t.Fatalf("native SQLite codes differ: %v / %v", raw, prepared)
	}
}

func TestDeliveryFixedReadDifferential(t *testing.T) {
	s, db, eventID, ids := deliveryFixedReadFixture(t)
	raw := m29DeliveryAdapter(t, false) // No slots: the exact pre-change reader.
	ctx := context.Background()
	compare := func(ctx context.Context, eventID string, ids []string) {
		t.Helper()
		want, wantErr := raw.SnapshotsForEvent(ctx, db, eventID)
		got, gotErr := s.DeliverySnapshotsForEvent(ctx, eventID)
		equalDeliveryReadErrors(t, wantErr, gotErr)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("singleton snapshots/order differ: raw=%+v prepared=%+v", want, got)
		}
		for _, id := range ids {
			want, wantErr := raw.Snapshot(ctx, db, id)
			got, gotErr := s.Snapshot(ctx, id)
			equalDeliveryReadErrors(t, wantErr, gotErr)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("delivery %s differs: raw=%+v prepared=%+v", id, want, got)
			}
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	compare(cancelled, eventID, ids) // A canceled cold observation must not poison slots.
	compare(ctx, eventID, ids)
	compare(cancelled, eventID, ids)
	// Distinct argument bindings must not leak through the same native handles.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	var readers sync.WaitGroup
	readErrors := make(chan error, 12)
	for i := 0; i < 12; i++ {
		readers.Add(1)
		go func(absent bool) {
			defer readers.Done()
			id, count := eventID, len(ids)
			if absent {
				id, count = uuid.NewString(), 0
			}
			for j := 0; j < 3; j++ {
				snapshots, err := s.DeliverySnapshotsForEvent(ctx, id)
				if err != nil || len(snapshots) != count {
					readErrors <- fmt.Errorf("concurrent event %s: count=%d want=%d err=%v", id, len(snapshots), count, err)
					return
				}
				for _, snapshot := range snapshots {
					if snapshot.EventID != id {
						readErrors <- fmt.Errorf("concurrent event %s received %s", id, snapshot.EventID)
						return
					}
				}
			}
		}(i%2 == 0)
	}
	readers.Wait()
	close(readErrors)
	for err := range readErrors {
		t.Error(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, input := range []string{"", "bad-id", uuid.NewString(), "  " + eventID + "  ", strings.ToUpper(eventID), strings.ReplaceAll(eventID, "-", "")} {
		compare(ctx, input, []string{input})
	}
	for _, fault := range []struct{ name, sql string }{
		{"route", `UPDATE event_deliveries SET route_identity='bad' WHERE delivery_id=?`},
		{"target", `UPDATE event_deliveries SET delivery_target_route='[]' WHERE delivery_id=?`},
		{"failure", `UPDATE event_deliveries SET failure='{"class":[]}' WHERE delivery_id=?`},
		{"timestamp", `UPDATE event_deliveries SET created_at='broken' WHERE delivery_id=?`},
		{"authority", `UPDATE event_deliveries SET execution_authority_generation=0 WHERE delivery_id=?`},
		{"retry", `UPDATE event_deliveries SET max_retries=-1 WHERE delivery_id=?`},
		{"join", `UPDATE event_deliveries SET run_id='foreign' WHERE delivery_id=?`},
		{"fresh", `UPDATE event_deliveries SET execution_authority_id='fresh-owner' WHERE delivery_id=?`},
		{"delete", `DELETE FROM event_deliveries WHERE delivery_id=?`},
	} {
		t.Run(fault.name, func(t *testing.T) {
			// Pool reads must see this connection-local shadow after slots are warm.
			if _, err := db.Exec(`CREATE TEMP TABLE event_deliveries AS SELECT * FROM main.event_deliveries`); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := db.Exec(`DROP TABLE temp.event_deliveries`); err != nil {
					t.Error(err)
				}
			}()
			if _, err := db.Exec(fault.sql, ids[0]); err != nil {
				t.Fatal(err)
			}
			compare(ctx, eventID, ids)
		})
		compare(ctx, eventID, ids)
	}
	for _, ddl := range []string{
		`ALTER TABLE event_delivery_handler_rule_selections RENAME COLUMN display_label TO missing_label`,
		`ALTER TABLE event_delivery_handler_rule_selections RENAME COLUMN missing_label TO display_label`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
		compare(ctx, eventID, ids)
	}
	// A transaction sees its uncommitted write, not the owner's prepared pool.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE event_deliveries SET execution_authority_id='transaction-local' WHERE delivery_id=?`, ids[0]); err != nil {
		t.Fatal(err)
	}
	want, wantErr := raw.SnapshotsForEvent(ctx, tx, eventID)
	got, gotErr := s.deliverySQLiteOwner.DeliverySnapshotsForEventTx(ctx, tx, eventID)
	equalDeliveryReadErrors(t, wantErr, gotErr)
	if !reflect.DeepEqual(want, got) {
		t.Fatal("transaction-local read changed")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	compare(ctx, eventID, ids)
	// Retire the native connection, not just its logical pool checkout.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var retired any
	if err := conn.Raw(func(native any) error { retired = native; return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
		t.Fatal(err)
	}
	_ = conn.Close()
	conn, err = db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(native any) error {
		if native == retired {
			return errors.New("retired connection reused")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `UPDATE event_deliveries SET execution_authority_id='replacement-owner' WHERE delivery_id=?`, ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	compare(ctx, eventID, ids)
	// Another complete store has independent slots, even with identical SQL.
	s2, _, event2, _ := deliveryFixedReadFixture(t)
	if got, err := s2.DeliverySnapshotsForEvent(ctx, event2); err != nil || len(got) != len(ids) {
		t.Fatalf("second pool: %v %v", got, err)
	}
	compare(ctx, eventID, ids)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	compare(ctx, eventID, ids)
}
