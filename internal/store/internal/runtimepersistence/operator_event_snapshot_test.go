package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type operatorSnapshotFixture struct {
	authorActivityReceiptFixture
	writer *sql.DB
	probe  *operatorSnapshotProbe
	ids    []string
	before []operatorread.OperatorEventFull
	opts   operatorread.OperatorEventListOptions
	base   time.Time
}

func newOperatorSnapshotFixture(t *testing.T, postgres bool) operatorSnapshotFixture {
	t.Helper()
	var connector driver.Connector
	if postgres {
		dsn, _, _ := testutil.StartPostgres(t)
		var err error
		connector, err = pq.NewConnector(dsn)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		connector = diagnosticSQLiteConnector{filepath.Join(t.TempDir(), "operator-snapshot.db") + "?_pragma=busy_timeout(1000)&_pragma=journal_mode(WAL)"}
	}
	p := &operatorSnapshotProbe{}
	db := sql.OpenDB(operatorSnapshotConnector{Connector: connector, probe: p})
	writer := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close(); _ = writer.Close() })
	f := operatorSnapshotFixture{writer: writer, probe: p, base: time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)}
	f.db = db
	ctx := testAuthorActivityContext()
	if postgres {
		store := admitTestPostgresStore(t, db)
		registerTestAuthorActivityCatalog(t, store)
		f.store, f.dialect = store, authoractivityfixture.DialectPostgres
	} else {
		store := NewSQLiteRuntimeStoreForTest(db)
		if err := store.BootstrapSchema(ctx, canonicalSchemaBootstrapTestRequest(t)); err != nil {
			t.Fatal(err)
		}
		store.SetEventPayloadAdmitter(storeTestPayloadAdmitter)
		registerTestAuthorActivityCatalog(t, store)
		f.store, f.dialect = store, authoractivityfixture.DialectSQLite
		var mode string
		if err := writer.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
			t.Fatalf("concurrent fixture must use real WAL: mode=%q err=%v", mode, err)
		}
	}
	// A helper escaping to the reader pool cannot silently get another connection.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	writer.SetMaxOpenConns(1)
	if err := writer.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	seedAuthorActivityReceiptRun(t, f.authorActivityReceiptFixture, ctx, runID)
	failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "snapshot_before", nil)
	for i := 0; i < 3; i++ {
		id := uuid.NewString()
		f.ids = append(f.ids, id)
		route := testEntitylessNodeDeliveryRoute("keep")
		if i == 0 {
			route = testEntitylessNodeDeliveryRoute("drop")
		}
		event := eventtest.ExistingRunRootIngress(id, "snapshot.event", "gateway", "", []byte(`{"snapshot":"before"}`), 0, runID, events.EventEnvelope{}, f.base.Add(time.Duration(i)*time.Second))
		if err := commitSemanticEventFixtureWithRoutes(ctx, f.store, event, []events.DeliveryRoute{route}); err != nil {
			t.Fatal(err)
		}
		seedDeliveryStateFixture(t, ctx, f.store, event, route, runtimedelivery.StateExhausted, &failure)
		full, err := f.store.(routeSettlementOperatorStore).LoadOperatorEvent(ctx, id)
		if err != nil || len(full.Deliveries) != 1 || len(full.DeadLetters) != 1 || len(full.Deliveries[0].DeadLetters) != 1 {
			t.Fatalf("complete before evidence: %+v err=%v", full, err)
		}
		f.before = append(f.before, full)
	}
	f.opts = operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{
		RunID: runID, SubscriberType: "node", SubscriberID: testEntitylessNodeDeliveryRoute("keep").Recipient.ID(),
	}, Order: "asc", Limit: 1}
	return f
}

// Commit all changed components atomically. These are valid projection fixtures,
// not a proposed business mutation API; fresh canonical public reads must admit
// every changed row after the interleaving.
func (f operatorSnapshotFixture) commitChange(ctx context.Context) error {
	tx, err := f.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "snapshot_after", map[string]any{"snapshot": "after"})
	raw, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		return err
	}
	for _, id := range f.ids[1:] {
		payload := []byte(`{"snapshot":"after"}`)
		if _, err := tx.ExecContext(ctx, `UPDATE events SET payload=$1,payload_bytes=$2 WHERE event_id=$3`, string(payload), payload, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET failure=$1,reason_code='snapshot_after' WHERE event_id=$2`, string(raw), id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE dead_letters SET failure=$1,handler_node='snapshot-after-node' WHERE original_event_id=$2`, string(raw), id); err != nil {
			return err
		}
	}
	// Move the not-yet-selected lookahead before the cursor. The first invocation
	// must still select it at the old position; the next page/request must not.
	if _, err := tx.ExecContext(ctx, `UPDATE events SET created_at=$1 WHERE event_id=$2`, f.base.Add(-time.Second), f.ids[2]); err != nil {
		return err
	}
	return tx.Commit()
}

func assertOperatorSnapshotTransaction(t *testing.T, p *operatorSnapshotProbe, postgres bool) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.options) != 1 || !p.options[0].ReadOnly || p.active != 0 || p.finished != 1 || p.outside != 0 || p.writes != 0 {
		t.Fatalf("read ownership: options=%+v active=%d finished=%d outside=%d writes=%d", p.options, p.active, p.finished, p.outside, p.writes)
	}
	if postgres && p.options[0].Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) {
		t.Fatalf("PostgreSQL read isolation=%v", p.options[0])
	}
}

func awaitOperatorSnapshotBarrier(t *testing.T, ctx context.Context, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("component read barrier not reached: %v", ctx.Err())
	}
}

func TestOperatorEventSnapshotInterleavingBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, operation := range []string{"list", "get"} {
			t.Run(fmt.Sprintf("postgres_%v/%s", postgres, operation), func(t *testing.T) {
				f := newOperatorSnapshotFixture(t, postgres)
				store := f.store.(routeSettlementOperatorStore)
				ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
				defer cancel()
				kind := "event"
				if operation == "list" {
					kind = "candidate"
				}
				entered, release := f.probe.arm(kind, 1, nil, nil)
				defer release()
				var page operatorread.OperatorEventListResult
				var event operatorread.OperatorEventFull
				done := make(chan error, 1)
				go func() {
					var err error
					if operation == "list" {
						page, err = store.ListOperatorEvents(ctx, f.opts)
					} else {
						event, err = store.LoadOperatorEvent(ctx, f.ids[1])
					}
					done <- err
				}()
				awaitOperatorSnapshotBarrier(t, ctx, entered)
				// The read remains held until after a real writer COMMIT. SQLite
				// therefore proves WAL progress, not a writer merely being launched.
				if err := f.commitChange(ctx); err != nil {
					t.Fatalf("writer could not commit while snapshot was pinned: %v", err)
				}
				release()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				assertOperatorSnapshotTransaction(t, f.probe, postgres)
				if f.probe.seen["membership"] == 0 || f.probe.seen["delivery"] == 0 {
					t.Fatalf("snapshot omitted direct membership/canonical hydration: %v", f.probe.seen)
				}
				f.probe.disarm()
				if operation == "list" {
					if len(page.Events) != 1 || page.NextCursor == "" {
						t.Fatalf("pre-change candidate/lookahead lost: %+v", page)
					}
					assertOperatorBatchScalarEqual(t, page.Events[0], f.before[1])
					if !reflect.DeepEqual(f.probe.candidates, f.ids) {
						t.Fatalf("consumed prefix/lookahead=%v want=%v", f.probe.candidates, f.ids)
					}
					later := f.opts
					later.Cursor = page.NextCursor
					tail, err := store.ListOperatorEvents(ctx, later)
					if err != nil || len(tail.Events) != 0 || tail.NextCursor != "" {
						t.Fatalf("cursor request retained old snapshot: %+v err=%v", tail, err)
					}
				} else {
					assertOperatorBatchScalarEqual(t, event, f.before[1])
				}
				fresh, err := store.LoadOperatorEvent(ctx, f.ids[1])
				if err != nil || fresh.Payload["snapshot"] != "after" || fresh.Deliveries[0].ReasonCode != "snapshot_after" || fresh.Deliveries[0].Failure.Detail.Code != "snapshot_after" || fresh.DeadLetters[0].HandlerNode != "snapshot-after-node" || fresh.Deliveries[0].DeadLetters[0].Failure.Detail.Code != "snapshot_after" {
					t.Fatalf("fresh invocation lost committed evidence: %+v err=%v", fresh, err)
				}
				freshPage, err := store.ListOperatorEvents(ctx, f.opts)
				if err != nil || len(freshPage.Events) != 1 || freshPage.Events[0].EventID != f.ids[2] || freshPage.Events[0].Payload["snapshot"] != "after" || freshPage.NextCursor == "" {
					t.Fatalf("fresh candidate order/evidence: %+v err=%v", freshPage, err)
				}
			})
		}
	}
}

func TestOperatorEventSnapshotFailureReleaseBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(fmt.Sprintf("postgres_%v", postgres), func(t *testing.T) {
			f := newOperatorSnapshotFixture(t, postgres)
			store := f.store.(routeSettlementOperatorStore)
			for _, operation := range []string{"list", "get"} {
				for _, mode := range []string{"cancel", "read_failure", "commit_failure"} {
					t.Run(operation+"/"+mode, func(t *testing.T) {
						ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
						defer cancel()
						kind, occurrence := "deadletter", 1
						if operation == "list" {
							kind, occurrence = "candidate", 2 // One matching event already assembled.
						}
						failure := errors.New("operator snapshot injected failure")
						var readFailure, commitFailure error
						if mode == "read_failure" {
							readFailure = failure
						} else if mode == "commit_failure" {
							kind, commitFailure = "none", failure
						}
						entered, release := f.probe.arm(kind, occurrence, readFailure, commitFailure)
						defer release()
						var page operatorread.OperatorEventListResult
						var event operatorread.OperatorEventFull
						done := make(chan error, 1)
						go func() {
							var err error
							if operation == "list" {
								page, err = store.ListOperatorEvents(ctx, f.opts)
							} else {
								event, err = store.LoadOperatorEvent(ctx, f.ids[1])
							}
							done <- err
						}()
						if mode == "cancel" {
							awaitOperatorSnapshotBarrier(t, ctx, entered)
							cancel()
							failure = context.Canceled
						}
						err := <-done
						if !errors.Is(err, failure) || !reflect.DeepEqual(page, operatorread.OperatorEventListResult{}) || !reflect.DeepEqual(event, operatorread.OperatorEventFull{}) {
							t.Fatalf("partial result or lost failure: page=%+v event=%+v err=%v", page, event, err)
						}
						assertOperatorSnapshotTransaction(t, f.probe, postgres)
						f.probe.disarm()
						progress, stop := context.WithTimeout(testAuthorActivityContext(), 5*time.Second)
						defer stop()
						// A successful fresh read on this one-connection pool proves the
						// failed invocation released/settled its retained transaction.
						fresh, err := store.LoadOperatorEvent(progress, f.ids[1])
						if err != nil {
							t.Fatalf("read connection not released: %v", err)
						}
						assertOperatorBatchScalarEqual(t, fresh, f.before[1])
						if _, err := f.writer.ExecContext(progress, `UPDATE events SET created_at=created_at WHERE event_id=$1`, f.ids[1]); err != nil {
							t.Fatalf("writer progress after failure: %v", err)
						}
					})
				}
			}
		})
	}
}
