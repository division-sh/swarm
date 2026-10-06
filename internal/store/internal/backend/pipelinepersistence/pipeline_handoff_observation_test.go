package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const frozenPipelineHandoffCount = `SELECT COUNT(*) FROM event_deliveries d WHERE d.run_id=$1 AND
	(d.status IN ('pending','in_progress') OR d.continuation_handoff_at IS NULL OR NOT EXISTS
	(SELECT 1 FROM event_receipts r WHERE r.event_id=d.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'))`

type handoffReadRecorder struct {
	eventReadQueryer
	beforeReceipt func()
	receiptReads  int
}

func (q *handoffReadRecorder) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	q.receiptReads++
	if q.beforeReceipt != nil {
		callback := q.beforeReceipt
		q.beforeReceipt = nil
		callback()
	}
	return q.eventReadQueryer.QueryRowContext(ctx, query, args...)
}

func handoffObservationDB(t *testing.T, postgres bool) *sql.DB {
	t.Helper()
	var db *sql.DB
	if postgres {
		_, db, _ = testutil.StartEmptyPostgres(t)
	} else {
		var err error
		db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "handoff.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
			t.Fatal(err)
		}
	}
	db.SetMaxOpenConns(2)
	for _, query := range []string{
		`CREATE TABLE event_deliveries (event_id TEXT NOT NULL, run_id TEXT NOT NULL, status TEXT NOT NULL, continuation_handoff_at TIMESTAMP)`,
		`CREATE TABLE event_receipts (event_id TEXT NOT NULL, subscriber_type TEXT NOT NULL, subscriber_id TEXT NOT NULL, side_effects TEXT)`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestPipelineHandoffTypedCompositionMatchesFrozenQueryBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			db := handoffObservationDB(t, postgres)
			run, foreignRun, event, foreignEvent := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			for _, cut := range []string{"empty", "complete", "pending", "in_progress", "missing_handoff", "missing_receipt", "active_missing_handoff", "active_missing_receipt", "handoff_missing_receipt", "overlapping_arms", "shared_event", "shared_missing_receipt", "foreign_type", "foreign_subscriber", "foreign_event", "foreign_run", "malformed_present_receipt"} {
				t.Run(cut, func(t *testing.T) {
					for _, query := range []string{`DELETE FROM event_deliveries`, `DELETE FROM event_receipts`} {
						if _, err := db.Exec(query); err != nil {
							t.Fatal(err)
						}
					}
					status, rowRun, handoff := "delivered", run, any(time.Now().UTC())
					receiptEvent, receiptType, receiptSubscriber, payload := event, "platform", "pipeline", "{}"
					want := 0
					switch cut {
					case "pending", "in_progress":
						status, want = cut, 1
					case "missing_handoff":
						handoff, want = nil, 1
					case "missing_receipt", "foreign_type", "foreign_subscriber", "foreign_event":
						want = 1
					case "active_missing_handoff":
						status, handoff, want = "pending", nil, 1
					case "active_missing_receipt":
						status, want = "pending", 1
					case "handoff_missing_receipt":
						handoff, want = nil, 1
					case "overlapping_arms":
						status, handoff, want = "pending", nil, 1
					case "shared_event", "shared_missing_receipt":
						want = 2
					case "foreign_run":
						rowRun = foreignRun
					case "malformed_present_receipt":
						payload = "not a receipt payload"
					}
					if cut != "empty" {
						if _, err := db.Exec(`INSERT INTO event_deliveries VALUES ($1,$2,$3,$4)`, event, rowRun, status, handoff); err != nil {
							t.Fatal(err)
						}
					}
					if cut == "shared_event" {
						if _, err := db.Exec(`INSERT INTO event_deliveries VALUES ($1,$2,'pending',CURRENT_TIMESTAMP),($1,$2,'delivered',NULL)`, event, run); err != nil {
							t.Fatal(err)
						}
					} else if cut == "shared_missing_receipt" {
						if _, err := db.Exec(`INSERT INTO event_deliveries VALUES ($1,$2,'delivered',CURRENT_TIMESTAMP)`, event, run); err != nil {
							t.Fatal(err)
						}
					}
					switch cut {
					case "foreign_type":
						receiptType = "agent"
					case "foreign_subscriber":
						receiptSubscriber = "not-pipeline"
					case "foreign_event":
						receiptEvent = foreignEvent
					}
					if cut != "missing_receipt" && cut != "active_missing_receipt" && cut != "handoff_missing_receipt" && cut != "overlapping_arms" && cut != "shared_missing_receipt" {
						if _, err := db.Exec(`INSERT INTO event_receipts VALUES ($1,$2,$3,$4)`, receiptEvent, receiptType, receiptSubscriber, payload); err != nil {
							t.Fatal(err)
						}
					}
					ctx := context.Background()
					tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					recorder := &handoffReadRecorder{eventReadQueryer: tx}
					got, err := ReadIncompletePipelineHandoffCount(ctx, recorder, run)
					var original int
					originalErr := tx.QueryRowContext(ctx, frozenPipelineHandoffCount, run).Scan(&original)
					if err != nil || originalErr != nil || got != want || original != want || recorder.receiptReads != 1 {
						t.Fatalf("cut=%s typed=%d err=%v original=%d err=%v want=%d receipt_reads=%d", cut, got, err, original, originalErr, want, recorder.receiptReads)
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestPipelineHandoffTypedCompositionRetainsOneSnapshotBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			db := handoffObservationDB(t, postgres)
			run, event := uuid.NewString(), uuid.NewString()
			if _, err := db.Exec(`INSERT INTO event_deliveries VALUES ($1,$2,'delivered',CURRENT_TIMESTAMP)`, event, run); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO event_receipts VALUES ($1,'platform','pipeline','{}')`, event); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			recorder := &handoffReadRecorder{eventReadQueryer: tx, beforeReceipt: func() {
				// The delivery SELECT has already fixed the snapshot. Commit the
				// missing receipt from another connection before the receipt read.
				if _, err := db.Exec(`DELETE FROM event_receipts WHERE event_id=$1`, event); err != nil {
					t.Fatal(err)
				}
			}}
			got, err := ReadIncompletePipelineHandoffCount(ctx, recorder, run)
			var original int
			originalErr := tx.QueryRowContext(ctx, frozenPipelineHandoffCount, run).Scan(&original)
			if err != nil || originalErr != nil || got != 0 || original != 0 {
				t.Fatalf("split read escaped its snapshot: typed=%d err=%v original=%d err=%v", got, err, original, originalErr)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if fresh, err := ReadIncompletePipelineHandoffCount(ctx, db, run); err != nil || fresh != 1 {
				t.Fatalf("control did not observe the later commit: %d %v", fresh, err)
			}
		})
	}
}

func TestPipelineHandoffTypedCompositionNeverReturnsPartialEvidenceBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			db := handoffObservationDB(t, postgres)
			run := uuid.NewString()
			for range 2 {
				if _, err := db.Exec(`INSERT INTO event_deliveries VALUES ($1,$2,'pending',NULL)`, uuid.NewString(), run); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			recorder := &handoffReadRecorder{eventReadQueryer: db}
			recorder.beforeReceipt = func() {
				recorder.beforeReceipt = cancel
			}
			if count, err := ReadIncompletePipelineHandoffCount(ctx, recorder, run); count != 0 || !errors.Is(err, context.Canceled) || recorder.receiptReads != 2 {
				t.Fatalf("partial count escaped cancellation: count=%d err=%v reads=%d", count, err, recorder.receiptReads)
			}
		})
	}
}
