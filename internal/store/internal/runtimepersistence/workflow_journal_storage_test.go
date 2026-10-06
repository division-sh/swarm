package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestWorkflowJournalStoragePreservesOriginalSnapshotAndPhysicalCountsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			run, other := uuid.NewString(), uuid.NewString()
			for _, runID := range []string{run, other} {
				seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				record := pipeline.ActivityAttemptRecord{
					RequestEventID: uuid.NewString(), RunID: runID, ExecutionMode: executionmode.Live,
					ActivityID: uuid.NewString(), Tool: "journal.storage", EffectClass: "non_idempotent_write",
					Attempt: 1, SuccessEvent: "fixture.succeeded", FailureEvent: "fixture.failed",
					InputHash: "exact-storage-input", StartedAt: time.Now().UTC(),
				}
				if _, inserted, err := fixture.store.(pipeline.ActivityAttemptJournal).StartActivityAttempt(ctx, record); err != nil || !inserted {
					t.Fatalf("start native journal: inserted=%t err=%v", inserted, err)
				}
			}
			var expected WorkflowJournalStorage
			if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM author_activity_occurrences`).Scan(&expected.StoryCount); err != nil {
				t.Fatal(err)
			}
			if err := fixture.db.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id=1`).Scan(&expected.StoryHead); err != nil {
				t.Fatal(err)
			}
			if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`, run).Scan(&expected.RunForkRevisions); err != nil {
				t.Fatal(err)
			}
			if expected.StoryCount < 2 || expected.StoryHead < 2 {
				t.Fatalf("global story/head control did not materialize both runs: %+v", expected)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if actual, err := ReadWorkflowJournalStorageForTest(ctx, fixture.store, run); err != nil || actual != expected {
				t.Fatalf("journal physical witness changed: got=%+v want=%+v err=%v", actual, expected, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("journal witness escaped original read snapshot: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadWorkflowJournalStorageForTest(cancelled, fixture.store, run); !errors.Is(err, context.Canceled) || got != (WorkflowJournalStorage{}) {
				t.Fatalf("cancelled witness retained partial evidence: %+v %v", got, err)
			}
		})
	}
}

func TestWorkflowJournalStorageRefusesInvalidClosedAndLateFailingOwnersBothStores(t *testing.T) {
	refuse := func(t *testing.T, ctx context.Context, selected any, run string) {
		t.Helper()
		if got, err := ReadWorkflowJournalStorageForTest(ctx, selected, run); err == nil || !reflect.DeepEqual(got, WorkflowJournalStorage{}) {
			t.Fatalf("invalid journal owner supplied evidence: %+v %v", got, err)
		}
	}
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(t, context.Background(), selected, uuid.NewString())
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			run := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, run)
			for _, invalid := range []string{"", "invalid", uuid.Nil.String()} {
				refuse(t, ctx, fixture.store, invalid)
			}
			// Ledger failure follows successful story/head reads. No populated
			// prefix may escape the original transaction's failure.
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE run_fork_revisions RENAME TO unavailable_journal_frontier`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			refuse(t, ctx, fixture.store, run)
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_journal_frontier RENAME TO run_fork_revisions`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadWorkflowJournalStorageForTest(ctx, fixture.store, run); err != nil {
				t.Fatalf("fault restoration did not preserve the original owner: %v", err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE author_activity_order RENAME TO unavailable_journal_order`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			refuse(t, ctx, fixture.store, run)
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_journal_order RENAME TO author_activity_order`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(t, ctx, fixture.store, run)
		})
	}
}
