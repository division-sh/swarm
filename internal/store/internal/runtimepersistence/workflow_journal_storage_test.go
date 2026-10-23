package runtimepersistence

import (
	"context"

	"errors"

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
			ctx = originalStoreFixtureContext(t, ctx, fixture.store)
			run, other := uuid.NewString(), uuid.NewString()
			for _, runID := range []string{run, other} {
				seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				record := pipeline.ActivityAttemptRecord{
					RequestEventID: uuid.NewString(), RunID: runID, ExecutionMode: executionmode.Live,
					ActivityID: uuid.NewString(), Tool: "journal.storage",
					Attempt: 1, SuccessEvent: "fixture.succeeded", FailureEvent: "fixture.failed",
					InputHash: "exact-storage-input", StartedAt: time.Now().UTC(),
				}
				if runID == other {
					record.ExecutionMode = executionmode.Mock
				}
				if _, inserted, err := activityNativeJournalForFixture(t, fixture, runID).StartActivityAttempt(ctx, record); err != nil || !inserted {
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
			if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts WHERE run_id=$1`, run).Scan(&expected.Attempts); err != nil {
				t.Fatal(err)
			}
			mockQuery := `SELECT COUNT(*) FROM author_activity_occurrences WHERE json_extract(projection, '$.execution_mode')='mock' AND source_owner='activity_attempts'`
			if backend.name == "postgres" {
				mockQuery = `SELECT COUNT(*) FROM author_activity_occurrences WHERE projection->>'execution_mode'='mock' AND source_owner='activity_attempts'`
			}
			if err := fixture.db.QueryRowContext(ctx, mockQuery).Scan(&expected.MockAttemptStories); err != nil {
				t.Fatal(err)
			}
			if expected.StoryCount < 2 || expected.StoryHead < 2 {
				t.Fatalf("global story/head control did not materialize both runs: %+v", expected)
			}
			if expected.Attempts != 1 || expected.MockAttemptStories != 1 {
				t.Fatalf("exact-run attempts and global mock stories were conflated: %+v", expected)
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
