package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunLifecycleInsertForkRejectsMissingPersistedBundleBeforeMutation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	forkRunID := uuid.NewString()
	missingHash := "bundle-v1:sha256:" + strings.Repeat("a", 64)

	err := pg.runAuthorActivityMutation(testAuthorActivityContext(), "reject missing persisted fork identity", func(txctx context.Context, tx *sql.Tx) error {
		return storerunlifecycle.InsertFork(
			txctx,
			tx,
			forkRunID,
			RunForkMaterializedStatus,
			uuid.NewString(),
			uuid.NewString(),
			0,
			time.Now().UTC(),
			storerunlifecycle.InsertForkOptions{BundleSourceFact: mustStoreTestPersistedBundleSourceFact(missingHash)},
		)
	})
	if !errors.Is(err, storerunlifecycle.ErrPersistedBundleUnavailable) {
		t.Fatalf("InsertFork error = %v, want ErrPersistedBundleUnavailable", err)
	}
	assertRunRowAbsent(t, db, forkRunID)
	assertNoRunAuthorActivity(t, db, forkRunID)
}

func TestRunForkMaterializerRejectsMissingAndDeletedRequestedPersistedBundleBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		hashDigit     string
		deleteCatalog bool
	}{
		{name: "missing", hashDigit: "b"},
		{name: "deleted", hashDigit: "c", deleteCatalog: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := newTestPostgresStore(t, db)
			ctx := testAuthorActivityContext()
			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Now().UTC()
			seedActivationReadySourceRun(t, db, sourceRunID, entityID, eventID, at)
			captureRunForkTestRevision(t, db, sourceRunID)

			targetHash := "bundle-v1:sha256:" + strings.Repeat(tc.hashDigit, 64)
			if tc.deleteCatalog {
				seedStoreTestPersistedBundle(t, db, targetHash)
				result, err := pg.ApplyBundleDeleteFinalMutation(ctx, runtimebundledelete.FinalMutationRequest{
					OperationName: runtimebundledelete.DefaultOperationName,
					BundleHash:    targetHash,
					RequestedAt:   at,
				})
				if err != nil {
					t.Fatalf("delete requested target bundle: %v", err)
				}
				if !result.Deleted {
					t.Fatalf("delete result = %#v, want deleted target bundle", result)
				}
			}

			_, err := pg.MaterializeRunFork(ctx, RunForkMaterializeRequest{
				SourceRunID:      sourceRunID,
				At:               eventID,
				BundleSourceFact: mustStoreTestPersistedBundleSourceFact(targetHash),
			})
			if !errors.Is(err, storerunlifecycle.ErrPersistedBundleUnavailable) {
				t.Fatalf("MaterializeRunFork error = %v, want ErrPersistedBundleUnavailable", err)
			}
			forkRunID := deterministicRunForkMaterializationID(sourceRunID, eventID)
			assertRunRowAbsent(t, db, forkRunID)
			assertNoRunAuthorActivity(t, db, forkRunID)
			assertNoForkEntityState(t, db, forkRunID)
		})
	}
}

func TestRunLifecycleInsertForkFailsAfterDeleteFirstSerializationWithoutMutation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	targetHash := "bundle-v1:sha256:" + strings.Repeat("d", 64)
	seedStoreTestPersistedBundle(t, db, targetHash)
	deleteTx := beginDeleteFirstBundleTransaction(t, ctx, db, targetHash)
	defer func() { _ = deleteTx.Rollback() }()

	forkRunID := uuid.NewString()
	done := make(chan error, 1)
	go func() {
		done <- pg.runAuthorActivityMutation(ctx, "delete-first fork identity admission", func(txctx context.Context, tx *sql.Tx) error {
			return storerunlifecycle.InsertFork(
				txctx,
				tx,
				forkRunID,
				RunForkMaterializedStatus,
				uuid.NewString(),
				uuid.NewString(),
				0,
				time.Now().UTC(),
				storerunlifecycle.InsertForkOptions{BundleSourceFact: mustStoreTestPersistedBundleSourceFact(targetHash)},
			)
		})
	}()
	waitForPostgresRunAdmissionLock(t, ctx, db, done)
	if err := deleteTx.Commit(); err != nil {
		t.Fatalf("commit delete-first bundle transaction: %v", err)
	}

	err := receiveBundleIdentityOperation(t, done)
	if !errors.Is(err, storerunlifecycle.ErrPersistedBundleUnavailable) {
		t.Fatalf("InsertFork after delete error = %v, want ErrPersistedBundleUnavailable", err)
	}
	assertRunRowAbsent(t, db, forkRunID)
	assertNoRunAuthorActivity(t, db, forkRunID)
}

func TestPostgresStandingRevisionFailsAfterDeleteFirstSerializationWithoutMutation(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := admitTestPostgresStore(t, db)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	workflowStore.ConfigureDeliveryLifecycleStore(selected)
	workflowStore.ConfigurePipelineObligationStore(selected.PipelineObligations())
	ctx := testAuthorActivityRuntimeContext()

	firstHash := "bundle-v1:sha256:" + strings.Repeat("e", 64)
	targetHash := "bundle-v1:sha256:" + strings.Repeat("f", 64)
	seedStoreTestPersistedBundle(t, db, firstHash)
	seedStoreTestPersistedBundle(t, db, targetHash)
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID:  runtimeflowidentity.StandingServiceID("project", "atomic-revision"),
		PackageKey: "project",
		FlowID:     "atomic-revision",
		InstanceID: uuid.NewString(),
		EntityID:   uuid.NewString(),
		Source:     mustStoreTestPersistedBundleSourceFact(firstHash),
	}
	created, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatalf("create standing service: %v", err)
	}
	baselineJournal := countStandingJournalRows(t, db, candidate.ServiceID)
	baselineActivity := countRunAuthorActivity(t, db, created.RunID)
	var baselineRevision int64
	if err := db.QueryRow(`SELECT revision_sequence FROM standing_services WHERE service_id = $1::uuid`, candidate.ServiceID).Scan(&baselineRevision); err != nil {
		t.Fatalf("load standing revision baseline: %v", err)
	}

	deleteTx := beginDeleteFirstBundleTransaction(t, ctx, db, targetHash)
	defer func() { _ = deleteTx.Rollback() }()
	revised := candidate
	revised.Source = mustStoreTestPersistedBundleSourceFact(targetHash)
	done := make(chan error, 1)
	go func() {
		_, err := workflowStore.ReconcileStandingService(ctx, revised)
		done <- err
	}()
	waitForPostgresRunAdmissionLock(t, ctx, db, done)
	if err := deleteTx.Commit(); err != nil {
		t.Fatalf("commit delete-first bundle transaction: %v", err)
	}

	err = receiveBundleIdentityOperation(t, done)
	if !errors.Is(err, storerunlifecycle.ErrPersistedBundleUnavailable) {
		t.Fatalf("standing revision after delete error = %v, want ErrPersistedBundleUnavailable", err)
	}
	var runHash, runSource string
	if err := db.QueryRow(`SELECT bundle_hash, bundle_source FROM runs WHERE run_id = $1::uuid`, created.RunID).Scan(&runHash, &runSource); err != nil {
		t.Fatalf("load standing run identity: %v", err)
	}
	if runHash != firstHash || runSource != storerunlifecycle.BundleSourcePersisted {
		t.Fatalf("standing run identity = %s/%s, want unchanged %s/persisted", runHash, runSource, firstHash)
	}
	var standingHash, standingSource string
	var revisionSequence int64
	if err := db.QueryRow(`
		SELECT current_bundle_hash, current_bundle_source, revision_sequence
		FROM standing_services
		WHERE service_id = $1::uuid
	`, candidate.ServiceID).Scan(&standingHash, &standingSource, &revisionSequence); err != nil {
		t.Fatalf("load standing projection: %v", err)
	}
	if standingHash != firstHash || standingSource != storerunlifecycle.BundleSourcePersisted || revisionSequence != baselineRevision {
		t.Fatalf("standing projection = %s/%s revision=%d, want unchanged %s/persisted revision=%d", standingHash, standingSource, revisionSequence, firstHash, baselineRevision)
	}
	if got := countStandingJournalRows(t, db, candidate.ServiceID); got != baselineJournal {
		t.Fatalf("standing journal rows = %d, want unchanged %d", got, baselineJournal)
	}
	if got := countRunAuthorActivity(t, db, created.RunID); got != baselineActivity {
		t.Fatalf("standing author activity rows = %d, want unchanged %d", got, baselineActivity)
	}
	assertBundleRowAbsent(t, ctx, selected, targetHash)
}

func beginDeleteFirstBundleTransaction(t *testing.T, ctx context.Context, db *sql.DB, bundleHash string) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin delete-first bundle transaction: %v", err)
	}
	var lockedHash string
	if err := tx.QueryRowContext(ctx, `
		SELECT bundle_hash
		FROM bundles
		WHERE bundle_hash = $1
		FOR UPDATE
	`, bundleHash).Scan(&lockedHash); err != nil {
		_ = tx.Rollback()
		t.Fatalf("lock delete-first bundle row: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `LOCK TABLE runs IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("hold delete-first run admission lock: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM bundles WHERE bundle_hash = $1`, bundleHash); err != nil {
		_ = tx.Rollback()
		t.Fatalf("delete bundle under held admission lock: %v", err)
	}
	return tx
}

func waitForPostgresRunAdmissionLock(t *testing.T, ctx context.Context, db *sql.DB, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("persisted bundle mutation completed before delete lock release: %v", err)
		default:
		}
		var waiting bool
		if err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_locks
				WHERE relation = 'runs'::regclass
				  AND mode = 'RowExclusiveLock'
				  AND NOT granted
			)
		`).Scan(&waiting); err != nil {
			t.Fatalf("inspect pending persisted bundle admission lock: %v", err)
		}
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("persisted bundle mutation did not wait on the canonical runs admission lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func receiveBundleIdentityOperation(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("persisted bundle mutation did not finish after delete committed")
		return nil
	}
}

func assertNoRunAuthorActivity(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	if got := countRunAuthorActivity(t, db, runID); got != 0 {
		t.Fatalf("author activity rows for run %s = %d, want 0", runID, got)
	}
}

func countRunAuthorActivity(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM author_activity_occurrences WHERE run_id = $1::uuid`, runID).Scan(&count); err != nil {
		t.Fatalf("count author activity for run %s: %v", runID, err)
	}
	return count
}

func assertNoForkEntityState(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id = $1::uuid`, runID).Scan(&count); err != nil {
		t.Fatalf("count fork entity state for run %s: %v", runID, err)
	}
	if count != 0 {
		t.Fatalf("fork entity state rows for run %s = %d, want 0", runID, count)
	}
}

func countStandingJournalRows(t *testing.T, db *sql.DB, serviceID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM standing_service_journal WHERE service_id = $1::uuid`, serviceID).Scan(&count); err != nil {
		t.Fatalf("count standing journal rows for %s: %v", serviceID, err)
	}
	return count
}
