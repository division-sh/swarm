package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunLifecycleInsertForkRejectsMissingPersistedBundleBeforeMutation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	forkRunID := uuid.NewString()
	missingHash := "bundle-v2:sha256:" + strings.Repeat("a", 64)

	err := pg.runPrivateAuthorActivityMutation(testAuthorActivityContext(), func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		return pg.runForkPostgresOwner.InsertRunForkRunTx(txctx, tx, story, forkRunID, uuid.NewString(), uuid.NewString(), 0, time.Now().UTC(),
			mustStoreTestSourceArtifactFact(missingHash))
	})
	if !errors.Is(err, storerunlifecycle.ErrSourceArtifactUnavailable) {
		t.Fatalf("InsertFork error = %v, want ErrSourceArtifactUnavailable", err)
	}
	assertRunRowAbsent(t, db, forkRunID)
	assertNoRunAuthorActivity(t, db, forkRunID)
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
