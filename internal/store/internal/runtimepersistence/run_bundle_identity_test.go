package runtimepersistence

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

var testCanonicalSourceArtifact = storeTestSourceArtifact("canonical-run-bundle")
var testCanonicalBundleHash = testCanonicalSourceArtifact.BundleHash()

func TestPostgresStore_RunConsumesCanonicalSourceArtifactIdentity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	seedStoreTestPersistedArtifact(t, db, testCanonicalSourceArtifact)

	runID := uuid.NewString()
	ctx := testAuthorActivityContextForBundle(testCanonicalBundleHash)
	registerTestAuthorActivityCatalogForContext(t, pg, ctx)
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(
		uuid.NewString(), "scan.requested", "test", "", []byte(`{}`), 0,
		runID, "", events.EventEnvelope{}, time.Now().UTC(),
	)); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	var gotHash string
	if err := db.QueryRow(`SELECT bundle_hash FROM runs WHERE run_id = $1::uuid`, runID).Scan(&gotHash); err != nil {
		t.Fatalf("load run bundle identity: %v", err)
	}
	if gotHash != testCanonicalBundleHash {
		t.Fatalf("bundle identity = %q, want %q", gotHash, testCanonicalBundleHash)
	}
}

func TestRunLifecycleOwnerRejectsSourceWithoutArtifact(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	runID := uuid.NewString()

	_, err := pg.CreateRun(testAuthorActivityContext(), storerunlifecycle.CreateRequest{
		RunID: runID, Origin: storerunlifecycle.ScenarioSetupRunOrigin(),
		Source: mustStoreTestSourceArtifactFact(testCanonicalBundleHash), StartedAt: time.Now().UTC(),
	})
	if !errors.Is(err, storerunlifecycle.ErrSourceArtifactUnavailable) {
		t.Fatalf("EnsureActive error = %v, want ErrSourceArtifactUnavailable", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`, runID).Scan(&count); err != nil {
		t.Fatalf("count run rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("run rows = %d, want 0", count)
	}
}

func assertRunRowAbsent(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`, runID).Scan(&count); err != nil {
		t.Fatalf("count run rows for %s: %v", runID, err)
	}
	if count != 0 {
		t.Fatalf("run rows for %s = %d, want 0", runID, count)
	}
}
