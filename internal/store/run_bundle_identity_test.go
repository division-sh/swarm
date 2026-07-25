package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const testCanonicalBundleHash = "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"

func TestPostgresStore_RunBundleSourceConsumesCanonicalSourceFact(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	if _, err := db.ExecContext(testAuthorActivityContext(), `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, testCanonicalBundleHash); err != nil {
		t.Fatalf("seed canonical bundle row: %v", err)
	}

	for _, tc := range []struct {
		name       string
		bundleHash string
		source     string
	}{
		{name: "persisted", bundleHash: testCanonicalBundleHash, source: storerunlifecycle.BundleSourcePersisted},
		{name: "ephemeral", bundleHash: "bundle-v1:sha256:3333333333333333333333333333333333333333333333333333333333333333", source: storerunlifecycle.BundleSourceEphemeral},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runID := uuid.NewString()
			ctx := testAuthorActivityContextForBundle(tc.bundleHash)
			registerTestAuthorActivityCatalogForContext(t, pg, ctx)
			if tc.source == storerunlifecycle.BundleSourcePersisted {
				ctx = withStoreTestPersistedBundleSource(ctx, tc.bundleHash)
			} else {
				ctx = withStoreTestEphemeralBundleSource(ctx, tc.bundleHash)
			}
			if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(
				uuid.NewString(),
				events.EventType("scan."+tc.name),
				"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
			)); err != nil {
				t.Fatalf("AppendEvent: %v", err)
			}
			assertRunCanonicalBundleIdentity(t, db, runID, tc.bundleHash, tc.source)
		})
	}
}

func TestRunLifecycleOwnerRejectsPersistedSourceWithoutBundleRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	runID := uuid.NewString()

	err := pg.runAuthorActivityMutation(testAuthorActivityContext(), "test ensure missing persisted run bundle", func(txctx context.Context, tx *sql.Tx) error {
		return storerunlifecycle.EnsureActive(txctx, tx, runID, "", "", storerunlifecycle.EnsureActiveOptions{
			HasStartedAtCol:  true,
			BundleSourceFact: mustStoreTestPersistedBundleSourceFact(testCanonicalBundleHash),
		})
	})
	if !errors.Is(err, storerunlifecycle.ErrPersistedBundleUnavailable) {
		t.Fatalf("EnsureActive error = %v, want ErrPersistedBundleUnavailable", err)
	}
	assertRunRowAbsent(t, db, runID)
}

func TestPostgresStore_ActiveRunBundleAvailabilityConflicts(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	persistedMissingRunID := uuid.NewString()
	deletedRunID := uuid.NewString()
	completedDeletedRunID := uuid.NewString()
	for _, seed := range []struct {
		runID  string
		status string
		hash   string
		source string
	}{
		{runID: persistedMissingRunID, status: "running", hash: "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222", source: storerunlifecycle.BundleSourcePersisted},
		{runID: deletedRunID, status: "paused", hash: testCanonicalBundleHash, source: storerunlifecycle.BundleSourceDeleted},
		{runID: completedDeletedRunID, status: "completed", hash: testCanonicalBundleHash, source: storerunlifecycle.BundleSourceDeleted},
	} {
		if _, err := db.ExecContext(testAuthorActivityContext(), `
			INSERT INTO runs (run_id, status, bundle_hash, bundle_source)
			VALUES ($1::uuid, $2, $3, $4)
		`, seed.runID, seed.status, seed.hash, seed.source); err != nil {
			t.Fatalf("seed run %s: %v", seed.runID, err)
		}
	}

	conflicts, err := pg.ActiveRunBundleAvailabilityConflicts(testAuthorActivityContext())
	if err != nil {
		t.Fatalf("ActiveRunBundleAvailabilityConflicts: %v", err)
	}
	if len(conflicts) != 2 {
		t.Fatalf("conflicts = %#v, want persisted-missing and deleted active conflicts", conflicts)
	}
	byRunID := map[string]ActiveRunBundleAvailabilityConflict{}
	for _, conflict := range conflicts {
		byRunID[conflict.RunID] = conflict
	}
	if got := byRunID[persistedMissingRunID]; got.ErrorCode != runbundle.CodeBundleDataIntegrityError {
		t.Fatalf("persisted missing conflict = %#v, want data-integrity error", got)
	}
	if got := byRunID[deletedRunID]; got.ErrorCode != runbundle.CodeBundleUnavailable || !got.BundleSource.IsDeleted() {
		t.Fatalf("deleted conflict = %#v, want unavailable deleted source", got)
	}
}

func withStoreTestPersistedBundleSource(ctx context.Context, bundleHash string) context.Context {
	return runtimecorrelation.WithBundleSourceFact(ctx, mustStoreTestPersistedBundleSourceFact(bundleHash))
}

func withStoreTestEphemeralBundleSource(ctx context.Context, bundleHash string) context.Context {
	return runtimecorrelation.WithBundleSourceFact(ctx, mustStoreTestEphemeralBundleSourceFact(bundleHash))
}

func assertRunCanonicalBundleIdentity(t *testing.T, db *sql.DB, runID, wantHash, wantSource string) {
	t.Helper()
	var gotHash, gotSource string
	if err := db.QueryRow(`
		SELECT bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&gotHash, &gotSource); err != nil {
		t.Fatalf("load run bundle identity: %v", err)
	}
	if gotHash != wantHash || gotSource != wantSource {
		t.Fatalf("bundle identity = %q/%q, want %q/%q", gotHash, gotSource, wantHash, wantSource)
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
