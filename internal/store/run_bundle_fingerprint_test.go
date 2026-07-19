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

const (
	testBootBundleFingerprint  = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	testOtherBundleFingerprint = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	testCanonicalBundleHash    = "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
)

func TestPostgresStore_RunBundleSourceClassifiesCurrentWritersAsLegacyAndKeepsServeAdmissionFingerprint(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := runtimecorrelation.WithBundleFingerprint(testAuthorActivityContext(), testBootBundleFingerprint)
	runID := uuid.NewString()

	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(uuid.NewString(),

		"scan.requested",
		"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("AppendEvent(first): %v", err)
	}
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, testBootBundleFingerprint)

	changedCtx := runtimecorrelation.WithBundleFingerprint(testAuthorActivityContext(), testOtherBundleFingerprint)
	if err := commitSemanticEventFixture(changedCtx, pg, eventtest.PersistedProjection(uuid.NewString(),

		"scan.followup",
		"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("AppendEvent(second): %v", err)
	}
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, testBootBundleFingerprint)
}

func TestPostgresStore_RunBundleSourceConsumesCanonicalSourceFact(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	runID := uuid.NewString()
	if _, err := db.ExecContext(testAuthorActivityContext(), `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, testCanonicalBundleHash); err != nil {
		t.Fatalf("seed canonical bundle row: %v", err)
	}
	ctx := testAuthorActivityContextForBundle(testCanonicalBundleHash)
	registerTestAuthorActivityCatalogForContext(t, pg, ctx)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, runtimecorrelation.BundleSourceFact{
		BundleHash:        testCanonicalBundleHash,
		BundleSource:      storerunlifecycle.BundleSourcePersisted,
		BundleFingerprint: testBootBundleFingerprint,
	})

	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(uuid.NewString(),

		"scan.requested",
		"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("AppendEvent(persisted): %v", err)
	}
	assertRunBundleIdentity(t, db, runID, testCanonicalBundleHash, storerunlifecycle.BundleSourcePersisted, testBootBundleFingerprint)

	ephemeralRunID := uuid.NewString()
	ephemeralHash := "bundle-v1:sha256:3333333333333333333333333333333333333333333333333333333333333333"
	ephemeralCtx := testAuthorActivityContextForBundle(ephemeralHash)
	registerTestAuthorActivityCatalogForContext(t, pg, ephemeralCtx)
	ephemeralCtx = runtimecorrelation.WithBundleSourceFact(ephemeralCtx, runtimecorrelation.BundleSourceFact{
		BundleHash:        ephemeralHash,
		BundleSource:      storerunlifecycle.BundleSourceEphemeral,
		BundleFingerprint: testOtherBundleFingerprint,
	})
	if err := commitSemanticEventFixture(ephemeralCtx, pg, eventtest.RunCreatingRootIngress(uuid.NewString(),

		"scan.dev",
		"test", "", []byte(`{}`), 0, ephemeralRunID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("AppendEvent(ephemeral): %v", err)
	}
	assertRunBundleIdentity(t, db, ephemeralRunID, ephemeralHash, storerunlifecycle.BundleSourceEphemeral, testOtherBundleFingerprint)
}

func TestPostgresStore_RunBundleSourceAllowsUnknownLegacyRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	runID := uuid.NewString()

	if err := commitSemanticEventFixture(testAuthorActivityContext(), pg, eventtest.RunCreatingRootIngress(uuid.NewString(),

		"legacy.requested",
		"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, "")
}

func TestPostgresStore_RunBundleSourceDoesNotPromoteLegacyFingerprintIntoBundleHashOnReopen(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
	`, runID); err != nil {
		t.Fatalf("seed unknown run: %v", err)
	}

	bootCtx := runtimecorrelation.WithBundleFingerprint(ctx, testBootBundleFingerprint)
	if err := commitSemanticEventFixture(bootCtx, pg, eventtest.RunCreatingRootIngress(uuid.NewString(),

		"legacy.filled",
		"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("AppendEvent(fill): %v", err)
	}
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, testBootBundleFingerprint)

	changedCtx := runtimecorrelation.WithBundleFingerprint(ctx, testOtherBundleFingerprint)
	if err := commitSemanticEventFixture(changedCtx, pg, eventtest.PersistedProjection(uuid.NewString(),

		"legacy.followup",
		"test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("AppendEvent(followup): %v", err)
	}
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, testBootBundleFingerprint)
}

func TestRunLifecycleOwnerPersistsCanonicalBundleHashWhenSupplied(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContextForBundle(testCanonicalBundleHash)
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, testCanonicalBundleHash); err != nil {
		t.Fatalf("seed canonical bundle row: %v", err)
	}

	if err := pg.runAuthorActivityMutation(ctx, "test ensure active run", func(txctx context.Context, tx *sql.Tx) error {
		return storerunlifecycle.EnsureActive(txctx, tx, runID, "", "", storerunlifecycle.EnsureActiveOptions{
			HasStartedAtCol:    true,
			HasBundleHashCol:   true,
			HasBundleSourceCol: true,
			BundleHash:         testCanonicalBundleHash,
			BundleSource:       storerunlifecycle.BundleSourcePersisted,
		})
	}); err != nil {
		t.Fatalf("EnsureActive: %v", err)
	}
	assertRunBundleIdentity(t, db, runID, testCanonicalBundleHash, storerunlifecycle.BundleSourcePersisted, "")
}

func TestRunLifecycleOwnerRejectsNonLegacySourceWithoutBundleHash(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	err := pg.runAuthorActivityMutation(testAuthorActivityContext(), "test ensure invalid active run", func(txctx context.Context, tx *sql.Tx) error {
		return storerunlifecycle.EnsureActive(txctx, tx, uuid.NewString(), "", "", storerunlifecycle.EnsureActiveOptions{
			HasBundleHashCol:   true,
			HasBundleSourceCol: true,
			BundleSource:       storerunlifecycle.BundleSourcePersisted,
		})
	})
	if err == nil {
		t.Fatal("EnsureActive error = nil, want missing bundle_hash rejection")
	}
}

func TestRunLifecycleOwnerRejectsPersistedSourceWithoutBundleRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()

	err := pg.runAuthorActivityMutation(ctx, "test ensure missing persisted run bundle", func(txctx context.Context, tx *sql.Tx) error {
		return storerunlifecycle.EnsureActive(txctx, tx, runID, "", "", storerunlifecycle.EnsureActiveOptions{
			HasBundleHashCol:   true,
			HasBundleSourceCol: true,
			BundleHash:         testCanonicalBundleHash,
			BundleSource:       storerunlifecycle.BundleSourcePersisted,
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
	ctx := testAuthorActivityContext()
	legacyRunID := uuid.NewString()
	persistedRunID := uuid.NewString()
	persistedMissingRunID := uuid.NewString()
	completedLegacyRunID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, testCanonicalBundleHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}

	for _, seed := range []struct {
		runID       string
		status      string
		hash        sql.NullString
		source      string
		fingerprint sql.NullString
	}{
		{runID: persistedRunID, status: "running", hash: sql.NullString{String: testCanonicalBundleHash, Valid: true}, source: storerunlifecycle.BundleSourcePersisted},
		{runID: persistedMissingRunID, status: "running", hash: sql.NullString{String: "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222", Valid: true}, source: storerunlifecycle.BundleSourcePersisted},
		{runID: legacyRunID, status: "paused", source: storerunlifecycle.BundleSourceLegacy, fingerprint: sql.NullString{String: testBootBundleFingerprint, Valid: true}},
		{runID: completedLegacyRunID, status: "completed", source: storerunlifecycle.BundleSourceLegacy, fingerprint: sql.NullString{String: testOtherBundleFingerprint, Valid: true}},
	} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint)
			VALUES ($1::uuid, $2, $3, $4, $5)
		`, seed.runID, seed.status, seed.hash, seed.source, seed.fingerprint); err != nil {
			t.Fatalf("seed run %s: %v", seed.runID, err)
		}
	}

	conflicts, err := pg.ActiveRunBundleAvailabilityConflicts(ctx)
	if err != nil {
		t.Fatalf("ActiveRunBundleAvailabilityConflicts: %v", err)
	}
	if len(conflicts) != 2 {
		t.Fatalf("conflicts = %#v, want persisted-missing and legacy active conflicts", conflicts)
	}
	byRunID := map[string]ActiveRunBundleAvailabilityConflict{}
	for _, conflict := range conflicts {
		byRunID[conflict.RunID] = conflict
	}
	if got := byRunID[persistedMissingRunID]; got.ErrorCode != runbundle.CodeBundleDataIntegrityError {
		t.Fatalf("persisted missing conflict = %#v, want data-integrity error", got)
	}
	if got := byRunID[legacyRunID]; got.ErrorCode != runbundle.CodeBundleUnavailable || got.Cause != storerunlifecycle.BundleSourceLegacy {
		t.Fatalf("legacy conflict = %#v, want bundle unavailable legacy", got)
	}
}

func assertRunBundleIdentity(t *testing.T, db *sql.DB, runID, wantHash, wantSource, wantLegacyFingerprint string) {
	t.Helper()
	var gotHash, gotFingerprint sql.NullString
	var gotSource string
	if err := db.QueryRow(`
		SELECT bundle_hash, bundle_source, bundle_fingerprint
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&gotHash, &gotSource, &gotFingerprint); err != nil {
		t.Fatalf("load run bundle identity: %v", err)
	}
	if wantHash == "" {
		if gotHash.Valid {
			t.Fatalf("bundle_hash = %q, want NULL", gotHash.String)
		}
	} else if !gotHash.Valid || gotHash.String != wantHash {
		t.Fatalf("bundle_hash = %q valid=%v, want %q", gotHash.String, gotHash.Valid, wantHash)
	}
	if gotSource != wantSource {
		t.Fatalf("bundle_source = %q, want %q", gotSource, wantSource)
	}
	if wantLegacyFingerprint == "" {
		if gotFingerprint.Valid {
			t.Fatalf("bundle_fingerprint = %q, want NULL", gotFingerprint.String)
		}
	} else if !gotFingerprint.Valid || gotFingerprint.String != wantLegacyFingerprint {
		t.Fatalf("bundle_fingerprint = %q valid=%v, want %q", gotFingerprint.String, gotFingerprint.Valid, wantLegacyFingerprint)
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
