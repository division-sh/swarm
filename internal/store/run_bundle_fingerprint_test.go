package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/store/runbundle"
	storerunlifecycle "swarm/internal/store/runlifecycle"
	"swarm/internal/testutil"
)

const (
	testBootBundleFingerprint  = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	testOtherBundleFingerprint = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	testCanonicalBundleHash    = "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
)

func TestPostgresStore_RunBundleSourceClassifiesCurrentWritersAsLegacyAndKeepsServeAdmissionFingerprint(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := runtimecorrelation.WithBundleFingerprint(context.Background(), testBootBundleFingerprint)
	runID := uuid.NewString()

	if err := pg.AppendEvent(ctx, events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        "scan.requested",
		SourceAgent: "test",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(first): %v", err)
	}
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, testBootBundleFingerprint)

	changedCtx := runtimecorrelation.WithBundleFingerprint(context.Background(), testOtherBundleFingerprint)
	if err := pg.AppendEvent(changedCtx, events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        "scan.followup",
		SourceAgent: "test",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(second): %v", err)
	}
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, testBootBundleFingerprint)
}

func TestPostgresStore_RunBundleSourceAllowsUnknownLegacyRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	runID := uuid.NewString()

	if err := pg.AppendEvent(context.Background(), events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        "legacy.requested",
		SourceAgent: "test",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, "")
}

func TestPostgresStore_RunBundleSourceDoesNotPromoteLegacyFingerprintIntoBundleHashOnReopen(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	runID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
	`, runID); err != nil {
		t.Fatalf("seed unknown run: %v", err)
	}

	bootCtx := runtimecorrelation.WithBundleFingerprint(ctx, testBootBundleFingerprint)
	if err := pg.AppendEvent(bootCtx, events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        "legacy.filled",
		SourceAgent: "test",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(fill): %v", err)
	}
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, testBootBundleFingerprint)

	changedCtx := runtimecorrelation.WithBundleFingerprint(ctx, testOtherBundleFingerprint)
	if err := pg.AppendEvent(changedCtx, events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        "legacy.followup",
		SourceAgent: "test",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(followup): %v", err)
	}
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, testBootBundleFingerprint)
}

func TestRunLifecycleOwnerPersistsCanonicalBundleHashWhenSupplied(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runID := uuid.NewString()

	if err := storerunlifecycle.EnsureActive(ctx, db, runID, "", "", storerunlifecycle.EnsureActiveOptions{
		HasStartedAtCol:    true,
		HasBundleHashCol:   true,
		HasBundleSourceCol: true,
		BundleHash:         testCanonicalBundleHash,
		BundleSource:       storerunlifecycle.BundleSourcePersisted,
	}); err != nil {
		t.Fatalf("EnsureActive: %v", err)
	}
	assertRunBundleIdentity(t, db, runID, testCanonicalBundleHash, storerunlifecycle.BundleSourcePersisted, "")
}

func TestPostgresStore_ActiveRunBundleAvailabilityConflicts(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
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
