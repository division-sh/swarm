package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecorrelation "swarm/internal/runtime/correlation"
	storerunlifecycle "swarm/internal/store/runlifecycle"
	"swarm/internal/testutil"
)

const (
	testBootBundleFingerprint  = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	testOtherBundleFingerprint = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	testCanonicalBundleHash    = "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
)

func TestPostgresStore_RunBundleSourceClassifiesCurrentWritersAsLegacyWithoutCopyingFingerprint(t *testing.T) {
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
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, "")

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
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, "")
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

func TestPostgresStore_RunBundleSourceDoesNotPromoteLegacyFingerprintOnReopen(t *testing.T) {
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
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, "")

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
	assertRunBundleIdentity(t, db, runID, "", storerunlifecycle.BundleSourceLegacy, "")
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

func TestPostgresStore_ActiveRunBundleMismatches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()
	matchingRunID := uuid.NewString()
	mismatchedRunID := uuid.NewString()
	legacyRunID := uuid.NewString()
	completedMismatchRunID := uuid.NewString()

	for _, seed := range []struct {
		runID       string
		status      string
		fingerprint sql.NullString
	}{
		{runID: matchingRunID, status: "running", fingerprint: sql.NullString{String: testBootBundleFingerprint, Valid: true}},
		{runID: mismatchedRunID, status: "paused", fingerprint: sql.NullString{String: testOtherBundleFingerprint, Valid: true}},
		{runID: legacyRunID, status: "running", fingerprint: sql.NullString{}},
		{runID: completedMismatchRunID, status: "completed", fingerprint: sql.NullString{String: testOtherBundleFingerprint, Valid: true}},
	} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, bundle_fingerprint)
			VALUES ($1::uuid, $2, $3)
		`, seed.runID, seed.status, seed.fingerprint); err != nil {
			t.Fatalf("seed run %s: %v", seed.runID, err)
		}
	}

	mismatches, err := pg.ActiveRunBundleMismatches(ctx, testBootBundleFingerprint)
	if err != nil {
		t.Fatalf("ActiveRunBundleMismatches: %v", err)
	}
	if len(mismatches) != 1 {
		t.Fatalf("mismatches = %#v, want one", mismatches)
	}
	if got := mismatches[0]; got.RunID != mismatchedRunID || got.Status != "paused" || got.BundleFingerprint != testOtherBundleFingerprint {
		t.Fatalf("mismatch = %#v, want paused mismatched run", got)
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
