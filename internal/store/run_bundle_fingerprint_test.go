package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/testutil"
)

const (
	testBootBundleFingerprint  = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	testOtherBundleFingerprint = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

func TestPostgresStore_RunBundleFingerprintPersistsAtRunCreationAndNeverOverwrites(t *testing.T) {
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
	assertRunBundleFingerprint(t, db, runID, testBootBundleFingerprint)

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
	assertRunBundleFingerprint(t, db, runID, testBootBundleFingerprint)
}

func TestPostgresStore_RunBundleFingerprintAllowsUnknownLegacyRows(t *testing.T) {
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
	assertRunBundleFingerprint(t, db, runID, "")
}

func TestPostgresStore_RunBundleFingerprintFillsUnknownWithoutOverwritingKnown(t *testing.T) {
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
	assertRunBundleFingerprint(t, db, runID, testBootBundleFingerprint)

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
	assertRunBundleFingerprint(t, db, runID, testBootBundleFingerprint)
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

func assertRunBundleFingerprint(t *testing.T, db *sql.DB, runID, want string) {
	t.Helper()
	var got sql.NullString
	if err := db.QueryRow(`SELECT bundle_fingerprint FROM runs WHERE run_id = $1::uuid`, runID).Scan(&got); err != nil {
		t.Fatalf("load run bundle fingerprint: %v", err)
	}
	if want == "" {
		if got.Valid {
			t.Fatalf("bundle_fingerprint = %q, want NULL", got.String)
		}
		return
	}
	if !got.Valid || got.String != want {
		t.Fatalf("bundle_fingerprint = %q valid=%v, want %q", got.String, got.Valid, want)
	}
}
