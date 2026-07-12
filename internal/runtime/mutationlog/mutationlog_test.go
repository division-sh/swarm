package mutationlog

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestReconstructEntityStateProjection_FailsOnMalformedGateKey(t *testing.T) {
	_, err := ReconstructEntityStateProjection([]ProjectionMutation{{
		Field:    "gates.",
		NewValue: true,
	}})
	if err == nil || !strings.Contains(err.Error(), "gates mutation key is required") {
		t.Fatalf("ReconstructEntityStateProjection error = %v", err)
	}
}

func TestReconstructEntityStateProjection_FailsOnMalformedAccumulatorKey(t *testing.T) {
	_, err := ReconstructEntityStateProjection([]ProjectionMutation{{
		Field:    "accumulator.",
		NewValue: map[string]any{"bad": true},
	}})
	if err == nil || !strings.Contains(err.Error(), "accumulator mutation key is required") {
		t.Fatalf("ReconstructEntityStateProjection error = %v", err)
	}
}

func TestReconstructEntityStateProjection_RoundTripsTrackedEntityState(t *testing.T) {
	got, err := ReconstructEntityStateProjection([]ProjectionMutation{
		{Field: "current_state", NewValue: "done"},
		{Field: "status", NewValue: "closed"},
		{Field: "gates.g_done", NewValue: true},
		{Field: "accumulator.evidence", NewValue: map[string]any{"score": float64(2)}},
	})
	if err != nil {
		t.Fatalf("ReconstructEntityStateProjection: %v", err)
	}
	if got.CurrentState != "done" {
		t.Fatalf("CurrentState = %q", got.CurrentState)
	}
	if got.Fields["status"] != "closed" {
		t.Fatalf("Fields = %#v", got.Fields)
	}
	if got.Gates["g_done"] != true {
		t.Fatalf("Gates = %#v", got.Gates)
	}
	acc, _ := got.Accumulator["evidence"].(map[string]any)
	if acc["score"] != float64(2) {
		t.Fatalf("Accumulator = %#v", got.Accumulator)
	}
}

func TestReconstructEntityStateProjection_AppliesNestedFieldMutationsOverTopLevelObjects(t *testing.T) {
	got, err := ReconstructEntityStateProjection([]ProjectionMutation{
		{Field: "metadata", NewValue: map[string]any{"region": "us", "score_band": "low"}},
		{Field: "metadata.region", NewValue: "ca"},
		{Field: "status", NewValue: "open"},
	})
	if err != nil {
		t.Fatalf("ReconstructEntityStateProjection: %v", err)
	}
	metadata, ok := got.Fields["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("Fields = %#v", got.Fields)
	}
	if metadata["region"] != "ca" {
		t.Fatalf("metadata.region = %#v, want ca", metadata["region"])
	}
	if metadata["score_band"] != "low" {
		t.Fatalf("metadata.score_band = %#v, want low", metadata["score_band"])
	}
	if got.Fields["status"] != "open" {
		t.Fatalf("Fields = %#v", got.Fields)
	}
	if _, ok := got.Fields["metadata.region"]; ok {
		t.Fatalf("Fields contains literal dotted key: %#v", got.Fields)
	}
}

func TestInsertStampsBundleSourceFactOnEnsuredRunRow(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	runID := uuid.NewString()
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
		BundleSource:      storerunlifecycle.BundleSourcePersisted,
		BundleFingerprint: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
	}
	seedMutationLogBundleRow(t, db, sourceFact.BundleHash)
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)

	if err := Insert(ctx, db, Record{
		EntityID:   uuid.NewString(),
		Field:      "status",
		OldValue:   nil,
		NewValue:   "open",
		WriterType: "system_node",
		WriterID:   "review",
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	var gotHash, gotFingerprint sql.NullString
	var gotSource string
	if err := db.QueryRow(`
		SELECT bundle_hash, bundle_source, bundle_fingerprint
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&gotHash, &gotSource, &gotFingerprint); err != nil {
		t.Fatalf("load run bundle source: %v", err)
	}
	if !gotHash.Valid || gotHash.String != sourceFact.BundleHash || gotSource != sourceFact.BundleSource || !gotFingerprint.Valid || gotFingerprint.String != sourceFact.BundleFingerprint {
		t.Fatalf("run bundle source = hash:%q valid:%v source:%q fingerprint:%q valid:%v, want %#v", gotHash.String, gotHash.Valid, gotSource, gotFingerprint.String, gotFingerprint.Valid, sourceFact)
	}
}

func TestInsertRejectsDeletedPersistedBundleSourceFact(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	runID := uuid.NewString()
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
		BundleSource:      storerunlifecycle.BundleSourcePersisted,
		BundleFingerprint: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
	}
	seedMutationLogBundleRow(t, db, sourceFact.BundleHash)
	if _, err := db.ExecContext(context.Background(), `DELETE FROM bundles WHERE bundle_hash = $1`, sourceFact.BundleHash); err != nil {
		t.Fatalf("delete bundle row: %v", err)
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)

	err := Insert(ctx, db, Record{
		EntityID:   uuid.NewString(),
		Field:      "status",
		OldValue:   nil,
		NewValue:   "open",
		WriterType: "system_node",
		WriterID:   "review",
	})
	if !errors.Is(err, storerunlifecycle.ErrPersistedBundleUnavailable) {
		t.Fatalf("Insert error = %v, want ErrPersistedBundleUnavailable", err)
	}
	if count := countMutationLogRunRows(t, db, runID); count != 0 {
		t.Fatalf("run rows for %s = %d, want 0", runID, count)
	}
	if count := countMutationRowsForRun(t, db, runID); count != 0 {
		t.Fatalf("entity_mutations rows for %s = %d, want 0", runID, count)
	}
}

func seedMutationLogBundleRow(t *testing.T, db *sql.DB, bundleHash string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, bundleHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
}

func countMutationLogRunRows(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`, runID).Scan(&count); err != nil {
		t.Fatalf("count run rows for %s: %v", runID, err)
	}
	return count
}

func countMutationRowsForRun(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid`, runID).Scan(&count); err != nil {
		t.Fatalf("count entity_mutations rows for %s: %v", runID, err)
	}
	return count
}
