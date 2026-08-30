package runtimepersistence

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	pipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteScenarioSetupEntitiesIdempotentExistingRows(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	requireDefaultSourceArtifactForTest(t, ctx, sqliteStore)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	req := pipeline.ScenarioSetupRequest{
		RunID:     runID,
		CreatedAt: time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC),
		Entities: []pipeline.ScenarioSetupEntityRequest{{
			Alias:        "subject",
			EntityID:     entityID,
			FlowInstance: "operating",
			EntityType:   "product",
			CurrentState: "waiting",
			Fields: map[string]any{
				"note": "seeded",
			},
			Gates: map[string]bool{
				"review_ready": true,
			},
		}},
	}

	if _, err := sqliteStore.SetupScenarioEntities(ctx, req); err != nil {
		t.Fatalf("SetupScenarioEntities first insert: %v", err)
	}
	if _, err := sqliteStore.SetupScenarioEntities(ctx, req); err != nil {
		t.Fatalf("SetupScenarioEntities matching replay: %v", err)
	}
	assertSQLiteScenarioSetupCounts(t, ctx, sqliteStore, runID, entityID, 1, 3)

	changed := req
	changed.Entities = append([]pipeline.ScenarioSetupEntityRequest(nil), req.Entities...)
	changed.Entities[0].Fields = map[string]any{"note": "changed"}
	if _, err := sqliteStore.SetupScenarioEntities(ctx, changed); err == nil || !strings.Contains(err.Error(), "already exists with different fields") {
		t.Fatalf("SetupScenarioEntities changed replay error = %v, want different fields", err)
	}
	assertSQLiteScenarioSetupCounts(t, ctx, sqliteStore, runID, entityID, 1, 3)
}

func TestSQLiteScenarioSetupPersistsExactExecutionProfileAtomically(t *testing.T) {
	ctx := testAuthorActivityContext()
	sourceFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("test context bundle source fact is missing")
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(sourceFact, "sha256:"+strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := scenarioexecution.NewProfile(identity, "derived/minimal", nil)
	if err != nil {
		t.Fatal(err)
	}
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	requireDefaultSourceArtifactForTest(t, ctx, store)
	runID := uuid.NewString()
	req := pipeline.ScenarioSetupRequest{
		RunID: runID, CreatedAt: time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
		ScenarioExecutionProfile: &profile,
		Entities: []pipeline.ScenarioSetupEntityRequest{{
			Alias: "subject", EntityID: uuid.NewString(), FlowInstance: "flow", EntityType: "default", CurrentState: "ready",
		}},
	}
	if _, err := store.SetupScenarioEntities(ctx, req); err != nil {
		t.Fatal(err)
	}
	var profileID, profileDigest, effectiveDigest string
	var raw []byte
	if err := store.backend.QueryRowContext(ctx, `
		SELECT profile_id, profile_digest, effective_source_digest, profile_bytes
		FROM run_scenario_execution_profiles WHERE run_id = ?
	`, runID).Scan(&profileID, &profileDigest, &effectiveDigest, &raw); err != nil {
		t.Fatal(err)
	}
	if profileID != profile.ID() || profileDigest != profile.Digest() || effectiveDigest != identity.Digest() || !bytes.Equal(raw, profile.CanonicalBytes()) {
		t.Fatalf("stored profile mismatch: id=%q digest=%q effective=%q bytes=%s", profileID, profileDigest, effectiveDigest, raw)
	}
	changedIdentity, _ := scenarioexecution.NewEffectiveSourceIdentity(sourceFact, "sha256:"+strings.Repeat("b", 64))
	changed, _ := scenarioexecution.NewProfile(changedIdentity, profile.ID(), nil)
	req.ScenarioExecutionProfile = &changed
	if _, err := store.SetupScenarioEntities(ctx, req); err == nil || !strings.Contains(err.Error(), "different scenario execution profile") {
		t.Fatalf("changed profile replay error = %v", err)
	}
	if err := store.backend.QueryRowContext(ctx, `SELECT profile_bytes FROM run_scenario_execution_profiles WHERE run_id = ?`, runID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, profile.CanonicalBytes()) {
		t.Fatal("conflicting replay mutated stored profile bytes")
	}
}

func TestPostgresScenarioSetupPersistsExactExecutionProfileAtomically(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	requireDefaultSourceArtifactForTest(t, ctx, store)
	sourceFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("test context bundle source fact is missing")
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(sourceFact, "sha256:"+strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := scenarioexecution.NewProfile(identity, "derived/minimal", nil)
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	req := pipeline.ScenarioSetupRequest{
		RunID: runID, CreatedAt: time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
		ScenarioExecutionProfile: &profile,
		Entities: []pipeline.ScenarioSetupEntityRequest{{
			Alias: "subject", EntityID: uuid.NewString(), FlowInstance: "flow", EntityType: "default", CurrentState: "ready",
		}},
	}
	if _, err := store.SetupScenarioEntities(ctx, req); err != nil {
		t.Fatal(err)
	}
	var profileID, profileDigest, effectiveDigest string
	var raw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT profile_id, profile_digest, effective_source_digest, profile_bytes
		FROM run_scenario_execution_profiles WHERE run_id = $1::uuid
	`, runID).Scan(&profileID, &profileDigest, &effectiveDigest, &raw); err != nil {
		t.Fatal(err)
	}
	if profileID != profile.ID() || profileDigest != profile.Digest() || effectiveDigest != identity.Digest() || !bytes.Equal(raw, profile.CanonicalBytes()) {
		t.Fatalf("stored profile mismatch: id=%q digest=%q effective=%q bytes=%s", profileID, profileDigest, effectiveDigest, raw)
	}
	changedIdentity, _ := scenarioexecution.NewEffectiveSourceIdentity(sourceFact, "sha256:"+strings.Repeat("b", 64))
	changed, _ := scenarioexecution.NewProfile(changedIdentity, profile.ID(), nil)
	req.ScenarioExecutionProfile = &changed
	if _, err := store.SetupScenarioEntities(ctx, req); err == nil || !strings.Contains(err.Error(), "different scenario execution profile") {
		t.Fatalf("changed profile replay error = %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT profile_bytes FROM run_scenario_execution_profiles WHERE run_id = $1::uuid`, runID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, profile.CanonicalBytes()) {
		t.Fatal("conflicting replay mutated stored profile bytes")
	}
}

func assertSQLiteScenarioSetupCounts(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore, runID, entityID string, wantEntities, wantMutations int) {
	t.Helper()
	var entities int
	if err := sqliteStore.backend.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM entity_state
		WHERE run_id = ? AND entity_id = ?
	`, runID, entityID).Scan(&entities); err != nil {
		t.Fatalf("count sqlite setup entities: %v", err)
	}
	if entities != wantEntities {
		t.Fatalf("sqlite setup entity rows = %d, want %d", entities, wantEntities)
	}

	var mutations int
	if err := sqliteStore.backend.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM entity_mutations
		WHERE run_id = ? AND entity_id = ?
		  AND writer_type = 'platform'
		  AND writer_id = 'test.setup_entities'
	`, runID, entityID).Scan(&mutations); err != nil {
		t.Fatalf("count sqlite setup mutations: %v", err)
	}
	if mutations != wantMutations {
		t.Fatalf("sqlite setup mutation rows = %d, want %d", mutations, wantMutations)
	}
}
