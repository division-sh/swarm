package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSQLiteScenarioSetupEntitiesIdempotentExistingRows(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	req := ScenarioSetupRequest{
		RunID:     runID,
		CreatedAt: time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC),
		Entities: []ScenarioSetupEntityRequest{{
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
	changed.Entities = append([]ScenarioSetupEntityRequest(nil), req.Entities...)
	changed.Entities[0].Fields = map[string]any{"note": "changed"}
	if _, err := sqliteStore.SetupScenarioEntities(ctx, changed); err == nil || !strings.Contains(err.Error(), "already exists with different fields") {
		t.Fatalf("SetupScenarioEntities changed replay error = %v, want different fields", err)
	}
	assertSQLiteScenarioSetupCounts(t, ctx, sqliteStore, runID, entityID, 1, 3)
}

func assertSQLiteScenarioSetupCounts(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore, runID, entityID string, wantEntities, wantMutations int) {
	t.Helper()
	var entities int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
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
	if err := sqliteStore.DB.QueryRowContext(ctx, `
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
