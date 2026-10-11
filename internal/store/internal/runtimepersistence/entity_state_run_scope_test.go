package runtimepersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestEntityStateSchema_AllowsSameEntityIDInDifferentRuns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	runA := uuid.NewString()
	runB := uuid.NewString()
	entityID := uuid.NewString()
	for _, runID := range []string{runA, runB} {
		requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, fields, created_at, updated_at
		)
		VALUES
			($1::uuid, $3::uuid, 'flow/source', 'default', '{"side":"source"}'::jsonb, now(), now()),
			($2::uuid, $3::uuid, 'flow/fork', 'default', '{"side":"fork"}'::jsonb, now(), now())
	`, runA, runB, entityID); err != nil {
		t.Fatalf("insert run-scoped entity rows: %v", err)
	}
	var sourcePath, sourceType, sourceSide, forkPath, forkType, forkSide string
	if err := db.QueryRowContext(ctx, `SELECT flow_instance, entity_type, fields->>'side' FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, runA, entityID).Scan(&sourcePath, &sourceType, &sourceSide); err != nil {
		t.Fatalf("load source row: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT flow_instance, entity_type, fields->>'side' FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, runB, entityID).Scan(&forkPath, &forkType, &forkSide); err != nil {
		t.Fatalf("load fork row: %v", err)
	}
	if sourcePath != "flow/source" || sourceType != "default" || sourceSide != "source" {
		t.Fatalf("source row = path:%q type:%q side:%q", sourcePath, sourceType, sourceSide)
	}
	if forkPath != "flow/fork" || forkType != "default" || forkSide != "fork" {
		t.Fatalf("fork row = path:%q type:%q side:%q", forkPath, forkType, forkSide)
	}
}
