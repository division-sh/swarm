package store

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestEntityStateSchema_AllowsSameEntityIDInDifferentRuns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runA := uuid.NewString()
	runB := uuid.NewString()
	entityID := uuid.NewString()
	for _, runID := range []string{runA, runB} {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
			t.Fatalf("seed run %s: %v", runID, err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		)
		VALUES
			($1::uuid, $3::uuid, 'flow/source', 'default', 'source_state', '{}'::jsonb, '{"side":"source"}'::jsonb, '{}'::jsonb, 1, now(), now(), now()),
			($2::uuid, $3::uuid, 'flow/fork', 'default', 'fork_state', '{}'::jsonb, '{"side":"fork"}'::jsonb, '{}'::jsonb, 1, now(), now(), now())
	`, runA, runB, entityID); err != nil {
		t.Fatalf("insert run-scoped entity rows: %v", err)
	}
	var sourceState, forkState string
	if err := db.QueryRowContext(ctx, `SELECT current_state FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, runA, entityID).Scan(&sourceState); err != nil {
		t.Fatalf("load source row: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT current_state FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, runB, entityID).Scan(&forkState); err != nil {
		t.Fatalf("load fork row: %v", err)
	}
	if sourceState != "source_state" || forkState != "fork_state" {
		t.Fatalf("states = source:%q fork:%q", sourceState, forkState)
	}
}
