package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// Native projection controls seed an explicit header, not construction receipts.
// These fixtures are not public constructor or activation qualification.
func seedWorkflowHeaderProjectionFixture(t *testing.T, ctx context.Context, db *sql.DB, runID, entityID, path, flow, entityType, stage, accumulator string, at time.Time) {
	t.Helper()
	_, err := db.ExecContext(ctx, `INSERT INTO flow_instances
		(run_id, instance_path, entity_id, entity_type, flow_template, mode, stage_defined,
		 current_state, gates, bookkeeping, accumulator, config, revision, entered_state_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,'static',TRUE,$6,'{}','{}',$7,'{"config":{}}',1,$8,$8,$8)`,
		runID, path, entityID, entityType, flow, stage, accumulator, at)
	if err != nil {
		t.Fatalf("seed native constructed-header projection: %v", err)
	}
}
