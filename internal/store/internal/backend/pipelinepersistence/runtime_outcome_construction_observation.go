package pipelinepersistence

import (
	"context"
	"database/sql"
)

func CountRunFlowInstanceStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}

func CountRunFlowRuntimeReadinessStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instance_runtime_readiness WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}
