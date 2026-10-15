package runforkpersistence

import (
	"context"
	"database/sql"
)

func CountSelectedExecutionEventMappingStorageTx(ctx context.Context, tx *sql.Tx, forkRunID, sourceRunID, sourceEventID, forkEventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_executions
		WHERE fork_run_id=$1 AND source_run_id=$2 AND source_event_id=$3 AND fork_event_id=$4`,
		forkRunID, sourceRunID, sourceEventID, forkEventID).Scan(&count)
	return count, err
}

func CountPostgresSelectedForkCancelledBranchEvidenceStorageTx(ctx context.Context, tx *sql.Tx, forkRunID, sourceRunID, forkEventID, policy string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences
		WHERE fork_run_id=$1::uuid AND source_run_id=$2::uuid AND fork_event_id=$3::uuid AND policy=$4
		AND source_frozen=false AND source_run_status_at_activation='cancelled' AND source_run_status_after_activation='cancelled'
		AND source_advanced_facts @> ARRAY['source_events_advanced_after_fork_point','source_run_terminal_at_activation','source_receipts_advanced_after_fork_point']::text[]`,
		forkRunID, sourceRunID, forkEventID, policy).Scan(&count)
	return count, err
}
