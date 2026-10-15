package runforkpersistence

import (
	"context"
	"database/sql"
)

func CountSelectedForkExactExecutionStorageTx(ctx context.Context, tx *sql.Tx, fork, source, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_executions
		WHERE fork_run_id=$1 AND source_event_id=$2 AND fork_event_id=$3`, fork, source, event).Scan(&count)
	return count, err
}

func ReadSelectedForkBindingModeStorageTx(ctx context.Context, tx *sql.Tx, fork, source, event string) (string, error) {
	var mode string
	err := tx.QueryRowContext(ctx, `SELECT mode FROM run_fork_selected_contract_bindings
		WHERE fork_run_id=$1 AND source_run_id=$2 AND fork_event_id=$3`, fork, source, event).Scan(&mode)
	return mode, err
}

func CountSelectedForkDeliveryReplayStorageTx(ctx context.Context, tx *sql.Tx, fork string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id=$1`, fork).Scan(&count)
	return count, err
}
