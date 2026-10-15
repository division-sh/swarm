package runforkpersistence

import (
	"context"
	"database/sql"
)

func CountSelectedContractBindingStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_bindings`).Scan(&count)
	return count, err
}

func CountGlobalSelectedRuntimeExecutionStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions`).Scan(&count)
	return count, err
}

func ReadSelectedRuntimeExecutionStateStorageTx(ctx context.Context, tx *sql.Tx, forkRunID string) (string, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, forkRunID).Scan(&state)
	return state, err
}

func CountSelectedRuntimeExecutionStorageTx(ctx context.Context, tx *sql.Tx, forkRunID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, forkRunID).Scan(&count)
	return count, err
}

func CountSelectedSourceRuntimeExecutionsStorageTx(ctx context.Context, tx *sql.Tx, sourceRunID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE source_run_id=$1`, sourceRunID).Scan(&count)
	return count, err
}

func CountClosedFirstSelectedRuntimeExecutionStorageTx(ctx context.Context, tx *sql.Tx, forkRunID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 AND generation=1 AND state='closed'`, forkRunID).Scan(&count)
	return count, err
}

func CountSelectedForkSourceEventLineageStorageTx(ctx context.Context, tx *sql.Tx, forkRunID, sourceEventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id=$1 AND source_event_id=$2`, forkRunID, sourceEventID).Scan(&count)
	return count, err
}
