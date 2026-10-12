package runforkpersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type SelectedDeploymentOperationStorage struct{ ID, State, Result, UpdatedAt string }
type SelectedDeploymentExecutionStorage struct {
	ExecutionID, State string
	Generation         int64
	Lease              delivery.WorkloadNullableText
}

func ReadSelectedDeploymentOperationStorageTx(ctx context.Context, tx *sql.Tx, runID string) (SelectedDeploymentOperationStorage, error) {
	var out SelectedDeploymentOperationStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(operation_id AS TEXT),state,CAST(result_json AS TEXT),CAST(updated_at AS TEXT) FROM run_fork_operations WHERE fork_run_id=$1`, runID).Scan(&out.ID, &out.State, &out.Result, &out.UpdatedAt)
	return out, err
}

func ReadSelectedDeploymentExecutionsStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]SelectedDeploymentExecutionStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(execution_id AS TEXT),generation,state,CAST(lease_expires_at AS TEXT) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SelectedDeploymentExecutionStorage
	for rows.Next() {
		var row SelectedDeploymentExecutionStorage
		var lease sql.NullString
		if err := rows.Scan(&row.ExecutionID, &row.Generation, &row.State, &lease); err != nil {
			return nil, err
		}
		row.Lease = delivery.WorkloadNullableText(lease)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func CountClosedSelectedDeploymentExecutionsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 AND state='closed'`, runID).Scan(&out)
	return out, err
}
