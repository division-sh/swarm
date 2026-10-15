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

type SelectedForkKeyStorage struct{ RunID, State string }

func ReadSelectedForkKeyStorageTx(ctx context.Context, tx *sql.Tx, key string) (SelectedForkKeyStorage, error) {
	var out SelectedForkKeyStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(fork_run_id AS TEXT),state FROM run_fork_operations WHERE idempotency_key=$1`, key).Scan(&out.RunID, &out.State)
	return out, err
}

func ReadSelectedForkKeyRunStateStorageTx(ctx context.Context, tx *sql.Tx, key, run string) (string, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM run_fork_operations WHERE idempotency_key=$1 AND fork_run_id=$2`, key, run).Scan(&state)
	return state, err
}

func CountSelectedForkKeyOperationsStorageTx(ctx context.Context, tx *sql.Tx, key string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_operations WHERE idempotency_key=$1`, key).Scan(&out)
	return out, err
}

func CountSelectedForkKeyActivatedOperationsStorageTx(ctx context.Context, tx *sql.Tx, key string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_operations WHERE idempotency_key=$1 AND state='activated'`, key).Scan(&out)
	return out, err
}

func CountSelectedForkSourceOperationsStorageTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_operations WHERE source_run_id=$1`, run).Scan(&out)
	return out, err
}

func ReadLatestSelectedDeploymentExecutionStateStorageTx(ctx context.Context, tx *sql.Tx, run string) (string, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation DESC LIMIT 1`, run).Scan(&state)
	return state, err
}

type SelectedDeploymentExecutionIdentityStorage struct {
	ID         string
	Generation int64
}

func ReadLatestSelectedDeploymentExecutionIdentityStorageTx(ctx context.Context, tx *sql.Tx, run string) (SelectedDeploymentExecutionIdentityStorage, error) {
	var out SelectedDeploymentExecutionIdentityStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(execution_id AS TEXT),generation FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation DESC LIMIT 1`, run).Scan(&out.ID, &out.Generation)
	return out, err
}

type SelectedDeploymentPointStorage struct {
	RunKind, OperationKind, ChildID, BindingKind    string
	RunRevision, OperationRevision, BindingRevision int64
}

func ReadSelectedDeploymentPointStorageTx(ctx context.Context, tx *sql.Tx, run string) (SelectedDeploymentPointStorage, error) {
	var out SelectedDeploymentPointStorage
	err := tx.QueryRowContext(ctx, `SELECT r.forked_from_point_kind, r.forked_from_revision,
		o.fork_point_kind, o.fork_revision, o.fork_run_id, b.fork_point_kind, b.fork_revision
		FROM runs r JOIN run_fork_operations o ON o.fork_run_id=r.run_id
		JOIN run_fork_selected_contract_bindings b ON b.binding_id=o.selected_binding_id
		WHERE r.run_id=$1`, run).Scan(&out.RunKind, &out.RunRevision, &out.OperationKind, &out.OperationRevision, &out.ChildID, &out.BindingKind, &out.BindingRevision)
	return out, err
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
