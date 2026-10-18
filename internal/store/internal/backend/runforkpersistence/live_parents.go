package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func insertSelectedExecutionLiveParentsTx(ctx context.Context, tx *sql.Tx, executionID string) error {
	result, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_execution_live_parents
		(execution_id,binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,generation,fork_event_id)
		SELECT execution_id,binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,generation,fork_event_id
		FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, executionID)
	return requireExactlyOneMutation(result, err, "insert exact selected execution live parents")
}

func insertForkOperationLiveParentsTx(ctx context.Context, tx *sql.Tx, operationID string) error {
	result, err := tx.ExecContext(ctx, `INSERT INTO run_fork_operation_live_parents
		(operation_id,selected_binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,fork_event_id)
		SELECT operation_id,selected_binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,fork_event_id
		FROM run_fork_operations WHERE operation_id=$1`, operationID)
	return requireExactlyOneMutation(result, err, "insert exact fork operation live parents")
}

// A canonical fact alone is not live authority. Missing or crossed associations
// must refuse rather than fall through to ordinary execution or ACK recovery.
func requireForkLiveParentsTx(ctx context.Context, tx *sql.Tx, id string, execution bool) error {
	query := `SELECT COUNT(*) FROM run_fork_operations c
		JOIN run_fork_operation_live_parents p ON p.operation_id=c.operation_id
		AND p.selected_binding_id=c.selected_binding_id AND p.fork_run_id=c.fork_run_id
		AND p.source_run_id=c.source_run_id AND p.fork_point_kind=c.fork_point_kind
		AND p.fork_revision=c.fork_revision
		AND COALESCE(CAST(p.fork_event_id AS TEXT),'')=COALESCE(CAST(c.fork_event_id AS TEXT),'')
		WHERE c.operation_id=$1`
	if execution {
		query = `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions c
			JOIN run_fork_selected_contract_execution_live_parents p ON p.execution_id=c.execution_id
			AND p.binding_id=c.binding_id AND p.fork_run_id=c.fork_run_id AND p.source_run_id=c.source_run_id
			AND p.fork_point_kind=c.fork_point_kind AND p.fork_revision=c.fork_revision AND p.generation=c.generation
			AND COALESCE(CAST(p.fork_event_id AS TEXT),'')=COALESCE(CAST(c.fork_event_id AS TEXT),'')
			WHERE c.execution_id=$1`
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, id).Scan(&count); err != nil {
		return fmt.Errorf("read canonical fork live parents: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("canonical fork fact lacks its exact live-parent association")
	}
	return nil
}
