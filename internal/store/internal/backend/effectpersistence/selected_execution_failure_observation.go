package effectpersistence

import (
	"context"
	"database/sql"
)

type SelectedExecutionFailureStorage struct {
	AttemptState, OperationState, Failure string
	AttemptSurfaceID, TurnSurfaceID       string
	ExecutionState                        string
}

func ReadSelectedExecutionFailureStorageTx(ctx context.Context, tx *sql.Tx, executionID string) (SelectedExecutionFailureStorage, error) {
	var out SelectedExecutionFailureStorage
	err := tx.QueryRowContext(ctx, `
		SELECT a.state, o.state, CAST(a.failure AS TEXT), CAST(a.capability_surface_id AS TEXT),
		       CAST(t.capability_surface_id AS TEXT), e.state
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		JOIN agent_turns t ON t.completion_attempt_id = a.attempt_id
		JOIN run_fork_selected_contract_runtime_executions e ON e.execution_id = o.selected_execution_id
		WHERE o.selected_execution_id = $1
		  AND o.authority_kind = 'selected_contract_fork'
		  AND a.adapter = 'openai_compatible'
	`, executionID).Scan(&out.AttemptState, &out.OperationState, &out.Failure, &out.AttemptSurfaceID, &out.TurnSurfaceID, &out.ExecutionState)
	return out, err
}

func CountSelectedExecutionActiveAttemptsStorageTx(ctx context.Context, tx *sql.Tx, executionID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id = $1
		  AND a.state IN ('authorized','launched','response_observed')
	`, executionID).Scan(&count)
	return count, err
}
