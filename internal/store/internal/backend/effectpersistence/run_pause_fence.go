package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
)

// RequireRunPauseSettledTx is called with the canonical run row locked. Work
// already settled or durably uncertain stays recorded, never redispatched.
func RequireRunPauseSettledTx(ctx context.Context, tx *sql.Tx, runID string) error {
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts attempt
		JOIN runtime_external_effect_operations operation ON operation.operation_id=attempt.operation_id
		WHERE (operation.agent_run_id=$1 OR EXISTS (SELECT 1 FROM run_fork_selected_contract_runtime_executions execution
			WHERE execution.execution_id=operation.selected_execution_id AND execution.fork_run_id=$1))
		AND attempt.state IN ('authorized','launched','response_observed')`, runID).Scan(&active); err != nil {
		return err
	}
	if active != 0 {
		return fmt.Errorf("live run pause awaits %d admitted provider attempts", active)
	}
	return nil
}
