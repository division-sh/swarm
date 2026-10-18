package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
)

const agentTurnLiveParentMatch = `p.turn_id=t.turn_id AND p.run_id=t.run_id
	AND p.completion_attempt_id=t.completion_attempt_id AND p.capability_surface_id=t.capability_surface_id
	AND NOT EXISTS (SELECT 1 FROM run_fork_selected_contract_turn_detachments retained WHERE retained.turn_id=t.turn_id)`

const completionTurnLiveParentExists = `EXISTS (SELECT 1 FROM agent_turns t
	JOIN agent_turn_live_parents p ON ` + agentTurnLiveParentMatch + `
	WHERE t.completion_attempt_id=a.attempt_id)`

func insertTurnLiveParentsTx(ctx context.Context, tx *sql.Tx, turnID string) error {
	if ctx == nil || tx == nil {
		return fmt.Errorf("completion turn pairing requires its native transaction and context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO agent_turn_live_parents
		(turn_id,run_id,completion_attempt_id,capability_surface_id)
		SELECT turn_id,run_id,completion_attempt_id,capability_surface_id FROM agent_turns WHERE turn_id=$1
		AND NOT EXISTS (SELECT 1 FROM run_fork_selected_contract_turn_detachments retained WHERE retained.turn_id=agent_turns.turn_id)`, turnID)
	if err != nil {
		return fmt.Errorf("pair exact completion turn live parents: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read completion turn live-parent insert count: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("completion turn pairing requires one exact canonical row: count=%d", count)
	}
	return nil
}
