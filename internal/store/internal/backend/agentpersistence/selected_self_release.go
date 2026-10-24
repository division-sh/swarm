package agentpersistence

import (
	"context"
	"database/sql"
	"errors"
)

// RequireSelectedSelfReleaseTx is only a disposition precondition: the selected
// grant must still be admitted, but its actors must have completed self-release.
func RequireSelectedSelfReleaseTx(ctx context.Context, tx *sql.Tx, runID, executionID string) error {
	var admitted bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runtime_generation_grants g
		WHERE g.selected_execution_id=$1 AND g.selected_fork_run_id=$2 AND g.state='admitted'
		AND NOT EXISTS (SELECT 1 FROM runtime_generation_grants next WHERE next.grant_id=g.grant_id AND next.state_version>g.state_version))`, executionID, runID).Scan(&admitted); err != nil {
		return err
	}
	if !admitted {
		return errors.New("selected live handoff requires its still-admitted generation")
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents agent
		WHERE agent.run_id=$1 AND (agent.lifecycle_phase<>'terminated' OR agent.lifecycle_run_mode<>'stopped')
		AND EXISTS (SELECT 1 FROM runtime_generation_grants g
			WHERE g.grant_id=agent.lifecycle_generation_grant_id AND g.selected_execution_id=$2)`, runID, executionID).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return errors.New("selected live handoff awaits exact actor self-release")
	}
	return nil
}

func RequireRunActorsParkedTx(ctx context.Context, tx *sql.Tx, runID string) error {
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents WHERE run_id=$1
		AND lifecycle_run_mode<>'stopped' AND lifecycle_phase NOT IN ('terminated','failed')`, runID).Scan(&active); err != nil {
		return err
	}
	if active != 0 {
		return errors.New("live run pause awaits its admitted actor work")
	}
	return nil
}
