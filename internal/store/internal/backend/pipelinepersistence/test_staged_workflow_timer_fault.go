package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

type StagedWorkflowTimerFault string

const (
	StagedWorkflowTimerMissing       StagedWorkflowTimerFault = "missing"
	StagedWorkflowTimerProgressed    StagedWorkflowTimerFault = "progressed"
	StagedWorkflowTimerForeignOrigin StagedWorkflowTimerFault = "foreign_origin"
)

// CorruptStagedWorkflowTimerTxForTest installs one exact hostile paused-child
// row. It cannot mutate ordinary timers or lend SQL to the calling proof.
func CorruptStagedWorkflowTimerTxForTest(ctx context.Context, tx *sql.Tx, runID, activationID string, fault StagedWorkflowTimerFault) error {
	for _, id := range []string{runID, activationID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return fmt.Errorf("staged timer fault requires exact storage identity")
		}
	}
	where := ` WHERE run_id=$1 AND timer_id=$2 AND task_type='workflow_timer'
		AND source_timer_id IS NOT NULL AND status='active'
		AND EXISTS (SELECT 1 FROM runs WHERE run_id=$1 AND status='paused')`
	args := []any{runID, activationID}
	var query string
	switch fault {
	case StagedWorkflowTimerMissing:
		query = `DELETE FROM timers` + where
	case StagedWorkflowTimerProgressed:
		query = `UPDATE timers SET status='cancelled', cancel_cause='rule_removed', cancelled_at=created_at` + where
	case StagedWorkflowTimerForeignOrigin:
		query = `UPDATE timers SET forked_from_point_revision=forked_from_point_revision+1` + where
	default:
		return fmt.Errorf("unknown staged timer fault %q", fault)
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("staged timer fault changed %d rows, want one", changed)
	}
	return nil
}
