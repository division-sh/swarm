package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

type WorkflowTimerReplayStorageObservation struct {
	Timers, ActiveTimers, TimerRevisionFacts, Events int
}

// This fixed readback preserves replay's exact cardinality witnesses. Neither
// the native connection nor a caller-supplied query or callback escapes.
func ObserveWorkflowTimerReplayStorageForTest(ctx context.Context, selected any, runID, entityID string) (WorkflowTimerReplayStorageObservation, error) {
	for _, id := range []string{runID, entityID} {
		if _, err := uuid.Parse(id); err != nil {
			return WorkflowTimerReplayStorageObservation{}, err
		}
	}
	var observed WorkflowTimerReplayStorageObservation
	read := func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM timers WHERE run_id=$1 AND entity_id=$2 AND task_type='workflow_timer'),
			(SELECT COUNT(*) FROM timers WHERE run_id=$1 AND entity_id=$2 AND task_type='workflow_timer' AND status='active'),
			(SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='timers'),
			(SELECT COUNT(*) FROM events WHERE run_id=$1)`, runID, entityID).
			Scan(&observed.Timers, &observed.ActiveTimers, &observed.TimerRevisionFacts, &observed.Events)
	}
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil {
			return observed, fmt.Errorf("timer replay readback requires the original selected store")
		}
		if err = owner.requireCurrentSchema(); err == nil {
			err = owner.backend.RunReadTransaction(ctx, read)
		}
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil {
			return observed, fmt.Errorf("timer replay readback requires the original selected store")
		}
		if err = owner.requireCurrentSchema(); err == nil {
			err = owner.backend.RunReadTransaction(ctx, read)
		}
	default:
		err = fmt.Errorf("timer replay readback requires the original selected store")
	}
	if err != nil {
		return WorkflowTimerReplayStorageObservation{}, err
	}
	return observed, nil
}
