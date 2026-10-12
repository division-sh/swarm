package genericschedule

import (
	"context"
	"database/sql"
)

type WorkflowTimerActivationStorage struct {
	TaskID, Status string
}

type WorkflowTimerOccurrenceStorage struct {
	TaskID, Status string
	FireAt         any
}

func ReadWorkflowTimerActivationStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID string) (WorkflowTimerActivationStorage, error) {
	query := `SELECT timer_name, status FROM timers WHERE run_id = ? AND entity_id = ? AND task_type = 'workflow_timer'`
	if postgres {
		query = `SELECT timer_name, status FROM timers WHERE run_id = $1::uuid AND entity_id = $2::uuid AND task_type = 'workflow_timer'`
	}
	var out WorkflowTimerActivationStorage
	if err := tx.QueryRowContext(ctx, query, runID, entityID).Scan(&out.TaskID, &out.Status); err != nil {
		return WorkflowTimerActivationStorage{}, err
	}
	return out, nil
}

func ReadWorkflowTimerOccurrenceStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID string) (WorkflowTimerOccurrenceStorage, error) {
	query := `SELECT timer_name, fire_at, status FROM timers WHERE run_id = ? AND entity_id = ? AND task_type = 'workflow_timer'`
	if postgres {
		query = `SELECT timer_name, fire_at, status FROM timers WHERE run_id = $1::uuid AND entity_id = $2::uuid AND task_type = 'workflow_timer'`
	}
	var out WorkflowTimerOccurrenceStorage
	if err := tx.QueryRowContext(ctx, query, runID, entityID).Scan(&out.TaskID, &out.FireAt, &out.Status); err != nil {
		return WorkflowTimerOccurrenceStorage{}, err
	}
	return out, nil
}

func CountWorkflowTimerStatusRowsTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID, status string) (int, error) {
	query := `SELECT COUNT(*) FROM timers WHERE run_id = ? AND entity_id = ? AND task_type = 'workflow_timer' AND status = ?`
	if postgres {
		query = `SELECT COUNT(*) FROM timers WHERE run_id = $1::uuid AND entity_id = $2::uuid AND task_type = 'workflow_timer' AND status = $3`
	}
	var count int
	err := tx.QueryRowContext(ctx, query, runID, entityID, status).Scan(&count)
	return count, err
}

func CountTimerRowsForRun(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE run_id=$1`, runID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func CountRunEventTimerStorageTx(ctx context.Context, tx *sql.Tx, runID, eventType string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE run_id=$1 AND fire_event=$2`, runID, eventType).Scan(&count)
	return count, err
}

// CountWorkflowSchedulerTimersForTest retains physical history, not active eligibility.
func CountWorkflowSchedulerTimersForTest(ctx context.Context, tx *sql.Tx, postgres bool, entity, path string) (int64, error) {
	query := `SELECT COUNT(*) FROM timers WHERE entity_id = ? AND flow_instance = ? AND owner_agent = ?`
	if postgres {
		query = `SELECT COUNT(*) FROM timers WHERE entity_id = $1::uuid AND flow_instance = $2 AND owner_agent = $3`
	}
	var count int64
	if err := tx.QueryRowContext(ctx, query, entity, path, "workflow-runtime").Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
