package genericschedule

import (
	"context"
	"database/sql"
)

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
