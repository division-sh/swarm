package runforkrevision

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
)

// This fixed physical witness retains every timer revision, not only the
// latest projection. It exposes neither ledger bodies nor caller SQL.
func CountWorkflowTimerRevisionFactsForTest(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return 0, err
	}
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family=$2`, runID, FamilyTimers).Scan(&count)
	return count, err
}
