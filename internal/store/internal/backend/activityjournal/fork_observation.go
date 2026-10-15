package activityjournal

import (
	"context"
	"database/sql"
)

func ReadActivityForkAttemptStatusStorageTx(ctx context.Context, tx *sql.Tx, runID, requestID string) (string, error) {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT status FROM activity_attempts WHERE request_event_id=$1 AND run_id=$2),'')`, requestID, runID).Scan(&status)
	return status, err
}
