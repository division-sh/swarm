package activityjournal

import (
	"context"
	"database/sql"
)

func ReadNumericMockActivityResultStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT CAST(result_payload AS TEXT) FROM activity_attempts WHERE run_id=$1 AND status='succeeded' AND execution_mode='mock'`, runID).Scan(&raw)
	return raw, err
}
