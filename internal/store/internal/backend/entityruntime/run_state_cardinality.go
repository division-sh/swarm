package entitystore

import (
	"context"
	"database/sql"
)

func CountRunStateEntityStorageTx(ctx context.Context, tx *sql.Tx, runID, state string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND current_state=$2`, runID, state).Scan(&count)
	return count, err
}
