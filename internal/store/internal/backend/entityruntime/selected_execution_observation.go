package entitystore

import (
	"context"
	"database/sql"
)

func CountSelectedExecutionRootEntityRowsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$1`, runID).Scan(&count)
	return count, err
}
