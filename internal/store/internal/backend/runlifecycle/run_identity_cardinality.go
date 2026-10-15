package runlifecycle

import (
	"context"
	"database/sql"
)

func CountRunIdentityStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}
