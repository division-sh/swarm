package runlifecycle

import (
	"context"
	"database/sql"
)

func ReadRunStatusStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1`, runID).Scan(&out)
	return out, err
}
