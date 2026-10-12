package mutationlog

import (
	"context"
	"database/sql"
)

func CountRunMutationStorageTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1`, run).Scan(&count)
	return count, err
}
