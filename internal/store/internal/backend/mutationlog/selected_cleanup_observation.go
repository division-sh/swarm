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

func CountRunEventMutationStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2`, runID, eventID).Scan(&count)
	return count, err
}
