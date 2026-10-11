package eventrecord

import (
	"context"
	"database/sql"
)

func CountRunNamedEventStorageTx(ctx context.Context, tx *sql.Tx, runID, eventName string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2`, runID, eventName).Scan(&out)
	return out, err
}

func CountRunExactEventStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, runID, eventID).Scan(&out)
	return out, err
}
