package pipelinepersistence

import (
	"context"
	"database/sql"
)

type FanOutRunProgressStorage struct{ Total, Cursor int }

func ReadRunFanOutProgressStorageTx(ctx context.Context, tx *sql.Tx, runID string) (FanOutRunProgressStorage, error) {
	var out FanOutRunProgressStorage
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),SUM(cursor) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Total, &out.Cursor)
	if err != nil {
		return FanOutRunProgressStorage{}, err
	}
	return out, nil
}

func CountLeasedRunFanOutStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND claim_owner IS NOT NULL`, runID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
