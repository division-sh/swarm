package pipelinepersistence

import (
	"context"
	"database/sql"
)

func ReadRunFanOutBarrierStatusStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM fan_out_obligation_barriers WHERE run_id=$1`, runID).Scan(&status); err != nil {
		return "", err
	}
	return status, nil
}
