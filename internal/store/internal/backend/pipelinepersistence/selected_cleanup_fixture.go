package pipelinepersistence

import (
	"context"
	"database/sql"
)

func CountGlobalPipelineReceiptStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts`).Scan(&count)
	return count, err
}
