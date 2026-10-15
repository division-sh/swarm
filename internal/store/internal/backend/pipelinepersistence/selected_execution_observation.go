package pipelinepersistence

import (
	"context"
	"database/sql"
)

func CountSelectedExecutionRootFlowHeadersStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND entity_id=$1`, runID).Scan(&count)
	return count, err
}

func CountEventReceiptStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1`, eventID).Scan(&count)
	return count, err
}

func ReadLatestReceiptFailureStorageTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var out string
	// Pipeline receipts have a processed_at clock, not updated_at. The old
	// failure-only diagnostic silently ignored that obsolete-column error.
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(CAST(failure AS TEXT),'') FROM event_receipts
		WHERE failure IS NOT NULL ORDER BY processed_at DESC LIMIT 1`).Scan(&out)
	return out, err
}
