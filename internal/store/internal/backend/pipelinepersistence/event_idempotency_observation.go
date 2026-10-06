package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

// ReadEventIdempotencyCardinalityTx retains the exact event-key predicate;
// API command receipts and event-name/run counts are different evidence.
func ReadEventIdempotencyCardinalityTx(ctx context.Context, tx *sql.Tx, key string) (int, error) {
	if tx == nil || key == "" {
		return 0, fmt.Errorf("event-key evidence requires a selected read transaction and nonempty key")
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE idempotency_key=$1`, key).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
