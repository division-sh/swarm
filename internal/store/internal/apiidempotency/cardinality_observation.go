package apiidempotency

import (
	"context"
	"database/sql"
	"fmt"
)

// Physical method/key cardinality includes every actor namespace. It is not
// a replay lookup, and never purges expired receipts or normalizes the keys.
func ReadMethodKeyCardinalityTx(ctx context.Context, tx *sql.Tx, method, key string) (int, error) {
	if ctx == nil || tx == nil || method == "" || key == "" {
		return 0, fmt.Errorf("API method/key evidence requires its selected read transaction and exact nonempty keys")
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_idempotency WHERE method = $1 AND idempotency_key = $2`, method, key).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func CountPhysicalAPIReceiptsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_idempotency`).Scan(&count)
	return count, err
}
