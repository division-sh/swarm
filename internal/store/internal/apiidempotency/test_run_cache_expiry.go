package apiidempotency

import (
	"context"
	"database/sql"
)

func CountRunTransportCacheStorageTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_idempotency WHERE resource_id=$1`, run).Scan(&out)
	return out, err
}

// ExpireRunTransportCacheTx leaves permanent domain receipts untouched.
func ExpireRunTransportCacheTx(ctx context.Context, tx *sql.Tx, run string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM api_idempotency WHERE resource_id=$1`, run)
	return err
}
