package adminpersistence

import (
	"context"
	"database/sql"
)

// These are the original physical cuts, not validated operation lookup or
// active-operation selection. Replay witnesses interpret the detached record.
func ReadResetCompletionStorageRecordTx(ctx context.Context, tx *sql.Tx) ([]byte, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT record FROM runtime_reset_operations`).Scan(&raw); err != nil {
		return nil, err
	}
	return append([]byte(nil), raw...), nil
}

func CountResetCompletionStorageOperationsTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_reset_operations`).Scan(&count)
	return count, err
}
