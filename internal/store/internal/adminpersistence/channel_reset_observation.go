package adminpersistence

import (
	"context"
	"database/sql"
)

func ReadChannelResetRecordStorageTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var record string
	err := tx.QueryRowContext(ctx, `SELECT CAST(record AS TEXT) FROM runtime_reset_operations LIMIT 1`).Scan(&record)
	return record, err
}
