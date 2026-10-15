package runforkpersistence

import (
	"context"
	"database/sql"
)

func ReadChannelSourceForkFailureStorageTx(ctx context.Context, tx *sql.Tx, key string) (sql.NullString, error) {
	var failure sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT CAST(failure_json AS TEXT) FROM run_fork_operations WHERE idempotency_key=$1`, key).Scan(&failure)
	return failure, err
}
