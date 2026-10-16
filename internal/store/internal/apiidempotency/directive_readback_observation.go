package apiidempotency

import (
	"context"
	"database/sql"
)

func ReadDirectiveProjectionResourceStorageTx(ctx context.Context, tx *sql.Tx, key string) (string, error) {
	var resource string
	err := tx.QueryRowContext(ctx, `SELECT resource_id FROM api_idempotency WHERE method = 'agent.send_directive' AND idempotency_key = $1`, key).Scan(&resource)
	return resource, err
}
