package eventpersistence

import (
	"context"
	"database/sql"
)

func CountInboundPublicationsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_publications`).Scan(&count)
	return count, err
}
