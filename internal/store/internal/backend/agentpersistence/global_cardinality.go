package agentpersistence

import (
	"context"
	"database/sql"
)

func CountGlobalAgentStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents`).Scan(&count)
	return count, err
}
