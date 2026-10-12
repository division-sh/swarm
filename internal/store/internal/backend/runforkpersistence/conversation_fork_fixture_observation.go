package runforkpersistence

import (
	"context"
	"database/sql"
)

// This is physical cardinality, including expired and deleted fork history.
func CountConversationForkStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT count(*) FROM conversation_forks`).Scan(&count)
	return count, err
}
