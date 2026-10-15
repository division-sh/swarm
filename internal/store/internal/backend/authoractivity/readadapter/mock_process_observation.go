package authoractivityadapter

import (
	"context"
	"database/sql"
)

func CountMockAuthorActivityStorageTx(ctx context.Context, tx *sql.Tx, postgres bool) (int, error) {
	query := `SELECT COUNT(*) FROM author_activity_occurrences WHERE json_extract(projection, '$.execution_mode') = 'mock'`
	if postgres {
		query = `SELECT COUNT(*) FROM author_activity_occurrences WHERE projection->>'execution_mode' = 'mock'`
	}
	var count int
	err := tx.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}
