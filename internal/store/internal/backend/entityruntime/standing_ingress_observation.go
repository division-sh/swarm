package entitystore

import (
	"context"
	"database/sql"
)

func CountStandingEntityStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, entityID string) (int, error) {
	query := `SELECT COUNT(*) FROM entity_state WHERE entity_id = ?`
	if postgres {
		query = `SELECT COUNT(*) FROM entity_state WHERE entity_id = $1::uuid`
	}
	var count int
	err := tx.QueryRowContext(ctx, query, entityID).Scan(&count)
	return count, err
}
