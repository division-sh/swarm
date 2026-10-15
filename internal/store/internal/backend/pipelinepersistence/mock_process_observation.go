package pipelinepersistence

import (
	"context"
	"database/sql"
)

func ReadMockEntityIdentityStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, entityID string) (string, error) {
	query := `SELECT entity_id FROM entity_state WHERE entity_id = ?`
	if postgres {
		query = `SELECT entity_id FROM entity_state WHERE entity_id = $1::uuid`
	}
	var stored string
	err := tx.QueryRowContext(ctx, query, entityID).Scan(&stored)
	return stored, err
}
