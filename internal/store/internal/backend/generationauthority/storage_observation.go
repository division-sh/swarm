package generationauthority

import (
	"context"
	"database/sql"
)

func CountGlobalGenerationGrantStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generation_grants`).Scan(&count)
	return count, err
}
