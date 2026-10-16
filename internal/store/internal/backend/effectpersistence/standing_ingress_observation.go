package effectpersistence

import (
	"context"
	"database/sql"
)

func CountExternalEffectOperationsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_operations`).Scan(&count)
	return count, err
}
