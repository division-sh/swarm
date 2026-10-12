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

func CountAdmittedUnboundBundleGrantsStorageTx(ctx context.Context, tx *sql.Tx, bundleHash string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generation_grants WHERE bundle_hash=$1 AND state='admitted' AND selected_binding_id IS NULL`, bundleHash).Scan(&out)
	return out, err
}
