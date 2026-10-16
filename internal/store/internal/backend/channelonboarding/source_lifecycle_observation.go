package channelonboarding

import (
	"context"
	"database/sql"
)

func CountSourceActivationsStorageTx(ctx context.Context, tx *sql.Tx, hash string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM connected_channel_activations WHERE bundle_hash=$1`, hash).Scan(&count)
	return count, err
}
