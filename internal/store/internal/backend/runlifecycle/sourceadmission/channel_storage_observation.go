package sourceadmission

import (
	"context"
	"database/sql"
)

func CountChannelSourceArtifactsStorageTx(ctx context.Context, tx *sql.Tx, hash string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM source_artifacts WHERE bundle_hash=$1`, hash).Scan(&count)
	return count, err
}
