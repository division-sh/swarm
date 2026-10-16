package sourceadmission

import (
	"context"
	"database/sql"
)

func CountPhysicalSourceArtifactsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM source_artifacts`).Scan(&count)
	return count, err
}
