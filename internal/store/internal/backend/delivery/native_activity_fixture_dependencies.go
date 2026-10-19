package delivery

import (
	"context"
	"database/sql"
)

// Physical cardinality witnesses deliberately include every delivery status.
// They grant no eligibility or claim authority and do not decode route data.
func CountSelectedForkDeliveryStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}
