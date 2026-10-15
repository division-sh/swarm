package authoractivityadapter

import (
	"context"
	"database/sql"
)

func ReadLatestDeliveryFailureStorageTx(ctx context.Context, tx *sql.Tx, deliveryID string) ([]byte, error) {
	var out []byte
	err := tx.QueryRowContext(ctx, `SELECT failure FROM author_activity_occurrences WHERE source_owner='event_deliveries' AND source_identity=$1 ORDER BY sequence DESC LIMIT 1`, deliveryID).Scan(&out)
	return out, err
}
