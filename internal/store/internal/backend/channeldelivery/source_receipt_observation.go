package channeldelivery

import (
	"context"
	"database/sql"
)

// This historical storage witness intentionally does not select current work.
func ReadFirstSentSourceReceiptStorageTx(ctx context.Context, tx *sql.Tx, sourceID string) (string, error) {
	var reference string
	err := tx.QueryRowContext(ctx, `SELECT CAST(r.provider_reference AS TEXT) FROM channel_delivery_receipts r
			JOIN channel_delivery_plans p ON p.delivery_id=r.delivery_id
			WHERE p.source_id=$1 AND r.state='sent' ORDER BY r.settled_at LIMIT 1`, sourceID).Scan(&reference)
	return reference, err
}
