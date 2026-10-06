package runtimepersistence

import (
	"context"
	"database/sql"
)

// The original reset replay proof counts every reset cache row, not merely one
// idempotency key or actor. Cache mutation stays with WithAPIIdempotency.
func ReadResetTransportCacheEntryCountForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_idempotency WHERE method='runtime.nuke'`).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
