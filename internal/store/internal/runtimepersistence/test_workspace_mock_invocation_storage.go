package runtimepersistence

import (
	"context"
	"database/sql"
)

type WorkspaceMockInvocationStorage struct {
	AgentDeliveries, Delivered, Emitted int
}

// This closed witness observes the invocation's private test store. It cannot
// select an event name, target, SQL shape, or another invocation's location.
func ReadWorkspaceMockInvocationStorageForTest(ctx context.Context, selected any) (WorkspaceMockInvocationStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return WorkspaceMockInvocationStorage{}, err
	}
	var out WorkspaceMockInvocationStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM event_deliveries WHERE subscriber_type='agent'),
			(SELECT COUNT(*) FROM event_deliveries WHERE subscriber_type='agent' AND status='delivered'),
			(SELECT COUNT(*) FROM events WHERE event_name='work.completed')`).Scan(&out.AgentDeliveries, &out.Delivered, &out.Emitted)
	})
	if err != nil {
		return WorkspaceMockInvocationStorage{}, err
	}
	return out, nil
}
