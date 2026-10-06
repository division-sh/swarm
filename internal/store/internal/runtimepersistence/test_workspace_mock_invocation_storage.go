package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
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
		var err error
		out.AgentDeliveries, out.Delivered, out.Emitted, err = delivery.ReadWorkspaceInvocationStorageCounts(ctx, tx)
		return err
	})
	if err != nil {
		return WorkspaceMockInvocationStorage{}, err
	}
	return out, nil
}
