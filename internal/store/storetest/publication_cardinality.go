package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountApplicationRunEvents(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountApplicationRunEventStorageForTest(ctx, selected, runID)
}

func CountExactRunEventDeliveries(ctx context.Context, selected any, runID, eventID string) (int, error) {
	return private.CountExactRunEventDeliveryStorageForTest(ctx, selected, runID, eventID)
}
