package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ReceiverJoinedInventoryStorageRow = private.ReceiverJoinedInventoryStorageRow
type LifecycleBusinessStorageRow = private.LifecycleBusinessStorageRow

func ReadLifecycleBusinessStorage(ctx context.Context, selected any, runID string) ([]LifecycleBusinessStorageRow, error) {
	return private.ReadLifecycleBusinessStorageForTest(ctx, selected, runID)
}

func ReadReceiverJoinedInventoryStorage(ctx context.Context, selected any, runID string) ([]ReceiverJoinedInventoryStorageRow, error) {
	return private.ReadReceiverJoinedInventoryStorageForTest(ctx, selected, runID)
}
