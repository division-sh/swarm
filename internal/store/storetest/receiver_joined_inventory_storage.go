package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ReceiverJoinedInventoryStorageRow = private.ReceiverJoinedInventoryStorageRow

func ReadReceiverJoinedInventoryStorage(ctx context.Context, selected any, runID string) ([]ReceiverJoinedInventoryStorageRow, error) {
	return private.ReadReceiverJoinedInventoryStorageForTest(ctx, selected, runID)
}
