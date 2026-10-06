package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ReceiverConstructionStorage = private.ReceiverConstructionStorage

func ReadReceiverConstructionStorage(ctx context.Context, selected any, runID, instance, entityID, entityType string) (ReceiverConstructionStorage, error) {
	return private.ReadReceiverConstructionStorageForTest(ctx, selected, runID, instance, entityID, entityType)
}
