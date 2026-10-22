package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ReceiverConstructionStorage = private.ReceiverConstructionStorage

func RemoveForkReceiverFieldCompanion(ctx context.Context, selected any, runID, entityID, instance string) error {
	return private.RemoveForkReceiverFieldCompanionForTest(ctx, selected, runID, entityID, instance)
}

func ReadReceiverConstructionStorage(ctx context.Context, selected any, runID, instance, entityID, entityType string) (ReceiverConstructionStorage, error) {
	return private.ReadReceiverConstructionStorageForTest(ctx, selected, runID, instance, entityID, entityType)
}
