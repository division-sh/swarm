package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type FanOutDrainDeliveryStorage = private.FanOutDrainDeliveryStorage
type FanOutDrainNullableText = private.FanOutDrainNullableText

func ReadFanOutDrainDeliveryStorage(ctx context.Context, selected any, runID string) ([]FanOutDrainDeliveryStorage, error) {
	return private.ReadFanOutDrainDeliveryStorageForTest(ctx, selected, runID)
}

func ReadLifecycleProjectionLogPayloads(ctx context.Context, selected any) ([][]byte, error) {
	return private.ReadLifecycleProjectionLogPayloadsForTest(ctx, selected)
}
