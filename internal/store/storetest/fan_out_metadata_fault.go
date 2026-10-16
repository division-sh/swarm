package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type FanOutRunClaimMetadataStorage = private.FanOutRunClaimMetadataStorage

func ReadFanOutRunCapsuleWire(ctx context.Context, selected any, runID string) ([]byte, error) {
	return private.ReadFanOutRunCapsuleWireForTest(ctx, selected, runID)
}

func ReadFanOutRunClaimMetadataStorage(ctx context.Context, selected any, runID string) (FanOutRunClaimMetadataStorage, error) {
	return private.ReadFanOutRunClaimMetadataStorageForTest(ctx, selected, runID)
}

func SetRetainedFanOutCapsuleWire(ctx context.Context, selected any, runID string, raw []byte) (int64, error) {
	return private.SetRetainedFanOutCapsuleWireForTest(ctx, selected, runID, raw)
}

func SetRetainedFanOutCardinality(ctx context.Context, selected any, runID string, cardinality int) (int64, error) {
	return private.SetRetainedFanOutCardinalityForTest(ctx, selected, runID, cardinality)
}
