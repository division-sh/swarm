package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type StageCompletionFieldlessStorage = private.StageCompletionFieldlessStorage

func CountStageCompletionNodeDeliveries(ctx context.Context, selected any, runID, eventName, nodeID, rootNodeID string) (int, error) {
	return private.CountStageCompletionNodeDeliveriesForTest(ctx, selected, runID, eventName, nodeID, rootNodeID)
}

func ReadStageCompletionDiscoveryState(ctx context.Context, selected any, runID, entityID string) (string, error) {
	return private.ReadStageCompletionDiscoveryStateForTest(ctx, selected, runID, entityID)
}

func ReadStageCompletionFieldlessStorage(ctx context.Context, selected any, runID, stage string) (StageCompletionFieldlessStorage, error) {
	return private.ReadStageCompletionFieldlessStorageForTest(ctx, selected, runID, stage)
}
