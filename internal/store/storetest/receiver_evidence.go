package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadReceiverHistoricalEntityState(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID string, revision int64) (runfork.RunForkEntityState, error) {
	return private.ReadReceiverHistoricalEntityStateForTest(ctx, selected, owner, entityID, revision)
}

func ReadEventIdempotencyCardinality(ctx context.Context, selected any, key string) (int, error) {
	return private.ReadEventIdempotencyCardinalityForTest(ctx, selected, key)
}
