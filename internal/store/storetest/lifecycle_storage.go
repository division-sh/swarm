package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadLifecycleEventCardinality(ctx context.Context, selected any, runID, eventName string) (int, error) {
	return private.ReadLifecycleEventCardinalityForTest(ctx, selected, runID, eventName)
}
