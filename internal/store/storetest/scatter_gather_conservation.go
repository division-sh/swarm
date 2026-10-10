package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ScatterGatherPhysicalCounts = private.ScatterGatherPhysicalCounts

func ReadScatterGatherPhysicalCounts(ctx context.Context, selected any, runID string) (ScatterGatherPhysicalCounts, error) {
	return private.ReadScatterGatherPhysicalCountsForTest(ctx, selected, runID)
}
