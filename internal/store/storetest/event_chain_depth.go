package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SetEventChainDepth(ctx context.Context, selected any, eventID string, depth int) error {
	return private.SetEventChainDepthForTest(ctx, selected, eventID, depth)
}
