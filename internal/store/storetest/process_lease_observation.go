package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/channelonboarding"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadProcessGenerationCardinality(ctx context.Context, selected any) (int, int, error) {
	return private.ReadProcessGenerationCardinalityForTest(ctx, selected)
}

func ReplaceRetainedChannelRuntime(ctx context.Context, selected channelonboarding.Store, activation, from, to string) error {
	return private.ReplaceRetainedChannelRuntimeForTest(ctx, selected, activation, from, to)
}
