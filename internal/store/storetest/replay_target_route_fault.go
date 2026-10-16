package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CorruptReplayEventTargetRoute(ctx context.Context, selected any, eventID string) error {
	return private.CorruptReplayEventTargetRouteForTest(ctx, selected, eventID)
}

func RestoreReplayEventTargetRoute(ctx context.Context, selected any, original events.Event) error {
	return private.RestoreReplayEventTargetRouteForTest(ctx, selected, original)
}
