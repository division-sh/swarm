package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type PreparedTargetConflictStorage = private.PreparedTargetConflictStorage

func SetPreparedTargetConflict(ctx context.Context, selected any, eventID, originalIdentity string, conflicting events.DeliveryRoute) (int64, error) {
	return private.SetPreparedTargetConflictForTest(ctx, selected, eventID, originalIdentity, conflicting)
}

func ReadPreparedTargetConflict(ctx context.Context, selected any, eventID, routeIdentity string) (PreparedTargetConflictStorage, error) {
	return private.ReadPreparedTargetConflictForTest(ctx, selected, eventID, routeIdentity)
}
