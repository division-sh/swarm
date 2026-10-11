package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ActivityForkSourceStorage = private.ActivityForkSourceStorage
type ActivityForkResultStorage = private.ActivityForkResultStorage
type ActivityForkAttemptStorage = private.ActivityForkAttemptStorage

func ReadActivityForkSourceStorage(ctx context.Context, selected any, runID, entityID, requestID string) (ActivityForkSourceStorage, error) {
	return private.ReadActivityForkSourceStorageForTest(ctx, selected, runID, entityID, requestID)
}

func ReadActivityForkRequestPayloadStorage(ctx context.Context, selected any, runID, eventID string) ([]byte, error) {
	return private.ReadActivityForkRequestPayloadStorageForTest(ctx, selected, runID, eventID)
}

func ReadActivityForkResultStorage(ctx context.Context, selected any, runID, eventID string) (ActivityForkResultStorage, error) {
	return private.ReadActivityForkResultStorageForTest(ctx, selected, runID, eventID)
}

func ReadActivityForkAttemptStorage(ctx context.Context, selected any, requestID string) (ActivityForkAttemptStorage, error) {
	return private.ReadActivityForkAttemptStorageForTest(ctx, selected, requestID)
}

func CountActivityForkAttemptStorage(ctx context.Context, selected any, requestID string) (int, error) {
	return private.CountActivityForkAttemptStorageForTest(ctx, selected, requestID)
}
