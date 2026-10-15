package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type DeploymentIntentStatusStorage = private.DeploymentIntentStatusStorage
type DeploymentCompletionClockStorage = private.DeploymentCompletionClockStorage

func ReadDeploymentIntentStatusStorage(ctx context.Context, selected any, run string) (DeploymentIntentStatusStorage, error) {
	return private.ReadDeploymentIntentStatusStorageForTest(ctx, selected, run)
}

func ReadDeploymentCompletionClockStorage(ctx context.Context, selected any, run string) (DeploymentCompletionClockStorage, error) {
	return private.ReadDeploymentCompletionClockStorageForTest(ctx, selected, run)
}

func CountRunTransportCacheStorage(ctx context.Context, selected any, run string) (int, error) {
	return private.CountRunTransportCacheStorageForTest(ctx, selected, run)
}

func ExpireRunTransportCache(ctx context.Context, selected any, run string) error {
	return private.ExpireRunTransportCacheForTest(ctx, selected, run)
}
