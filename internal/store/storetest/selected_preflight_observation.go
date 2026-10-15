package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadRunlessSelectedPreparationSurfaceStorage(ctx context.Context, selected any, surfaceID, preparationID, actorID string) ([]byte, error) {
	return private.ReadRunlessSelectedPreparationSurfaceStorageForTest(ctx, selected, surfaceID, preparationID, actorID)
}

func CountGlobalRunStorage(ctx context.Context, selected any) (int, error) {
	return private.CountGlobalRunStorageForTest(ctx, selected)
}

func CountGlobalAgentStorage(ctx context.Context, selected any) (int, error) {
	return private.CountGlobalAgentStorageForTest(ctx, selected)
}

func CountGlobalSelectedRuntimeExecutionStorage(ctx context.Context, selected any) (int, error) {
	return private.CountGlobalSelectedRuntimeExecutionStorageForTest(ctx, selected)
}

func CountGlobalGenerationGrantStorage(ctx context.Context, selected any) (int, error) {
	return private.CountGlobalGenerationGrantStorageForTest(ctx, selected)
}
