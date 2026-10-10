package storetest

import (
	"context"
	"github.com/division-sh/swarm/internal/operatorread"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type TrackedEntityMutationProjectionStorage = private.TrackedEntityMutationProjectionStorage

func ReadTrackedEntityMutationProjectionStorage(ctx context.Context, selected any, run, entity string) (TrackedEntityMutationProjectionStorage, error) {
	return private.ReadTrackedEntityMutationProjectionStorageForTest(ctx, selected, run, entity)
}

func CorruptRegistryVerdict(ctx context.Context, selected any, run, entity, verdict string) error {
	return private.CorruptRegistryVerdictForTest(ctx, selected, run, entity, verdict)
}

func ReadRunEntityMutationHistoryStorage(ctx context.Context, selected any, run string) ([]operatorread.RunDebugMutation, error) {
	return private.ReadRunEntityMutationHistoryStorageForTest(ctx, selected, run)
}
