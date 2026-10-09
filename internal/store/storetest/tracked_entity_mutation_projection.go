package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type TrackedEntityMutationProjectionStorage = private.TrackedEntityMutationProjectionStorage

func ReadTrackedEntityMutationProjectionStorage(ctx context.Context, selected any, run, entity string) (TrackedEntityMutationProjectionStorage, error) {
	return private.ReadTrackedEntityMutationProjectionStorageForTest(ctx, selected, run, entity)
}

func CorruptRegistryVerdict(ctx context.Context, selected any, run, entity, verdict string) error {
	return private.CorruptRegistryVerdictForTest(ctx, selected, run, entity, verdict)
}
