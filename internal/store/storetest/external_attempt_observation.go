package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ExternalAttemptStorage = private.ExternalAttemptStorage
type ProviderTurnAttemptLineageStorage = private.ProviderTurnAttemptLineageStorage

func ReadProviderTurnAttemptLineageStorage(ctx context.Context, selected any) ([]ProviderTurnAttemptLineageStorage, error) {
	return private.ReadProviderTurnAttemptLineageStorageForTest(ctx, selected)
}

func ReadExternalAttemptStorage(ctx context.Context, selected any) ([]ExternalAttemptStorage, error) {
	return private.ReadExternalAttemptStorageForTest(ctx, selected)
}
