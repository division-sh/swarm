package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ExternalAttemptStorage = private.ExternalAttemptStorage

func ReadExternalAttemptStorage(ctx context.Context, selected any) ([]ExternalAttemptStorage, error) {
	return private.ReadExternalAttemptStorageForTest(ctx, selected)
}
