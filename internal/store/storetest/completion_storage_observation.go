package storetest

import (
	"context"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type CompletionAttemptStorage = private.CompletionAttemptStorage
type CompletionSpendStorage = private.CompletionSpendStorage
type CompletionTargetStorage = private.CompletionTargetStorage

func ReadCompletionAttemptStorage(ctx context.Context, selected runtimeeffects.Store, adapter, attemptID string) ([]CompletionAttemptStorage, error) {
	return private.ReadCompletionAttemptStorageForTest(ctx, selected, adapter, attemptID)
}
