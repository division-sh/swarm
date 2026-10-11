package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type RunEventCounterStorageConsistency = private.RunEventCounterStorageConsistency
type FanOutSemanticRejectionStorage = private.FanOutSemanticRejectionStorage
type FanOutObligationHistoryStorage = private.FanOutObligationHistoryStorage

func ReadRunEventCounterStorageConsistency(ctx context.Context, selected any, runID string) (RunEventCounterStorageConsistency, error) {
	return private.ReadRunEventCounterStorageConsistencyForTest(ctx, selected, runID)
}

func ReadFanOutSemanticRejectionStorage(ctx context.Context, selected any, runID string) ([]FanOutSemanticRejectionStorage, error) {
	return private.ReadFanOutSemanticRejectionStorageForTest(ctx, selected, runID)
}

func ReadFanOutObligationHistoryStorage(ctx context.Context, selected any, runID string) ([]FanOutObligationHistoryStorage, error) {
	return private.ReadFanOutObligationHistoryStorageForTest(ctx, selected, runID)
}
