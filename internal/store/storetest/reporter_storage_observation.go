package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type RunEventCounterStorageConsistency = private.RunEventCounterStorageConsistency
type FanOutSemanticRejectionStorage = private.FanOutSemanticRejectionStorage
type FanOutObligationHistoryStorage = private.FanOutObligationHistoryStorage

type RunRevisionFactStorage = private.RunRevisionFactStorage
type FanOutRunBlockedClaimStorage = private.FanOutRunBlockedClaimStorage

func ReadLatestFanOutObligationFactStorage(ctx context.Context, selected any, runID string) (FanOutObligationHistoryStorage, error) {
	return private.ReadLatestFanOutObligationFactStorageForTest(ctx, selected, runID)
}

func ReadRunRevisionFactStorage(ctx context.Context, selected any, runID string, revision int64) (RunRevisionFactStorage, error) {
	return private.ReadRunRevisionFactStorageForTest(ctx, selected, runID, revision)
}

func ReadFanOutRunBlockedClaimStorage(ctx context.Context, selected any, runID string) (FanOutRunBlockedClaimStorage, error) {
	return private.ReadFanOutRunBlockedClaimStorageForTest(ctx, selected, runID)
}

func ReadRunEventCounterStorageConsistency(ctx context.Context, selected any, runID string) (RunEventCounterStorageConsistency, error) {
	return private.ReadRunEventCounterStorageConsistencyForTest(ctx, selected, runID)
}

func ReadFanOutSemanticRejectionStorage(ctx context.Context, selected any, runID string) ([]FanOutSemanticRejectionStorage, error) {
	return private.ReadFanOutSemanticRejectionStorageForTest(ctx, selected, runID)
}

func ReadFanOutObligationHistoryStorage(ctx context.Context, selected any, runID string) ([]FanOutObligationHistoryStorage, error) {
	return private.ReadFanOutObligationHistoryStorageForTest(ctx, selected, runID)
}
