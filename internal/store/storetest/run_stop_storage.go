package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type RunStopStorage = private.RunStopStorage
type StopFanOutIntentStorage = private.StopFanOutIntentStorage
type StopFanOutOutcomeStorage = private.StopFanOutOutcomeStorage
type StopRunControlClockStorage = private.StopRunControlClockStorage

func ReadStopFanOutIntentsStorage(ctx context.Context, selected any, runID string) ([]StopFanOutIntentStorage, error) {
	return private.ReadStopFanOutIntentsStorageForTest(ctx, selected, runID)
}

func ReadStopFanOutOutcomesStorage(ctx context.Context, selected any, runID string) ([]StopFanOutOutcomeStorage, error) {
	return private.ReadStopFanOutOutcomesStorageForTest(ctx, selected, runID)
}

func ReadStopRunControlClockStorage(ctx context.Context, selected any, runID string) (StopRunControlClockStorage, error) {
	return private.ReadStopRunControlClockStorageForTest(ctx, selected, runID)
}

func CountRunStopRevisionRows(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunStopRevisionRowsForTest(ctx, selected, runID)
}

func CountRunStopFactRevisionRows(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunStopFactRevisionRowsForTest(ctx, selected, runID)
}

func RemovePausedRunControl(ctx context.Context, selected any, runID string) error {
	return private.RemovePausedRunControlForTest(ctx, selected, runID)
}

func ContradictPausedRunControl(ctx context.Context, selected any, runID string) error {
	return private.ContradictPausedRunControlForTest(ctx, selected, runID)
}

func ReadRunStopStorage(ctx context.Context, selected any, runID string) (RunStopStorage, error) {
	return private.ReadRunStopStorageForTest(ctx, selected, runID)
}

func CountRunStopNonterminalDeliveries(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunStopNonterminalDeliveriesForTest(ctx, selected, runID)
}
