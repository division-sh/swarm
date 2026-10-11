package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type RunStopStorage = private.RunStopStorage

func RemovePausedRunControl(ctx context.Context, selected any, runID string) error {
	return private.RemovePausedRunControlForTest(ctx, selected, runID)
}

func ContradictPausedRunControl(ctx context.Context, selected any, runID string) error {
	return private.ContradictPausedRunControlForTest(ctx, selected, runID)
}

func ReadRunStopStorage(ctx context.Context, selected any, runID string) (RunStopStorage, error) {
	return private.ReadRunStopStorageForTest(ctx, selected, runID)
}
