package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ClockStorageObservation = private.ClockStorageObservation

func CountInstanceClockActivations(ctx context.Context, selected any) (int, error) {
	return private.CountInstanceClockActivationsForTest(ctx, selected)
}

func CorruptClockImmutableHash(ctx context.Context, selected any, runID, activationID string) error {
	return private.CorruptClockImmutableHashForTest(ctx, selected, runID, activationID)
}

func ReadClockStorage(ctx context.Context, selected any, runID, activationID string) (ClockStorageObservation, error) {
	return private.ReadClockStorageForTest(ctx, selected, runID, activationID)
}
