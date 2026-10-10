package storetest

import (
	"context"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountDeadLettersForOriginalEvent(ctx context.Context, selected any, originalEventID string) (int, error) {
	return private.CountDeadLettersForOriginalEventForTest(ctx, selected, originalEventID)
}

type DeadLetterObservationRow = private.DeadLetterObservationRow

type TargetFailureDeadLetterStorage = private.TargetFailureDeadLetterStorage

func ReadTargetFailureDeadLetterStorage(ctx context.Context, selected any, eventID string) (TargetFailureDeadLetterStorage, error) {
	return private.ReadTargetFailureDeadLetterStorageForTest(ctx, selected, eventID)
}

type ChainDepthDeadLetterStorage = private.ChainDepthDeadLetterStorage

func ReadChainDepthDeadLetterStorage(ctx context.Context, selected any, runID, entityID string) (ChainDepthDeadLetterStorage, error) {
	return private.ReadChainDepthDeadLetterStorageForTest(ctx, selected, runID, entityID)
}

func CountDeadLetterEntityRelationsSince(ctx context.Context, selected any, since time.Time, entityID string) (int, error) {
	return private.CountDeadLetterEntityRelationsSinceForTest(ctx, selected, since, entityID)
}

func ReadDeadLetterObservationRows(ctx context.Context, selected any) ([]DeadLetterObservationRow, error) {
	return private.ReadDeadLetterObservationRowsForTest(ctx, selected)
}
