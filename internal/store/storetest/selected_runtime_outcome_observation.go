package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountRunFlowRuntimeReadinessStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunFlowRuntimeReadinessStorageForTest(ctx, selected, runID)
}

func CountSelectedForkSourceEventLineageStorage(ctx context.Context, selected any, forkRunID, sourceEventID string) (int, error) {
	return private.CountSelectedForkSourceEventLineageStorageForTest(ctx, selected, forkRunID, sourceEventID)
}

func CountRunEventOrSourceEventStorage(ctx context.Context, selected any, runID, eventID string) (int, error) {
	return private.CountRunEventOrSourceEventStorageForTest(ctx, selected, runID, eventID)
}

func CountSelectedExecutionAttemptsStorage(ctx context.Context, selected any, executionID string) (int, error) {
	return private.CountSelectedExecutionAttemptsStorageForTest(ctx, selected, executionID)
}

func CountSelectedAuthoredHTTPAttemptsStorage(ctx context.Context, selected any, executionID string) (int, error) {
	return private.CountSelectedAuthoredHTTPAttemptsStorageForTest(ctx, selected, executionID)
}

func CountSelectedAuthoredHTTPSettledSurfaceAttemptsStorage(ctx context.Context, selected any, executionID, surfaceID string) (int, error) {
	return private.CountSelectedAuthoredHTTPSettledSurfaceAttemptsStorageForTest(ctx, selected, executionID, surfaceID)
}

func CountClosedFirstSelectedRuntimeExecutionStorage(ctx context.Context, selected any, forkRunID string) (int, error) {
	return private.CountClosedFirstSelectedRuntimeExecutionStorageForTest(ctx, selected, forkRunID)
}

func CountSelectedExecutionSettledAttemptsStorage(ctx context.Context, selected any, executionID string) (int, error) {
	return private.CountSelectedExecutionSettledAttemptsStorageForTest(ctx, selected, executionID)
}

func CountCompletionAttemptBudgetReservationsStorage(ctx context.Context, selected any, attemptID string) (int, error) {
	return private.CountCompletionAttemptBudgetReservationsStorageForTest(ctx, selected, attemptID)
}

func CountSelectedContractBindingStorage(ctx context.Context, selected any) (int, error) {
	return private.CountSelectedContractBindingStorageForTest(ctx, selected)
}

func CountSelectedRuntimeExecutionStorage(ctx context.Context, selected any, forkRunID string) (int, error) {
	return private.CountSelectedRuntimeExecutionStorageForTest(ctx, selected, forkRunID)
}

func CountSelectedSourceRuntimeExecutionsStorage(ctx context.Context, selected any, sourceRunID string) (int, error) {
	return private.CountSelectedSourceRuntimeExecutionsStorageForTest(ctx, selected, sourceRunID)
}

func ReadSelectedRuntimeExecutionStateStorage(ctx context.Context, selected any, forkRunID string) (string, error) {
	return private.ReadSelectedRuntimeExecutionStateStorageForTest(ctx, selected, forkRunID)
}

func CountRunFlowInstanceStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunFlowInstanceStorageForTest(ctx, selected, runID)
}

func CountRunExactEventStorage(ctx context.Context, selected any, runID, eventID string) (int, error) {
	return private.CountRunExactEventStorageForTest(ctx, selected, runID, eventID)
}
