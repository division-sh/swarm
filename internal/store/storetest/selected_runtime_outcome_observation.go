package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountSelectedContractBindingStorage(ctx context.Context, selected any) (int, error) {
	return private.CountSelectedContractBindingStorageForTest(ctx, selected)
}

func CountSelectedRuntimeExecutionStorage(ctx context.Context, selected any, forkRunID string) (int, error) {
	return private.CountSelectedRuntimeExecutionStorageForTest(ctx, selected, forkRunID)
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
