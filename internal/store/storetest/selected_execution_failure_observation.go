package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SelectedExecutionFailureStorage = private.SelectedExecutionFailureStorage

func ReadSelectedExecutionFailureStorage(ctx context.Context, selected any, executionID string) (SelectedExecutionFailureStorage, error) {
	return private.ReadSelectedExecutionFailureStorageForTest(ctx, selected, executionID)
}

func CountSelectedExecutionActiveAttemptsStorage(ctx context.Context, selected any, executionID string) (int, error) {
	return private.CountSelectedExecutionActiveAttemptsStorageForTest(ctx, selected, executionID)
}
