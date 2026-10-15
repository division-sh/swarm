package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
)

type SelectedExecutionFailureStorage = effectpersistence.SelectedExecutionFailureStorage

func ReadSelectedExecutionFailureStorageForTest(ctx context.Context, selected any, executionID string) (SelectedExecutionFailureStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{executionID}, func(ctx context.Context, tx *sql.Tx) (SelectedExecutionFailureStorage, error) {
		return effectpersistence.ReadSelectedExecutionFailureStorageTx(ctx, tx, executionID)
	})
}

func CountSelectedExecutionActiveAttemptsStorageForTest(ctx context.Context, selected any, executionID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{executionID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return effectpersistence.CountSelectedExecutionActiveAttemptsStorageTx(ctx, tx, executionID)
	})
}
