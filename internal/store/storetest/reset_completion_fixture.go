package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadResetCompletionStorageRecord(ctx context.Context, selected any) ([]byte, error) {
	return private.ReadResetCompletionStorageRecordForTest(ctx, selected)
}

func CountResetCompletionStorageOperations(ctx context.Context, selected any) (int, error) {
	return private.CountResetCompletionStorageOperationsForTest(ctx, selected)
}

func InstallPostgresResetAPICompletionLoss(ctx context.Context, selected any) error {
	return private.InstallPostgresResetAPICompletionLossForTest(ctx, selected)
}

func RemovePostgresResetAPICompletionLoss(ctx context.Context, selected any) error {
	return private.RemovePostgresResetAPICompletionLossForTest(ctx, selected)
}
