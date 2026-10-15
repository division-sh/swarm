package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SelectedForkCleanupStorageCounts = private.SelectedForkCleanupStorageCounts

func ReadPostgresSelectedForkCleanupStorage(ctx context.Context, selected any, run string) (SelectedForkCleanupStorageCounts, error) {
	return private.ReadPostgresSelectedForkCleanupStorageForTest(ctx, selected, run)
}

func SetSelectedReplayScopeCollisionFault(ctx context.Context, selected any, enabled bool) error {
	return private.SetSelectedReplayScopeCollisionFaultForTest(ctx, selected, enabled)
}

func SetSelectedExecutionLineageInsertFault(ctx context.Context, selected any, enabled bool) error {
	return private.SetSelectedExecutionLineageInsertFaultForTest(ctx, selected, enabled)
}
