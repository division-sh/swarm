package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadWorkflowStateEntityStorage(ctx context.Context, selected any, runID, instance, state string) (string, bool, error) {
	return private.ReadWorkflowStateEntityStorageForTest(ctx, selected, runID, instance, state)
}

func CountRunStateEntityStorage(ctx context.Context, selected any, runID, state string) (int, error) {
	return private.CountRunStateEntityStorageForTest(ctx, selected, runID, state)
}
