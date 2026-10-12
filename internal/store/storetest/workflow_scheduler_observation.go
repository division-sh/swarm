package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountRunTimerStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunTimerStorageForTest(ctx, selected, runID)
}

func CountWorkflowSchedulerTimers(ctx context.Context, selected any, entity, path string) (int64, error) {
	return private.CountWorkflowSchedulerTimersForTest(ctx, selected, entity, path)
}
