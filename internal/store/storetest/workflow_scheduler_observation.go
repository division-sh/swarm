package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountRunTimerStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunTimerStorageForTest(ctx, selected, runID)
}

func CountRunEventTimerStorage(ctx context.Context, selected any, runID, eventType string) (int, error) {
	return private.CountRunEventTimerStorageForTest(ctx, selected, runID, eventType)
}

func CountWorkflowSchedulerTimers(ctx context.Context, selected any, entity, path string) (int64, error) {
	return private.CountWorkflowSchedulerTimersForTest(ctx, selected, entity, path)
}
