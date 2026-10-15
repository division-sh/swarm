package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type WorkflowTimerActivationStorage = private.WorkflowTimerActivationStorage
type WorkflowTimerOccurrenceStorage = private.WorkflowTimerOccurrenceStorage

func ReadWorkflowTimerActivationStorage(ctx context.Context, selected any, runID, entityID string) (WorkflowTimerActivationStorage, error) {
	return private.ReadWorkflowTimerActivationStorageForTest(ctx, selected, runID, entityID)
}

func ReadWorkflowTimerOccurrenceStorage(ctx context.Context, selected any, runID, entityID string) (WorkflowTimerOccurrenceStorage, error) {
	return private.ReadWorkflowTimerOccurrenceStorageForTest(ctx, selected, runID, entityID)
}

func CountWorkflowTimerStatusRows(ctx context.Context, selected any, runID, entityID, status string) (int, error) {
	return private.CountWorkflowTimerStatusRowsForTest(ctx, selected, runID, entityID, status)
}

func CountRunTimerStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunTimerStorageForTest(ctx, selected, runID)
}

func CountRunEventTimerStorage(ctx context.Context, selected any, runID, eventType string) (int, error) {
	return private.CountRunEventTimerStorageForTest(ctx, selected, runID, eventType)
}

func CountWorkflowSchedulerTimers(ctx context.Context, selected any, entity, path string) (int64, error) {
	return private.CountWorkflowSchedulerTimersForTest(ctx, selected, entity, path)
}
