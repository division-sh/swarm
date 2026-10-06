package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type WorkflowJournalStorage = private.WorkflowJournalStorage

func ReadWorkflowJournalStorage(ctx context.Context, selected any, runID string) (WorkflowJournalStorage, error) {
	return private.ReadWorkflowJournalStorageForTest(ctx, selected, runID)
}

func ReadWorkflowActivityAttemptStatuses(ctx context.Context, selected any) ([]string, error) {
	return private.ReadWorkflowActivityAttemptStatusesForTest(ctx, selected)
}
