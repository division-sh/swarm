package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountWorkflowSchedulerTimers(ctx context.Context, selected any, entity, path string) (int64, error) {
	return private.CountWorkflowSchedulerTimersForTest(ctx, selected, entity, path)
}
