package storetest

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SeedSelectedForkSourceTimer(ctx context.Context, selected any, runID, entityID string, ref timeridentity.WorkflowTimerActivationRef, at time.Time) error {
	return private.SeedSelectedForkSourceTimerForTest(ctx, selected, runID, entityID, ref, at)
}

func ReadSelectedForkSourceTimerStatus(ctx context.Context, selected any, timerID, runID string) (string, error) {
	return private.ReadSelectedForkSourceTimerStatusForTest(ctx, selected, timerID, runID)
}
