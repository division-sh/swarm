package storetest

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SeedHistoricalActivityForkLoop(ctx context.Context, selected any, runID, entityID, requestID string, activation loopruntime.Activation, at time.Time) error {
	return private.SeedHistoricalActivityForkLoopForTest(ctx, selected, runID, entityID, requestID, activation, at)
}

func SeedHistoricalActivityForkAttempt(ctx context.Context, selected any, record pipeline.ActivityAttemptRecord) error {
	return private.SeedHistoricalActivityForkAttemptForTest(ctx, selected, record)
}
