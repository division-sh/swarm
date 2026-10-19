package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SeedHistoricalActivityForkAttempt(ctx context.Context, selected any, record pipeline.ActivityAttemptRecord) error {
	return private.SeedHistoricalActivityForkAttemptForTest(ctx, selected, record)
}
