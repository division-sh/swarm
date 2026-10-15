package storetest

import (
	"context"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SeedEntityToolHistoricalReadOnlyImport(ctx context.Context, selected any, runID, entityID, eventID string, at time.Time) error {
	return private.SeedEntityToolHistoricalReadOnlyImportForTest(ctx, selected, runID, entityID, eventID, at)
}
