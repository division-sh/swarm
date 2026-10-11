package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountRecordedDeadLetterStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRecordedDeadLetterStorageForTest(ctx, selected, runID)
}
