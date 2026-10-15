package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadRunFanOutBarrierStatusStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadRunFanOutBarrierStatusStorageForTest(ctx, selected, runID)
}
