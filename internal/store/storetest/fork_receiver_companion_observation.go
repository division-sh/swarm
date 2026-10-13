package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadForkReceiverCompanionStorage(ctx context.Context, selected any, runID string, paths ...string) (map[string][]string, error) {
	return private.ReadForkReceiverCompanionStorageForTest(ctx, selected, runID, paths...)
}
