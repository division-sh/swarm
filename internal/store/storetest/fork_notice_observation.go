package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadForkReceiverNoticeStorage(ctx context.Context, selected any, runID string) ([]string, error) {
	return private.ReadForkReceiverNoticeStorageForTest(ctx, selected, runID)
}
