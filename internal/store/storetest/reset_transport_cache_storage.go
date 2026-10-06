package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadResetTransportCacheEntryCount(ctx context.Context, selected any) (int, error) {
	return private.ReadResetTransportCacheEntryCountForTest(ctx, selected)
}
