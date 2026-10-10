package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadRunNamedEventIdentityStorage(ctx context.Context, selected any, runID, eventName string) (string, error) {
	return private.ReadRunNamedEventIdentityStorageForTest(ctx, selected, runID, eventName)
}
