package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadResourcePhysicalSnapshot(ctx context.Context, selected any) (map[string][]string, error) {
	return private.ReadResourcePhysicalSnapshotForTest(ctx, selected)
}

func ReadRunCreationRequestHashStorage(ctx context.Context, selected any, id string) (string, error) {
	return private.ReadRunCreationRequestHashStorageForTest(ctx, selected, id)
}

func ReadSourceRequestWire(ctx context.Context, selected any, id string) ([]byte, error) {
	return private.ReadSourceRequestWireForTest(ctx, selected, id)
}

func CountPruneInvocationStorage(ctx context.Context, selected any, id string) (int, error) {
	return private.CountPruneInvocationStorageForTest(ctx, selected, id)
}
