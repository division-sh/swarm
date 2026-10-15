package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type GateCompletionCardStorage = private.GateCompletionCardStorage
type GateCompletionFlowStorage = private.GateCompletionFlowStorage
type GateCompletionAPIStorage = private.GateCompletionAPIStorage

func ReadGateCompletionPrincipalStorage(ctx context.Context, selected any) (string, error) {
	return private.ReadGateCompletionPrincipalStorageForTest(ctx, selected)
}

func ReadGateCompletionCardStorage(ctx context.Context, selected any, cardID string) (GateCompletionCardStorage, error) {
	return private.ReadGateCompletionCardStorageForTest(ctx, selected, cardID)
}

func ReadGateCompletionFlowStorage(ctx context.Context, selected any, runID string) (GateCompletionFlowStorage, error) {
	return private.ReadGateCompletionFlowStorageForTest(ctx, selected, runID)
}

func ReadGateCompletionChangesStorage(ctx context.Context, selected any, cardID string) ([][3]string, error) {
	return private.ReadGateCompletionChangesStorageForTest(ctx, selected, cardID)
}

func ReadGateCompletionReceiptsStorage(ctx context.Context, selected any, runID string) ([][5]string, error) {
	return private.ReadGateCompletionReceiptsStorageForTest(ctx, selected, runID)
}

func ReadGateCompletionAPIStorage(ctx context.Context, selected any, cardID, method, key string) ([]GateCompletionAPIStorage, error) {
	return private.ReadGateCompletionAPIStorageForTest(ctx, selected, cardID, method, key)
}
