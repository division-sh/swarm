package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ClosedReceiverReadinessStorage = private.ClosedReceiverReadinessStorage
type ClosedReceiverAuthorityStorage = private.ClosedReceiverAuthorityStorage
type ClosedReceiverEmissionStorage = private.ClosedReceiverEmissionStorage
type ClosedReceiverDeliveryStorage = private.ClosedReceiverDeliveryStorage
type ClosedReceiverAgentRouteStorage = private.ClosedReceiverAgentRouteStorage
type ClosedReceiverReceiptStorage = private.ClosedReceiverReceiptStorage
type ClosedReceiverAgentStorage = private.ClosedReceiverAgentStorage

func ReadClosedReceiverReadinessStorage(ctx context.Context, selected any, runID, bridgeNodeID string) (ClosedReceiverReadinessStorage, error) {
	return private.ReadClosedReceiverReadinessStorageForTest(ctx, selected, runID, bridgeNodeID)
}

func ReadClosedReceiverAuthorityStorage(ctx context.Context, selected any) ([]ClosedReceiverAuthorityStorage, error) {
	return private.ReadClosedReceiverAuthorityStorageForTest(ctx, selected)
}

func ReadClosedReceiverEmissionStorage(ctx context.Context, selected any, runID, eventName string) (ClosedReceiverEmissionStorage, error) {
	return private.ReadClosedReceiverEmissionStorageForTest(ctx, selected, runID, eventName)
}

func CountClosedReceiverPipelineSuccessStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountClosedReceiverPipelineSuccessStorageForTest(ctx, selected, runID)
}

func ReadClosedReceiverDeliveryStorage(ctx context.Context, selected any, runID string) ([]ClosedReceiverDeliveryStorage, error) {
	return private.ReadClosedReceiverDeliveryStorageForTest(ctx, selected, runID)
}

func ReadClosedReceiverReceiptStorage(ctx context.Context, selected any, runID string) ([]ClosedReceiverReceiptStorage, error) {
	return private.ReadClosedReceiverReceiptStorageForTest(ctx, selected, runID)
}

func ReadClosedReceiverAgentStorage(ctx context.Context, selected any) ([]ClosedReceiverAgentStorage, error) {
	return private.ReadClosedReceiverAgentStorageForTest(ctx, selected)
}

func ReadClosedReceiverAgentRouteStorage(ctx context.Context, selected any, runID string) ([]ClosedReceiverAgentRouteStorage, error) {
	return private.ReadClosedReceiverAgentRouteStorageForTest(ctx, selected, runID)
}

func ReadClosedReceiverRuntimeLogStorage(ctx context.Context, selected any, runID string) ([]string, error) {
	return private.ReadClosedReceiverRuntimeLogStorageForTest(ctx, selected, runID)
}
