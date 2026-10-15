package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/durabledata"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ResourceVersionMetadataStorage = private.ResourceVersionMetadataStorage
type RootReadyResourceHeadStorage = private.RootReadyResourceHeadStorage

func ReadPortfolioResourcePinStorage(ctx context.Context, selected any, runID, eventName string) (string, error) {
	return private.ReadPortfolioResourcePinStorageForTest(ctx, selected, runID, eventName)
}

func CountRunResourcePinsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunResourcePinsStorageForTest(ctx, selected, runID)
}

func CountRunDataCreationOperationsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunDataCreationOperationsStorageForTest(ctx, selected, runID)
}

func CountResourceVersionStorage(ctx context.Context, selected any, versionID string) (int, error) {
	return private.CountResourceVersionStorageForTest(ctx, selected, versionID)
}

func ReadResourceVersionMetadataStorage(ctx context.Context, selected any, versionID string) (ResourceVersionMetadataStorage, error) {
	return private.ReadResourceVersionMetadataStorageForTest(ctx, selected, versionID)
}

func ReadResourceVersionJSONLStorage(ctx context.Context, selected any, versionID string) ([]byte, error) {
	return private.ReadResourceVersionJSONLStorageForTest(ctx, selected, versionID)
}

func CountRootReadyResourceVersionsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountRootReadyResourceVersionsStorageForTest(ctx, selected)
}

func CountRootReadyResourceHeadHistoryStorage(ctx context.Context, selected any) (int, error) {
	return private.CountRootReadyResourceHeadHistoryStorageForTest(ctx, selected)
}

func ReadRootReadyResourceHeadStorage(ctx context.Context, selected any) (RootReadyResourceHeadStorage, error) {
	return private.ReadRootReadyResourceHeadStorageForTest(ctx, selected)
}

func ReadResourceHeadVersionStorage(ctx context.Context, selected any, ref durabledata.DeclarationRef) (string, error) {
	return private.ReadResourceHeadVersionStorageForTest(ctx, selected, ref)
}

func CountResourceSourceInvocationStorage(ctx context.Context, selected any, sourceID string) (int, error) {
	return private.CountResourceSourceInvocationStorageForTest(ctx, selected, sourceID)
}

func CountGlobalResourceSourceInvocationsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountGlobalResourceSourceInvocationsStorageForTest(ctx, selected)
}

func CountGlobalDataCreationOperationsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountGlobalDataCreationOperationsStorageForTest(ctx, selected)
}

func CountGlobalResourceVersionsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountGlobalResourceVersionsStorageForTest(ctx, selected)
}

func CountGlobalResourcePinsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountGlobalResourcePinsStorageForTest(ctx, selected)
}
