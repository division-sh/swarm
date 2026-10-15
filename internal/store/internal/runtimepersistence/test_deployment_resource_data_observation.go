package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/durabledata"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

type ResourceVersionMetadataStorage = storedurabledata.ResourceVersionMetadataStorage
type RootReadyResourceHeadStorage = storedurabledata.RootReadyResourceHeadStorage

func ReadPortfolioResourcePinStorageForTest(ctx context.Context, selected any, runID, eventName string) (string, error) {
	if err := (durabledata.DeclarationRef{FlowPath: "portfolio", EventName: eventName}).Validate(); err != nil {
		return "", err
	}
	return readDeploymentStorageForTest(ctx, selected, runID, func(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
		return storedurabledata.ReadPortfolioResourcePinStorageTx(ctx, tx, runID, eventName)
	})
}

func CountRunResourcePinsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, storedurabledata.CountRunResourcePinsStorageTx)
}

func CountRunDataCreationOperationsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, storedurabledata.CountRunDataCreationOperationsStorageTx)
}

func CountResourceVersionStorageForTest(ctx context.Context, selected any, versionID string) (int, error) {
	if err := durabledata.VersionID(versionID).Validate(); err != nil {
		return 0, err
	}
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return storedurabledata.CountResourceVersionStorageTx(ctx, tx, versionID)
	})
}

func ReadResourceVersionMetadataStorageForTest(ctx context.Context, selected any, versionID string) (ResourceVersionMetadataStorage, error) {
	if err := durabledata.VersionID(versionID).Validate(); err != nil {
		return ResourceVersionMetadataStorage{}, err
	}
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ResourceVersionMetadataStorage, error) {
		return storedurabledata.ReadResourceVersionMetadataStorageTx(ctx, tx, versionID)
	})
}

func ReadResourceVersionJSONLStorageForTest(ctx context.Context, selected any, versionID string) ([]byte, error) {
	if err := durabledata.VersionID(versionID).Validate(); err != nil {
		return nil, err
	}
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]byte, error) {
		return storedurabledata.ReadResourceVersionJSONLStorageTx(ctx, tx, versionID)
	})
}

func CountRootReadyResourceVersionsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readSelectedPreflightGlobalCount(ctx, selected, storedurabledata.CountRootReadyResourceVersionsStorageTx)
}

func CountRootReadyResourceHeadHistoryStorageForTest(ctx context.Context, selected any) (int, error) {
	return readSelectedPreflightGlobalCount(ctx, selected, storedurabledata.CountRootReadyResourceHeadHistoryStorageTx)
}

func ReadRootReadyResourceHeadStorageForTest(ctx context.Context, selected any) (RootReadyResourceHeadStorage, error) {
	return readServedReadRole(ctx, selected, storedurabledata.ReadRootReadyResourceHeadStorageTx)
}

func ReadResourceHeadVersionStorageForTest(ctx context.Context, selected any, ref durabledata.DeclarationRef) (string, error) {
	if err := ref.Validate(); err != nil {
		return "", err
	}
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return storedurabledata.ReadResourceHeadVersionStorageTx(ctx, tx, ref)
	})
}

func CountResourceSourceInvocationStorageForTest(ctx context.Context, selected any, sourceID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, sourceID, storedurabledata.CountResourceSourceInvocationStorageTx)
}

func CountGlobalResourceSourceInvocationsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readSelectedPreflightGlobalCount(ctx, selected, storedurabledata.CountGlobalResourceSourceInvocationsStorageTx)
}

func CountGlobalDataCreationOperationsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readSelectedPreflightGlobalCount(ctx, selected, storedurabledata.CountGlobalDataCreationOperationsStorageTx)
}

func CountGlobalResourceVersionsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readSelectedPreflightGlobalCount(ctx, selected, storedurabledata.CountGlobalResourceVersionsStorageTx)
}

func CountGlobalResourcePinsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readSelectedPreflightGlobalCount(ctx, selected, storedurabledata.CountGlobalResourcePinsStorageTx)
}
