package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountRunMutationStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunMutationStorageForTest(ctx, selected, runID)
}

func CountRunEventMutationStorage(ctx context.Context, selected any, runID, eventID string) (int, error) {
	return private.CountRunEventMutationStorageForTest(ctx, selected, runID, eventID)
}

type ScenarioSetupRunStorage = private.ScenarioSetupRunStorage
type PostgresScenarioSetupEntityStorage = private.PostgresScenarioSetupEntityStorage

func ReadPostgresScenarioSetupRunStorage(ctx context.Context, selected any, runID string) (ScenarioSetupRunStorage, error) {
	return private.ReadPostgresScenarioSetupRunStorageForTest(ctx, selected, runID)
}

func ReadPostgresScenarioSetupEntityStorage(ctx context.Context, selected any, runID, entityID string) (PostgresScenarioSetupEntityStorage, error) {
	return private.ReadPostgresScenarioSetupEntityStorageForTest(ctx, selected, runID, entityID)
}

func CountPostgresScenarioSetupMutations(ctx context.Context, selected any, runID, entityID string) (int, error) {
	return private.CountPostgresScenarioSetupMutationsForTest(ctx, selected, runID, entityID)
}

func ReadPostgresScenarioSetupFieldsStorage(ctx context.Context, selected any, runID, entityID string) ([]byte, error) {
	return private.ReadPostgresScenarioSetupFieldsStorageForTest(ctx, selected, runID, entityID)
}

func CountPhysicalEntities(ctx context.Context, selected any) (int, error) {
	return private.CountPhysicalEntitiesForTest(ctx, selected)
}

func RemoveScenarioSetupReplayReceipts(ctx context.Context, selected any) error {
	return private.RemoveScenarioSetupReplayReceiptsForTest(ctx, selected)
}
