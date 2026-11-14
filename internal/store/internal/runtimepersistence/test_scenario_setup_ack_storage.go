package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type ScenarioSetupAckStorage = pipelinepersistence.ScenarioSetupAckStorage

func ReadScenarioSetupAckStorageForTest(ctx context.Context, selected any, runID, entityID string) (ScenarioSetupAckStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ScenarioSetupAckStorage{}, err
	}
	for _, id := range []string{runID, entityID} {
		if err := validateSelectedForkStorageIdentity(id); err != nil {
			return ScenarioSetupAckStorage{}, err
		}
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReadScenarioSetupAckStorageForTest(ctx, runID, entityID)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReadScenarioSetupAckStorageForTest(ctx, runID, entityID)
	default:
		return ScenarioSetupAckStorage{}, fmt.Errorf("scenario setup acknowledgment requires the original selected owner")
	}
}
