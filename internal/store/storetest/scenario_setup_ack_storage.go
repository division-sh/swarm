package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ScenarioSetupAckStorage = private.ScenarioSetupAckStorage

func ReadScenarioSetupAckStorage(ctx context.Context, selected any, runID, entityID string) (ScenarioSetupAckStorage, error) {
	return private.ReadScenarioSetupAckStorageForTest(ctx, selected, runID, entityID)
}
