package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
)

type ScenarioPhysicalProfile = scenarioexecutionpersistence.PhysicalProfile

func ReadScenarioPhysicalProfileForTest(ctx context.Context, selected any) (ScenarioPhysicalProfile, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, scenarioexecutionpersistence.ReadPhysicalProfileTx)
}

func CountScenarioPhysicalProfilesForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, scenarioexecutionpersistence.CountPhysicalProfilesTx)
}
