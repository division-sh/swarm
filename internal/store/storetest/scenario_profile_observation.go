package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ScenarioPhysicalProfile = private.ScenarioPhysicalProfile

func ReadScenarioPhysicalProfile(ctx context.Context, selected any) (ScenarioPhysicalProfile, error) {
	return private.ReadScenarioPhysicalProfileForTest(ctx, selected)
}

func CountScenarioPhysicalProfiles(ctx context.Context, selected any) (int, error) {
	return private.CountScenarioPhysicalProfilesForTest(ctx, selected)
}
