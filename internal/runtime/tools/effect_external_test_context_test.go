package tools_test

import (
	"context"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func unmanagedToolTestContext() context.Context {
	return runtimeeffects.WithDifferentOwner(context.Background(), runtimeeffects.OwnerBuildTestInfrastructure)
}
