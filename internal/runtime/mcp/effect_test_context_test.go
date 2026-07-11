package mcp

import (
	"context"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func unmanagedMCPTestContext() context.Context {
	return runtimeeffects.WithDifferentOwner(context.Background(), runtimeeffects.OwnerBuildTestInfrastructure)
}
