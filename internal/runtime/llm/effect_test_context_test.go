package llm

import (
	"context"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func unmanagedLLMTestContext() context.Context {
	return runtimeeffects.WithDifferentOwner(context.Background(), runtimeeffects.OwnerBuildTestInfrastructure)
}
