package manager

import (
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

func liveTestEffectController(store runtimeeffects.Store) *runtimeeffects.Controller {
	return runtimeeffects.NewController(store).WithExecutionPosture(executionposture.Live)
}
