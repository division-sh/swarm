package serveapp

import (
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

func liveTestCompletionController(store runtimeeffects.Store, completion runtimeeffects.CompletionStore, heartbeat runtimeeffects.CompletionHeartbeatStore, projector runtimeeffects.CompletionSpendProjector) *runtimeeffects.Controller {
	return runtimeeffects.NewCompletionController(store, completion, heartbeat, projector).WithExecutionPosture(executionposture.Live)
}
