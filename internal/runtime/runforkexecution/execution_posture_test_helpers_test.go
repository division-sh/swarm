package runforkexecution

import (
	"context"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

func executeLiveSelectedContractRunFork(ctx context.Context, req SelectedContractExecutionRequest) (SelectedContractExecutionResult, error) {
	if !req.AgentRuntime.ExecutionPosture.Valid() {
		req.AgentRuntime.ExecutionPosture = executionposture.Live
	}
	return ExecuteSelectedContractRunFork(ctx, req)
}

func activateLiveSelectedContractRunFork(ctx context.Context, req SelectedContractActivationGateRequest) (SelectedContractActivationGateResult, error) {
	if !req.AgentRuntime.ExecutionPosture.Valid() {
		req.AgentRuntime.ExecutionPosture = executionposture.Live
	}
	return ActivateSelectedContractRunFork(ctx, req)
}

func liveTestEffectController(store runtimeeffects.Store) *runtimeeffects.Controller {
	return runtimeeffects.NewController(store).WithExecutionPosture(executionposture.Live)
}

func liveTestCompletionController(store runtimeeffects.Store, completion runtimeeffects.CompletionStore, heartbeat runtimeeffects.CompletionHeartbeatStore, projector runtimeeffects.CompletionSpendProjector) *runtimeeffects.Controller {
	return runtimeeffects.NewCompletionController(store, completion, heartbeat, projector).WithExecutionPosture(executionposture.Live)
}
