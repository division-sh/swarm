package runtimepersistence

import (
	"context"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func (*selectedRouteRecoveryPostgresBus) SweepPipelineObligations(context.Context, int) (runtimepipelineobligation.SweepResult, error) {
	return runtimepipelineobligation.SweepResult{Exhausted: true}, nil
}

func (*selectedRouteRecoveryPostgresBus) PipelineWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, nil
}

func (*sqliteFlowActivationBus) SweepPipelineObligations(context.Context, int) (runtimepipelineobligation.SweepResult, error) {
	return runtimepipelineobligation.SweepResult{Exhausted: true}, nil
}

func (*sqliteFlowActivationBus) PipelineWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, nil
}
