package pipeline

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func (pc *PipelineCoordinator) executeNodeHandlerPlan(ctx context.Context, node identity.ExecutableNode, evt events.Event) bool {
	handled, _ := pc.executeNodeHandlerPlanResult(ctx, node, evt)
	return handled
}

func (pc *PipelineCoordinator) executeNodeHandlerPlanResult(ctx context.Context, node identity.ExecutableNode, evt events.Event) (bool, error) {
	handled, _, err := pc.executeNodeHandlerPlanResultWithEmissionPlan(ctx, node, evt, nil)
	return handled, err
}

func (pc *PipelineCoordinator) dispatchWorkflowNodeEventResult(ctx context.Context, evt events.Event) (bool, error) {
	handled, _, err := pc.dispatchWorkflowNodeEventResultWithEmissionPlan(ctx, evt, nil)
	return handled, err
}
