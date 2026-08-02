package pipeline

import (
	"context"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func testWorkflowInstanceRoute(instancePath string) runtimeflowidentity.Route {
	return runtimeflowidentity.RouteForInstancePath(instancePath)
}

func mustCurrentWorkflowState(t *testing.T, pc *PipelineCoordinator, ctx context.Context, entityID string) WorkflowState {
	t.Helper()
	route := testWorkflowInstanceRoute(entityID)
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok && inbound.FlowInstance() != "" {
		route = testWorkflowInstanceRoute(inbound.FlowInstance())
	}
	state, err := pc.currentWorkflowState(ctx, route, entityID)
	if err != nil {
		t.Fatalf("load current workflow state: %v", err)
	}
	return state
}
