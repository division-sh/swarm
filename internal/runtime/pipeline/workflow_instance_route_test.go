package pipeline

import (
	"context"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func testWorkflowInstanceRoute(instancePath string) runtimeflowidentity.Route {
	return runtimeflowidentity.RouteForInstancePath(instancePath)
}

func mustCurrentWorkflowState(t *testing.T, pc *PipelineCoordinator, ctx context.Context, route runtimeflowidentity.Route, entityID string) WorkflowState {
	t.Helper()
	state, err := pc.currentWorkflowState(ctx, route, entityID)
	if err != nil {
		t.Fatalf("load current workflow state: %v", err)
	}
	return state
}
