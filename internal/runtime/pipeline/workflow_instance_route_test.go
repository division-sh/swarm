package pipeline

import (
	"context"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func testWorkflowInstanceRoute(instancePath string) runtimeflowidentity.Route {
	return runtimeflowidentity.RouteForInstancePath(instancePath)
}

func mustCurrentWorkflowState(t *testing.T, pc *PipelineCoordinator, ctx context.Context, route runtimeflowidentity.Route, entityID string) WorkflowState {
	t.Helper()
	state, err := pc.currentWorkflowState(ctx, route, identity.NormalizeEntityID(entityID))
	if err != nil {
		t.Fatalf("load current workflow state: %v", err)
	}
	return state
}
