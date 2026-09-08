package pipeline

import (
	"context"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func testWorkflowInstanceRoute(instancePath string) runtimeflowidentity.Route {
	return runtimeflowidentity.RouteForInstancePath(instancePath)
}

func testRunScopedWorkflowInstance(instancePath string) runtimeflowidentity.RunScopedFlowInstance {
	return testRunScopedWorkflowInstanceForRun(testPipelineRunID, instancePath)
}

func testRunScopedWorkflowInstanceForRun(runID, instancePath string) runtimeflowidentity.RunScopedFlowInstance {
	identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, testWorkflowInstanceRoute(instancePath))
	if err != nil {
		panic(err)
	}
	return identity
}

func testRunScopedWorkflowInstanceFromContext(ctx context.Context, instancePath string) runtimeflowidentity.RunScopedFlowInstance {
	return testRunScopedWorkflowInstanceForRun(runtimecorrelation.RunIDFromContext(ctx), instancePath)
}

func testRunScopedWorkflowRoute(ctx context.Context, route runtimeflowidentity.Route) runtimeflowidentity.RunScopedFlowInstance {
	identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runtimecorrelation.RunIDFromContext(ctx), route)
	if err != nil {
		panic(err)
	}
	return identity
}

func mustCurrentWorkflowState(t *testing.T, pc *PipelineCoordinator, ctx context.Context, route runtimeflowidentity.Route, entityID string) WorkflowState {
	t.Helper()
	state, err := pc.currentWorkflowState(ctx, testRunScopedWorkflowRoute(ctx, route), identity.NormalizeEntityID(entityID))
	if err != nil {
		t.Fatalf("load current workflow state: %v", err)
	}
	return state
}
