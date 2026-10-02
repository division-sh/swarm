package pipeline_test

import (
	"context"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func testWorkflowInstanceRoute(instancePath string) runtimeflowidentity.Route {
	return runtimeflowidentity.RouteForInstancePath(instancePath)
}

func testRunScopedWorkflowInstanceForRun(runID, instancePath string) runtimeflowidentity.RunScopedFlowInstance {
	route := testWorkflowInstanceRoute(instancePath)
	if instancePath == runID {
		route = runtimeflowidentity.StoredRoute(".", runID, runID)
	}
	identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, route)
	if err != nil {
		panic(err)
	}
	return identity
}

func testRunScopedWorkflowInstanceFromContext(ctx context.Context, instancePath string) runtimeflowidentity.RunScopedFlowInstance {
	return testRunScopedWorkflowInstanceForRun(runtimecorrelation.RunIDFromContext(ctx), instancePath)
}
