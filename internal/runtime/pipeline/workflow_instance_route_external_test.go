package pipeline_test

import (
	"context"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func testWorkflowInstanceRoute(instancePath string) runtimeflowidentity.Route {
	return runtimeflowidentity.RouteForInstancePath(instancePath)
}

func testRunScopedWorkflowInstance(instancePath string) runtimeflowidentity.RunScopedFlowInstance {
	return testRunScopedWorkflowInstanceForRun("77777777-7777-7777-7777-777777777777", instancePath)
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
