package pipeline_test

import runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"

func testWorkflowInstanceRoute(instancePath string) runtimeflowidentity.Route {
	return runtimeflowidentity.RouteForInstancePath(instancePath)
}
