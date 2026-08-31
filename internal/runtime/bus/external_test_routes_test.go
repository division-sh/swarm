package bus_test

import runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"

func testRunScopedFlowRoute(route runtimeflowidentity.Route) runtimeflowidentity.RunScopedFlowInstance {
	return testRunScopedFlowRouteForRun(eventBusTestRunID, route)
}

func testRunScopedFlowRouteForRun(runID string, route runtimeflowidentity.Route) runtimeflowidentity.RunScopedFlowInstance {
	identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, route)
	if err != nil {
		panic(err)
	}
	return identity
}

func testUncheckedRunScopedFlowRoute(route runtimeflowidentity.Route) runtimeflowidentity.RunScopedFlowInstance {
	return runtimeflowidentity.RunScopedFlowInstance{RunID: eventBusTestRunID, Route: route}
}
