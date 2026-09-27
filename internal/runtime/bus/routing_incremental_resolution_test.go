package bus

import (
	"reflect"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func TestRouteIncrementalResolutionMatchesFullRebuild(t *testing.T) {
	rt := newRouteTable(nil)
	rt.templates["workers"] = routeFlowTemplate{
		FlowID: "workers", LocalEvents: map[string]struct{}{"data": {}},
	}
	rt.patterns = []routePattern{
		{EventPattern: "workers/*/data", Subscriber: Subscriber{Path: "wild"}},
		{EventPattern: "workers/alpha/data", Subscriber: Subscriber{Path: "exact"}},
	}
	rt.templateObservers["workers"] = []routeTemplateSourceObserver{{
		SourceTemplatePath: "workers", SourceLocalEvent: "data",
		Subscriber: Subscriber{Path: "observer"},
	}}
	rt.rebuildLocked()
	check := func(label string) {
		t.Helper()
		incremental := rt.routes
		before := rt.ResolveForRun(busInternalTestRunID, "workers/alpha/data")
		rt.rebuildLocked()
		if !reflect.DeepEqual(incremental, rt.routes) {
			t.Fatalf("%s: incremental routes differ from complete rebuild: got=%+v want=%+v", label, incremental, rt.routes)
		}
		if after := rt.ResolveForRun(busInternalTestRunID, "workers/alpha/data"); !reflect.DeepEqual(before, after) {
			t.Fatalf("%s: lookup differs from complete rebuild: got=%+v want=%+v", label, before, after)
		}
	}
	add := func(runID, instance string) runtimeflowidentity.RunScopedFlowInstance {
		t.Helper()
		identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, runtimeflowidentity.DeriveRoute("workers", instance))
		if err != nil {
			t.Fatal(err)
		}
		if err := rt.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
			t.Fatal(err)
		}
		check(runID + "/" + instance)
		return identity
	}
	alpha := add(busInternalTestRunID, "alpha")
	add("foreign-run", "alpha")
	add(busInternalTestRunID, "beta")
	if err := rt.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: alpha}); err != nil {
		t.Fatal(err)
	}
	check("replay")
	if err := rt.RemoveFlowInstanceRoute(alpha); err != nil {
		t.Fatal(err)
	}
	check("removal")
	staged, err := runtimeflowidentity.NewRunScopedFlowInstance(busInternalTestRunID, runtimeflowidentity.DeriveRoute("workers", "gamma"))
	if err != nil {
		t.Fatal(err)
	}
	if added, err := rt.addFlowInstanceRouteForTopology(FlowInstanceRouteMaterializationRequest{Identity: staged}, nil); err != nil || !added {
		t.Fatalf("staged addition: added=%t err=%v", added, err)
	}
	rt.rebuildStagedFlowInstanceRoutes()
	check("staged")
	add(busInternalTestRunID, "delta")
}
