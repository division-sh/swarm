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
		before := rt.ResolveForRun(busInternalTestRunID, "workers/alpha/data")
		incremental := rt.routes
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

func TestRouteRetirementInvalidatesResolutionCache(t *testing.T) {
	rt := newRouteTable(nil)
	rt.templates["workers"] = routeFlowTemplate{
		FlowID: "workers", LocalEvents: map[string]struct{}{"data": {}},
	}
	rt.patterns = []routePattern{
		{EventPattern: "workers/*/data", Subscriber: Subscriber{Path: "wild"}},
		{EventPattern: "workers/alpha/data", Subscriber: Subscriber{Path: "exact"}},
	}
	rt.templateObservers["workers"] = []routeTemplateSourceObserver{{
		SourceTemplatePath: "workers", SourceLocalEvent: "data", Subscriber: Subscriber{Path: "observer"},
	}}
	rt.rebuildLocked()
	add := func(runID, instance string) runtimeflowidentity.RunScopedFlowInstance {
		t.Helper()
		identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, runtimeflowidentity.DeriveRoute("workers", instance))
		if err != nil {
			t.Fatal(err)
		}
		if err := rt.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
			t.Fatal(err)
		}
		return identity
	}
	alpha := add(busInternalTestRunID, "alpha")
	beta := add(busInternalTestRunID, "beta")
	foreign := add("foreign-run", "alpha")
	for _, retired := range []runtimeflowidentity.RunScopedFlowInstance{alpha, beta} {
		if err := rt.RemoveFlowInstanceRoute(retired); err != nil {
			t.Fatal(err)
		}
		if rt.HasFlowInstanceRoute(retired) || !rt.HasFlowInstanceRoute(foreign) || !rt.resolutionIndexDirty {
			t.Fatal("retirement must remove its exact owner and invalidate, not rebuild, the derived cache")
		}
		observed := map[routeResolutionKey][]Subscriber{}
		for _, runID := range []string{busInternalTestRunID, "foreign-run"} {
			for _, event := range []string{"workers/alpha/data", "workers/beta/data", "workers/absent/data"} {
				observed[routeResolutionKey{runID: runID, eventType: event}] = rt.ResolveForRun(runID, event)
			}
		}
		if rt.resolutionIndexDirty {
			t.Fatal("lookup did not rebuild its invalidated cache")
		}
		rt.rebuildLocked()
		for key, want := range observed {
			if got := rt.ResolveForRun(key.runID, key.eventType); !reflect.DeepEqual(got, want) {
				t.Fatalf("retirement lookup %v differs from full rebuild: got=%+v want=%+v", key, got, want)
			}
		}
	}
	add(busInternalTestRunID, "alpha")
	if len(rt.ResolveForRun(busInternalTestRunID, "workers/alpha/data")) == 0 {
		t.Fatal("successor route was not resolved after retirement")
	}
}
