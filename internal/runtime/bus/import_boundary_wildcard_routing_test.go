package bus_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	swruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestImportBoundaryWildcardScopesImportedPackageToOwnSubtreeByDefault(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{})
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if routes := rt.Resolve("worker/task.done"); len(routes) != 1 || routes[0].ID != "worker-listener" {
		t.Fatalf("Resolve(worker/task.done) = %#v, want worker-listener", routes)
	}
	if routes := rt.Resolve("producer/task.done"); len(routes) != 0 {
		t.Fatalf("Resolve(producer/task.done) = %#v, want no sibling route without grant", routes)
	}
	if owners := source.RuntimeEventOwners("producer/task.done"); len(owners) != 0 {
		t.Fatalf("RuntimeEventOwners(producer/task.done) = %#v, want no sibling owner without grant", owners)
	}
	if _, ok := source.NodeEventHandler("worker-listener", "producer/task.done"); ok {
		t.Fatal("NodeEventHandler(worker-listener, producer/task.done) matched ungranted sibling event")
	}

	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	local := eventtest.RootIngress("evt-worker-local", "worker/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eb.Publish(context.Background(), local); err != nil {
		t.Fatalf("Publish local: %v", err)
	}
	if got := store.deliveries["evt-worker-local"]; len(got) != 1 || got[0] != "worker-listener" {
		t.Fatalf("local persisted deliveries = %#v, want worker-listener", got)
	}
	sibling := eventtest.RootIngress("evt-producer-sibling", "producer/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eb.Publish(context.Background(), sibling); err != nil {
		t.Fatalf("Publish sibling: %v", err)
	}
	if got := store.deliveries["evt-producer-sibling"]; len(got) != 0 {
		t.Fatalf("sibling persisted deliveries = %#v, want none without grant", got)
	}
}

func TestImportBoundaryWildcardObserveGrantAddsNarrowSiblingCandidate(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ObserveGrant: "      observe:\n        - source: producer\n          events: [task.done]\n",
	})
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if routes := rt.Resolve("producer/task.done"); len(routes) != 1 || routes[0].ID != "worker-listener" || routes[0].RouteSource != "import_boundary_wildcard_grant" {
		t.Fatalf("Resolve(producer/task.done) = %#v, want worker-listener via grant", routes)
	}
	if owners := source.RuntimeEventOwners("producer/task.done"); len(owners) != 1 || owners[0] != "worker-listener" {
		t.Fatalf("RuntimeEventOwners(producer/task.done) = %#v, want worker-listener", owners)
	}
	if _, ok := source.NodeEventHandler("worker-listener", "producer/task.done"); !ok {
		t.Fatal("NodeEventHandler(worker-listener, producer/task.done) did not resolve through grant")
	}

	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress("evt-granted-sibling", "producer/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "worker-listener" {
		t.Fatalf("routed recipients = %#v, want worker-listener", plan.RoutedRecipients)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish granted sibling: %v", err)
	}
	if got := store.deliveries["evt-granted-sibling"]; len(got) != 1 || got[0] != "worker-listener" {
		t.Fatalf("persisted deliveries = %#v, want worker-listener", got)
	}

}

func TestImportBoundaryWildcardBoundedGrantDeliversAcrossSurfaces(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ProducerAuthored:   true,
		ObserveGrant:       "      observe:\n        - source: producer\n          events: [task.*]\n",
		ProducerExtraEvent: "task.failed",
	})
	assertImportBoundaryWildcardAuthorizationDeliversAcrossSurfaces(t, source, "producer/task.*", "**/task.done")

	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := routes.Resolve("producer/task.failed"); len(got) != 0 {
		t.Fatalf("Resolve(producer/task.failed) = %#v, want no route outside the consumer wildcard intersection", got)
	}

	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	nonIntersecting := eventtest.RootIngress("evt-bounded-grant-non-intersection", "producer/task.failed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eb.Publish(context.Background(), nonIntersecting); err != nil {
		t.Fatalf("Publish non-intersecting bounded grant event: %v", err)
	}
	if got := store.deliveries["evt-bounded-grant-non-intersection"]; len(got) != 0 {
		t.Fatalf("non-intersecting persisted deliveries = %#v, want none", got)
	}
}

func TestImportBoundaryWildcardLocalBoundedGrantDeliversAcrossSurfaces(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ProducerAuthored:   true,
		WorkerSubscription: "task.*",
		ObserveGrant:       "      observe:\n        - source: producer\n          events: [task.*]\n",
	})
	assertImportBoundaryWildcardAuthorizationDeliversAcrossSurfaces(t, source, "producer/task.*", "task.*")
}

func TestImportBoundaryWildcardLocalExactGrantDeliversAcrossSurfaces(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ProducerAuthored:   true,
		WorkerSubscription: "task.*",
		ObserveGrant:       "      observe:\n        - source: producer\n          events: [task.done]\n",
	})
	assertImportBoundaryWildcardAuthorizationDeliversAcrossSurfaces(t, source, "producer/task.done", "task.*")
}

func TestImportBoundaryWildcardTemplateSourceGrantMaterializesAcrossSurfaces(t *testing.T) {
	opts := importBoundaryWildcardFixtureOptions{
		ProducerMode:             "template",
		ProducerAuthored:         true,
		ProducerStaticDescendant: true,
		ObserveGrant:             "      observe:\n        - source: producer\n          events: [task.*]\n",
		ProducerExtraEvent:       "task.failed",
	}
	source := loadBusImportBoundaryWildcardSource(t, opts)
	resolution := semanticview.ResolveImportBoundaryWildcardSubscription(source, "flows/worker", "worker", "worker", map[string]struct{}{"task.done": {}}, "**/task.done")
	var grantPattern *semanticview.ImportBoundaryWildcardPattern
	for i := range resolution.Patterns {
		if resolution.Patterns[i].RouteSource == "import_boundary_wildcard_grant" {
			grantPattern = &resolution.Patterns[i]
		}
	}
	if grantPattern == nil || grantPattern.EventPattern != "producer/task.done" || grantPattern.SourceTemplatePath != "producer" || grantPattern.SourceLocalEvent != "task.done" {
		t.Fatalf("template grant resolution = %#v, want static proof witness with explicit source-template provenance", resolution)
	}
	directRoutes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable direct collision proof: %v", err)
	}
	directSibling := runtimeflowidentity.DeriveRoute("producer", "inst-direct")
	if err := directRoutes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: directSibling}); err != nil {
		t.Fatalf("AddFlowInstanceRoute direct sibling: %v", err)
	}
	if err := directRoutes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: directSibling}); err != nil {
		t.Fatalf("exact direct sibling replay: %v", err)
	}
	directMismatch := runtimeflowidentity.StoredRoute("worker", "other", directSibling.InstancePath)
	if directRoutes.HasFlowInstanceRoute(directMismatch) {
		t.Fatal("direct mismatched identity was reported present")
	}
	if err := directRoutes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: directMismatch}); err == nil || !strings.Contains(err.Error(), "is owned by scope") {
		t.Fatalf("direct mismatched add error = %v, want complete-owner conflict", err)
	}
	if err := directRoutes.RemoveFlowInstanceRoute(directMismatch); err == nil || !strings.Contains(err.Error(), "is owned by scope") {
		t.Fatalf("direct mismatched remove error = %v, want complete-owner conflict", err)
	}
	directInconsistent := runtimeflowidentity.StoredRoute("worker", "unowned", "producer/unowned")
	if err := directRoutes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: directInconsistent}); err == nil || !strings.Contains(err.Error(), "identity is inconsistent") {
		t.Fatalf("direct inconsistent add error = %v, want tuple validation failure", err)
	}
	if err := directRoutes.RemoveFlowInstanceRoute(directInconsistent); err == nil || !strings.Contains(err.Error(), "identity is inconsistent") {
		t.Fatalf("direct inconsistent remove error = %v, want tuple validation failure", err)
	}
	directCollision := runtimeflowidentity.DeriveRoute("producer", "child")
	if err := directRoutes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: directCollision}); err == nil || !strings.Contains(err.Error(), "collides with authored canonical identity") {
		t.Fatalf("AddFlowInstanceRoute direct collision error = %v, want fail-closed canonical collision", err)
	}
	if directRoutes.HasFlowInstanceRoute(directCollision) {
		t.Fatal("direct colliding instance route was installed")
	}
	if got := directRoutes.Resolve("producer/child/task.done"); len(got) != 0 {
		t.Fatalf("direct static descendant route after rejected collision = %#v, want unchanged authority", got)
	}
	if got := directRoutes.Resolve("producer/inst-direct/task.done"); len(got) != 1 || got[0].ID != "worker-listener" {
		t.Fatalf("direct sibling route after rejected collision = %#v, want existing route unchanged", got)
	}

	for _, tc := range []struct {
		name     string
		prebuilt bool
	}{
		{name: "default route admission"},
		{name: "supplied route admission", prebuilt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadBusImportBoundaryWildcardSource(t, opts)
			var prebuilt *runtimebus.RouteTable
			if tc.prebuilt {
				var err error
				prebuilt, err = runtimebus.DeriveRouteTable(source)
				if err != nil {
					t.Fatalf("DeriveRouteTable: %v", err)
				}
			}
			store := &routePersistenceTestStore{}
			eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source, RouteTable: prebuilt})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			if got := eb.RouteTable().Resolve("producer/inst-1/task.done"); len(got) != 0 {
				t.Fatalf("Resolve before materialization = %#v, want no template-instance route", got)
			}
			if got := eb.RouteTable().Resolve("producer/task.done"); len(got) != 0 {
				t.Fatalf("Resolve template base event = %#v, want no static template route", got)
			}
			if got := eb.RouteTable().Resolve("producer/child/task.done"); len(got) != 0 {
				t.Fatalf("Resolve static descendant before materialization = %#v, want no path-shaped template authority", got)
			}
			identity := runtimeflowidentity.StoredRoute("producer", "inst-1", "producer/inst-1")
			if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
				t.Fatalf("AddFlowInstanceRoute: %v", err)
			}
			if got := eb.RouteTable().Resolve("producer/inst-1/task.done"); len(got) != 1 || got[0].ID != "worker-listener" || got[0].RouteSource != "import_boundary_wildcard_grant" {
				t.Fatalf("Resolve materialized template event = %#v, want grant-backed worker route", got)
			}
			if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
				t.Fatalf("exact EventBus identity replay: %v", err)
			}
			mismatchedIdentity := runtimeflowidentity.StoredRoute("worker", "other", identity.InstancePath)
			if eb.RouteTable().HasFlowInstanceRoute(mismatchedIdentity) {
				t.Fatal("EventBus mismatched identity was reported present")
			}
			if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: mismatchedIdentity}); err == nil || !strings.Contains(err.Error(), "is owned by scope") {
				t.Fatalf("EventBus mismatched add error = %v, want complete-owner conflict", err)
			}
			deleteCalls := len(store.deleteCalls)
			if err := eb.RemoveFlowInstanceRoute(mismatchedIdentity); err == nil || !strings.Contains(err.Error(), "is owned by scope") {
				t.Fatalf("EventBus mismatched remove error = %v, want complete-owner conflict", err)
			}
			if len(store.deleteCalls) != deleteCalls {
				t.Fatalf("persistence delete calls = %#v, rejected removal must not reach persistence", store.deleteCalls)
			}
			if got := eb.RouteTable().Resolve("producer/inst-1/task.done"); len(got) != 1 || got[0].ID != "worker-listener" {
				t.Fatalf("route after mismatched identity operations = %#v, want installed owner unchanged", got)
			}
			if got := eb.RouteTable().Resolve("producer/inst-1/task.failed"); len(got) != 0 {
				t.Fatalf("Resolve non-intersecting template event = %#v, want no route", got)
			}
			if got := eb.RouteTable().Resolve("producer/inst-2/task.done"); len(got) != 0 {
				t.Fatalf("Resolve unmaterialized sibling instance = %#v, want no route", got)
			}
			if got := eb.RouteTable().Resolve("producer/inst-1/nested/task.done"); len(got) != 0 {
				t.Fatalf("Resolve deeper nested event = %#v, want one-instance-segment grant scope", got)
			}
			if got := eb.RouteTable().Resolve("producer/child/task.done"); len(got) != 0 {
				t.Fatalf("Resolve static descendant after materialization = %#v, want lifecycle provenance isolation", got)
			}
			collisionIdentity := runtimeflowidentity.DeriveRoute("producer", "child")
			if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: collisionIdentity}); err == nil || !strings.Contains(err.Error(), "collides with authored canonical identity") {
				t.Fatalf("AddFlowInstanceRoute canonical collision error = %v, want fail closed", err)
			}
			if eb.RouteTable().HasFlowInstanceRoute(collisionIdentity) {
				t.Fatal("colliding EventBus instance route was installed")
			}
			staticEvent := eventtest.RootIngress("evt-static-descendant-"+strings.ReplaceAll(tc.name, " ", "-"), "producer/child/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			staticPlan, err := eb.CheckPublishRecipientPlan(context.Background(), staticEvent)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan static descendant: %v", err)
			}
			for _, recipient := range staticPlan.RoutedRecipients {
				if recipient.ID == "worker-listener" {
					t.Fatalf("static descendant recipient plan = %#v, rejected collision must not add observer authority", staticPlan)
				}
			}
			if err := eb.Publish(context.Background(), staticEvent); err != nil {
				t.Fatalf("Publish static descendant: %v", err)
			}
			if got := store.deliveries[staticEvent.ID()]; len(got) != 0 {
				t.Fatalf("static descendant persisted deliveries = %#v, want none after rejected collision", got)
			}
			if got := eb.RouteTable().Resolve("producer/inst-1/task.done"); len(got) != 1 || got[0].ID != "worker-listener" {
				t.Fatalf("existing sibling route after rejected collision = %#v, want unchanged authority", got)
			}

			evt := eventtest.RootIngress("evt-template-grant-"+strings.ReplaceAll(tc.name, " ", "-"), "producer/inst-1/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "worker-listener" || len(plan.DeliveryRoutes) != 1 {
				t.Fatalf("publish recipient plan = %#v, want one grant-backed worker delivery", plan)
			}
			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if got := store.deliveries[evt.ID()]; len(got) != 1 || got[0] != "worker-listener" {
				t.Fatalf("persisted deliveries = %#v, want worker-listener", got)
			}
			secondIdentity := runtimeflowidentity.DeriveRoute("producer", "inst-2")
			if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: secondIdentity}); err != nil {
				t.Fatalf("AddFlowInstanceRoute(inst-2): %v", err)
			}
			if got := eb.RouteTable().Resolve("producer/inst-2/task.done"); len(got) != 1 || got[0].ID != "worker-listener" {
				t.Fatalf("Resolve second materialized instance = %#v, want grant-backed worker route", got)
			}
			if err := eb.RemoveFlowInstanceRoute(identity); err != nil {
				t.Fatalf("RemoveFlowInstanceRoute: %v", err)
			}
			if got := eb.RouteTable().Resolve("producer/inst-1/task.done"); len(got) != 0 {
				t.Fatalf("Resolve after route removal = %#v, want no template-instance route", got)
			}
			if got := eb.RouteTable().Resolve("producer/inst-2/task.done"); len(got) != 1 || got[0].ID != "worker-listener" {
				t.Fatalf("Resolve surviving sibling instance = %#v, want independent grant-backed route", got)
			}
			if got := eb.RouteTable().Resolve("producer/child/task.done"); len(got) != 0 {
				t.Fatalf("Resolve static descendant after removal = %#v, want no grant route", got)
			}
			if err := eb.RemoveFlowInstanceRoute(secondIdentity); err != nil {
				t.Fatalf("RemoveFlowInstanceRoute(inst-2): %v", err)
			}
			if got := eb.RouteTable().Resolve("producer/inst-2/task.done"); len(got) != 0 {
				t.Fatalf("Resolve after second route removal = %#v, want no template-instance route", got)
			}
		})
	}
}

func TestImportBoundaryWildcardTemplateSourceAndConsumerLifecycleOrdersRemainExact(t *testing.T) {
	opts := importBoundaryWildcardFixtureOptions{
		WorkerMode:               "template",
		ProducerMode:             "template",
		ProducerStaticDescendant: true,
		ObserveGrant:             "      observe:\n        - source: producer\n          events: [task.done]\n",
	}
	for _, sourceFirst := range []bool{true, false} {
		name := "consumer first"
		if sourceFirst {
			name = "source first"
		}
		t.Run(name, func(t *testing.T) {
			source := loadBusImportBoundaryWildcardSource(t, opts)
			routes, err := runtimebus.DeriveRouteTable(source)
			if err != nil {
				t.Fatalf("DeriveRouteTable: %v", err)
			}
			sourceIdentity := runtimeflowidentity.DeriveRoute("producer", "source-1")
			consumerIdentity := runtimeflowidentity.DeriveRoute("worker", "consumer-1")
			first := sourceIdentity
			second := consumerIdentity
			if !sourceFirst {
				first, second = second, first
			}
			if err := routes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: first}); err != nil {
				t.Fatalf("AddFlowInstanceRoute(first): %v", err)
			}
			if got := routes.Resolve("producer/source-1/task.done"); len(got) != 0 {
				t.Fatalf("Resolve before both lifecycles exist = %#v, want no route", got)
			}
			if err := routes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: second}); err != nil {
				t.Fatalf("AddFlowInstanceRoute(second): %v", err)
			}
			if got := routes.Resolve("producer/source-1/task.done"); len(got) != 1 || got[0].ID != "worker-listener" || got[0].Path != "worker/consumer-1" {
				t.Fatalf("Resolve after both lifecycles exist = %#v, want exact template-consumer route", got)
			}
			if got := routes.Resolve("producer/child/task.done"); len(got) != 0 {
				t.Fatalf("Resolve authored static descendant = %#v, want no path-shaped authority", got)
			}
			if err := routes.RemoveFlowInstanceRoute(consumerIdentity); err != nil {
				t.Fatalf("RemoveFlowInstanceRoute(consumer): %v", err)
			}
			if got := routes.Resolve("producer/source-1/task.done"); len(got) != 0 {
				t.Fatalf("Resolve after consumer removal = %#v, want no stale observer route", got)
			}
			if err := routes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: consumerIdentity}); err != nil {
				t.Fatalf("re-add consumer route: %v", err)
			}
			if got := routes.Resolve("producer/source-1/task.done"); len(got) != 1 || got[0].Path != "worker/consumer-1" {
				t.Fatalf("Resolve after consumer rematerialization = %#v, want restored exact route", got)
			}
			if err := routes.RemoveFlowInstanceRoute(sourceIdentity); err != nil {
				t.Fatalf("RemoveFlowInstanceRoute(source): %v", err)
			}
			if got := routes.Resolve("producer/source-1/task.done"); len(got) != 0 {
				t.Fatalf("Resolve after source removal = %#v, want no stale source route", got)
			}
		})
	}
}

func assertImportBoundaryWildcardAuthorizationDeliversAcrossSurfaces(t *testing.T, source semanticview.Source, eventPattern, matchPattern string) {
	t.Helper()
	if issues := semanticview.ImportBoundaryWildcardGrantIssues(source); len(issues) != 0 {
		t.Fatalf("observe grant issues = %#v, want grant accepted", issues)
	}
	relations := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveTypedPubSubRelations()
	if len(relations.Issues) != 0 || len(relations.Matches) != 1 {
		t.Fatalf("typed pub/sub relations = %#v, want one grant-backed relation", relations)
	}
	match := relations.Matches[0]
	if match.Authorization == nil || match.Authorization.EventPattern != eventPattern || match.Authorization.MatchPattern != matchPattern {
		t.Fatalf("typed pub/sub authorization = %#v, want event pattern %q and match pattern %q", match.Authorization, eventPattern, matchPattern)
	}
	topology := routingtopology.Build(source)
	if len(topology.Issues) != 0 || !importBoundaryWildcardTopologyContainsAuthorization(topology, eventPattern) {
		t.Fatalf("routing topology = %#v, want grant-backed typed pub/sub edge", topology)
	}
	report := bootverify.Run(context.Background(), source, bootverify.Options{})
	if importBoundaryWildcardReportContains(report.Errors(), "flow_package_wildcard_observe_grant", "") || importBoundaryWildcardReportContains(report.Errors(), semanticview.TypedPubSubFailureAuthorizationAmbiguous, "") {
		t.Fatalf("verify errors = %#v, want grant admitted", report.Errors())
	}
	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := routes.Resolve("producer/task.done"); len(got) != 1 || got[0].ID != "worker-listener" || got[0].RouteSource != "import_boundary_wildcard_grant" {
		t.Fatalf("Resolve(producer/task.done) = %#v, want one grant-backed worker route", got)
	}
	if owners := source.RuntimeEventOwners("producer/task.done"); len(owners) != 1 || owners[0] != "worker-listener" {
		t.Fatalf("RuntimeEventOwners(producer/task.done) = %#v, want grant-backed worker owner", owners)
	}
	if _, ok := source.NodeEventHandler("worker-listener", "producer/task.done"); !ok {
		t.Fatal("NodeEventHandler(worker-listener, producer/task.done) did not resolve through grant")
	}
	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress("evt-grant-delivery", "producer/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "worker-listener" || len(plan.DeliveryRoutes) != 1 {
		t.Fatalf("publish recipient plan = %#v, want one routed worker delivery", plan)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish grant-backed event: %v", err)
	}
	if got := store.deliveries["evt-grant-delivery"]; len(got) != 1 || got[0] != "worker-listener" {
		t.Fatalf("grant-backed persisted deliveries = %#v, want worker-listener", got)
	}
}

func TestImportBoundaryWildcardAuthorizationAmbiguityFailsClosedAcrossSurfaces(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ObserveGrant: "      observe:\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n" +
			"        - source: flows/producer\n" +
			"          events: [task.done]\n",
	})
	assertImportBoundaryWildcardAuthorizationAmbiguityFailsClosedAcrossSurfaces(t, source)
}

func TestImportBoundaryWildcardBoundedAndExactAuthorizationAmbiguityFailsClosedAcrossSurfaces(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ObserveGrant: "      observe:\n" +
			"        - source: producer\n" +
			"          events: [task.*]\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n",
	})
	issue := assertImportBoundaryWildcardAuthorizationAmbiguityFailsClosedAcrossSurfaces(t, source)
	patterns := map[string]bool{}
	for _, authorization := range issue.Authorizations {
		patterns[authorization.EventPattern] = true
	}
	if !patterns["producer/task.*"] || !patterns["producer/task.done"] || len(patterns) != 2 {
		t.Fatalf("authorization patterns = %#v, want distinct bounded and exact proofs", patterns)
	}
}

func TestImportBoundaryWildcardLocalBoundedAndExactAuthorizationAmbiguityFailsClosedAcrossSurfaces(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		WorkerSubscription: "task.*",
		ObserveGrant: "      observe:\n" +
			"        - source: producer\n" +
			"          events: [task.*]\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n",
	})
	issue := assertImportBoundaryWildcardAuthorizationAmbiguityFailsClosedAcrossSurfaces(t, source)
	patterns := map[string]bool{}
	for _, authorization := range issue.Authorizations {
		patterns[authorization.EventPattern] = true
	}
	if !patterns["producer/task.*"] || !patterns["producer/task.done"] || len(patterns) != 2 {
		t.Fatalf("authorization patterns = %#v, want distinct local bounded and exact proofs", patterns)
	}
}

func TestImportBoundaryWildcardTemplateSourceBoundedAndExactAuthorizationAmbiguityFailsClosedAcrossSurfaces(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ProducerMode: "template",
		ObserveGrant: "      observe:\n" +
			"        - source: producer\n" +
			"          events: [task.*]\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n",
	})
	issue := assertImportBoundaryWildcardAuthorizationAmbiguityFailsClosedAcrossSurfaces(t, source)
	patterns := map[string]bool{}
	for _, authorization := range issue.Authorizations {
		patterns[authorization.EventPattern] = true
	}
	if !patterns["producer/task.*"] || !patterns["producer/task.done"] || len(patterns) != 2 {
		t.Fatalf("template-source authorization patterns = %#v, want distinct bounded and exact proofs", patterns)
	}
}

func assertImportBoundaryWildcardAuthorizationAmbiguityFailsClosedAcrossSurfaces(t *testing.T, source semanticview.Source) semanticview.TypedPubSubConsumerIssue {
	t.Helper()
	relations := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveTypedPubSubRelations()
	if len(relations.Matches) != 0 || len(relations.Issues) != 1 {
		t.Fatalf("typed pub/sub relations = %#v, want one ambiguity and no edge authority", relations)
	}
	if issue := relations.Issues[0]; issue.Failure != semanticview.TypedPubSubFailureAuthorizationAmbiguous || len(issue.Authorizations) != 2 {
		t.Fatalf("typed pub/sub issue = %#v, want two-proof authorization ambiguity", issue)
	} else if issue.Producer.ID != "" || issue.Event.EventKey() != "producer/task.done" {
		t.Fatalf("typed pub/sub issue = %#v, want producerless declared-event authority", issue)
	}

	topology := routingtopology.Build(source)
	if len(topology.Edges) != 0 || len(topology.Issues) != 1 || topology.Issues[0].Failure != semanticview.TypedPubSubFailureAuthorizationAmbiguous {
		t.Fatalf("routing topology = %#v, want ambiguity issue and no edge", topology)
	}

	report := bootverify.Run(context.Background(), source, bootverify.Options{})
	if !importBoundaryWildcardReportContains(report.Errors(), semanticview.TypedPubSubFailureAuthorizationAmbiguous, "multiple distinct import authorization proofs") {
		t.Fatalf("verify errors = %#v, want typed pub/sub authorization hard invalidity", report.Errors())
	}
	if importBoundaryWildcardReportContains(report.Findings, "event_consumer_exists", "producer/task.done") {
		t.Fatalf("verify findings = %#v, ambiguity should replace the generic missing-consumer warning", report.Findings)
	}
	validationOpts := swruntime.DefaultWorkflowContractValidationOptions(nil)
	validationOpts.CheckMCPReachable = false
	validationOpts.FatalBootWarnings = false
	validationOpts.FatalToolImplementationWarning = false
	if _, err := swruntime.ValidateWorkflowContractSurface(context.Background(), source, validationOpts); err == nil || !strings.Contains(err.Error(), semanticview.TypedPubSubFailureAuthorizationAmbiguous) {
		t.Fatalf("runtime contract validation error = %v, want event.publish runtime-context admission failure", err)
	}

	routes, err := runtimebus.DeriveRouteTable(source)
	if routes != nil {
		t.Fatalf("route table = %#v, want no runtime authority for ambiguous relation", routes)
	}
	var authorizationErr *runtimebus.TypedPubSubAuthorizationError
	if !errors.As(err, &authorizationErr) || len(authorizationErr.Issues) != 1 {
		t.Fatalf("DeriveRouteTable error = %v, want typed pub/sub authorization error", err)
	}
	if eb, err := runtimebus.NewEventBusWithOptions(&routePersistenceTestStore{}, runtimebus.EventBusOptions{ContractBundle: source}); err == nil || eb != nil {
		t.Fatalf("NewEventBusWithOptions = (%#v, %v), want fail-closed startup", eb, err)
	}
	validSource := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ObserveGrant: "      observe:\n        - source: producer\n          events: [task.done]\n",
	})
	prebuiltRoutes, err := runtimebus.DeriveRouteTable(validSource)
	if err != nil {
		t.Fatalf("derive valid prebuilt route table: %v", err)
	}
	if eb, err := runtimebus.NewEventBusWithOptions(&routePersistenceTestStore{}, runtimebus.EventBusOptions{ContractBundle: source, RouteTable: prebuiltRoutes}); err == nil || eb != nil {
		t.Fatalf("NewEventBusWithOptions with prebuilt routes = (%#v, %v), want contract ambiguity to remain authoritative", eb, err)
	}
	return relations.Issues[0]
}

func TestImportBoundaryWildcardAuthorizationAmbiguityRetainsAuthoredProducerProof(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ProducerAuthored: true,
		ObserveGrant: "      observe:\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n" +
			"        - source: flows/producer\n" +
			"          events: [task.done]\n",
	})

	relations := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveTypedPubSubRelations()
	if len(relations.Issues) != 1 || strings.TrimSpace(relations.Issues[0].Producer.ID) == "" {
		t.Fatalf("typed pub/sub relations = %#v, want one ambiguity with authored producer provenance", relations)
	}
	if routes, err := runtimebus.DeriveRouteTable(source); err == nil || routes != nil {
		t.Fatalf("DeriveRouteTable = (%#v, %v), want authored-producer ambiguity to remain fail closed", routes, err)
	}
}

func TestImportBoundaryWildcardExactDuplicateAuthorizationCollapsesAcrossSurfaces(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{
		ObserveGrant: "      observe:\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n" +
			"        - source: producer\n" +
			"          events: [task.done]\n",
	})

	relations := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveTypedPubSubRelations()
	if len(relations.Issues) != 0 || len(relations.Matches) != 0 {
		t.Fatalf("typed pub/sub relations = %#v, want no conflict and no synthetic producer edge", relations)
	}
	topology := routingtopology.Build(source)
	if len(topology.Issues) != 0 || len(topology.Edges) != 0 {
		t.Fatalf("routing topology = %#v, want no conflict and no synthetic producer edge", topology)
	}
	routes, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := routes.Resolve("producer/task.done"); len(got) != 1 || got[0].ID != "worker-listener" {
		t.Fatalf("Resolve(producer/task.done) = %#v, want one deduplicated worker route", got)
	}
}

func TestImportBoundaryWildcardGrantFailsClosedWhenInvalid(t *testing.T) {
	cases := []struct {
		name        string
		grant       string
		wantMessage string
	}{
		{
			name:        "unknown source",
			grant:       "      observe:\n        - source: missing\n          events: [task.done]\n",
			wantMessage: "does not resolve",
		},
		{
			name:        "broad event",
			grant:       "      observe:\n        - source: producer\n          events: [\"**\"]\n",
			wantMessage: "unbounded wildcard",
		},
		{
			name:        "unknown event",
			grant:       "      observe:\n        - source: producer\n          events: [missing.done]\n",
			wantMessage: "does not match any event",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{ObserveGrant: tc.grant})
			report := bootverify.Run(context.Background(), source, bootverify.Options{})
			if !importBoundaryWildcardReportContains(report.Errors(), "flow_package_wildcard_observe_grant", tc.wantMessage) {
				t.Fatalf("expected flow_package_wildcard_observe_grant containing %q, got %#v", tc.wantMessage, report.Errors())
			}
		})
	}
}

func TestImportBoundaryWildcardExplicitCrossTreeSubscriptionWithoutGrantFailsClosed(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{WorkerSubscription: "producer/**/task.done"})
	report := bootverify.Run(context.Background(), source, bootverify.Options{})
	if !importBoundaryWildcardReportContains(report.Errors(), "flow_package_wildcard_observe_grant", "no package-subtree candidate") {
		t.Fatalf("expected ungranted_or_unknown_subscription, got %#v", report.Errors())
	}
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if routes := rt.Resolve("producer/task.done"); len(routes) != 0 {
		t.Fatalf("Resolve(producer/task.done) = %#v, want no route for ungranted explicit cross-tree wildcard", routes)
	}
}

func TestImportBoundaryWildcardPreservesTemplateInstanceSubtree(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{WorkerMode: "template"})
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if routes := rt.Resolve("worker/inst-1/task.done"); len(routes) != 0 {
		t.Fatalf("Resolve before materialization = %#v, want none", routes)
	}
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: runtimeflowidentity.DeriveRoute("worker", "inst-1"),
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	routes := rt.Resolve("worker/inst-1/task.done")
	if len(routes) != 1 || routes[0].ID != "worker-listener" || routes[0].Path != "worker/inst-1" {
		t.Fatalf("Resolve(worker/inst-1/task.done) = %#v, want materialized worker-listener", routes)
	}
	if sibling := rt.Resolve("producer/task.done"); len(sibling) != 0 {
		t.Fatalf("Resolve(producer/task.done) = %#v, want no sibling route for template wildcard without grant", sibling)
	}
}

func TestRootWildcardSubscriptionsRemainUnchanged(t *testing.T) {
	source := loadBusImportBoundaryWildcardSource(t, importBoundaryWildcardFixtureOptions{RootWildcard: true})
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if routes := rt.Resolve("producer/task.done"); len(routes) != 1 || routes[0].ID != "root-listener" {
		t.Fatalf("Resolve(producer/task.done) = %#v, want root-listener", routes)
	}
}

type importBoundaryWildcardFixtureOptions struct {
	ObserveGrant             string
	WorkerMode               string
	ProducerMode             string
	WorkerSubscription       string
	RootWildcard             bool
	ProducerAuthored         bool
	ProducerExtraEvent       string
	ProducerStaticDescendant bool
}

func loadBusImportBoundaryWildcardSource(t *testing.T, opts importBoundaryWildcardFixtureOptions) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeBusImportBoundaryWildcardFixture(t, opts)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeBusImportBoundaryWildcardFixture(t *testing.T, opts importBoundaryWildcardFixtureOptions) string {
	// routing-example-census: different-concept issue=none owner=flow_package.wildcard_observation_boundary proof=internal/runtime/bus/import_boundary_wildcard_routing_test.go:TestImportBoundaryWildcardScopesImportedPackageToOwnSubtreeByDefault
	t.Helper()
	root := t.TempDir()
	mode := strings.TrimSpace(opts.WorkerMode)
	if mode == "" {
		mode = "static"
	}
	producerMode := strings.TrimSpace(opts.ProducerMode)
	if producerMode == "" {
		producerMode = "static"
	}
	workerSubscription := strings.TrimSpace(opts.WorkerSubscription)
	if workerSubscription == "" {
		workerSubscription = "**/task.done"
	}
	rootNode := "{}\n"
	if opts.RootWildcard {
		rootNode = `
root-listener:
  id: root-listener
  execution_type: system_node
  subscribes_to: ["**/task.done"]
  event_handlers:
    "**/task.done": {}
`
	}
	workerBind := ""
	if strings.TrimSpace(opts.ObserveGrant) != "" {
		workerBind = "    bind:\n" + opts.ObserveGrant
	}
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: bus-import-boundary-wildcard
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: worker
    flow: worker
    mode: `+mode+`
`+workerBind+`  - id: producer
    flow: producer
    mode: `+producerMode+`
`)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: bus-import-boundary-wildcard\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "nodes.yaml"), rootNode)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), "name: worker\nversion: \"1.0.0\"\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
mode: `+mode+`
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), "task.done: {}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "worker", "nodes.yaml"), `
worker-listener:
  id: worker-listener
  execution_type: system_node
  subscribes_to: ["`+workerSubscription+`"]
  event_handlers:
    "`+workerSubscription+`": {}
`)
	producerPackage := "name: producer\nversion: \"1.0.0\"\n"
	if opts.ProducerStaticDescendant {
		producerPackage += "platform_version: \">=0.7.0 <0.8.0\"\nflows:\n  - id: child\n    flow: child\n    mode: static\n"
	}
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "package.yaml"), producerPackage)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: `+producerMode+`
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	producerEvents := "task.done: {}\n"
	if extra := strings.TrimSpace(opts.ProducerExtraEvent); extra != "" {
		producerEvents += extra + ": {}\n"
	}
	producerNodes := "{}\n"
	if opts.ProducerAuthored {
		producerEvents += "task.start: {}\n"
		producerNodes = `
producer-source:
  id: producer-source
  execution_type: system_node
  subscribes_to: [task.start]
  produces: [task.done]
  event_handlers:
    task.start:
      emit:
        event: task.done
        broadcast: true
`
	}
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), producerEvents)
	writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), producerNodes)
	if opts.ProducerStaticDescendant {
		descendant := filepath.Join(root, "flows", "producer", "flows", "child")
		writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(descendant, "package.yaml"), "name: child\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n")
		writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(descendant, "schema.yaml"), `
name: child
mode: static
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
		writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(descendant, "policy.yaml"), "{}\n")
		writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(descendant, "agents.yaml"), "{}\n")
		writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(descendant, "events.yaml"), "task.done: {}\n")
		writeBusImportBoundaryWildcardFixtureFile(t, filepath.Join(descendant, "nodes.yaml"), "{}\n")
	}
	return root
}

func writeBusImportBoundaryWildcardFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir fixture dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}

func importBoundaryWildcardReportContains(findings []bootverify.Finding, checkID, substr string) bool {
	for _, finding := range findings {
		if strings.TrimSpace(finding.CheckID) != checkID {
			continue
		}
		if substr == "" || strings.Contains(finding.Message, substr) {
			return true
		}
	}
	return false
}

func importBoundaryWildcardTopologyContainsAuthorization(topology routingtopology.Topology, eventPattern string) bool {
	for _, edge := range topology.Edges {
		if edge.TypedPubSub != nil && edge.TypedPubSub.Authorization != nil && edge.TypedPubSub.Authorization.EventPattern == eventPattern {
			return true
		}
	}
	return false
}
