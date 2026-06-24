package bus_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestImportBoundaryInputAliasRoutesParentEventToPackageInputPin(t *testing.T) {
	source := loadBusImportBoundaryAliasSource(t)
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	routes := rt.Resolve("parent.lead_captured")
	if len(routes) != 1 || routes[0].ID != "worker-node" {
		t.Fatalf("Resolve(parent.lead_captured) = %#v, want worker-node", routes)
	}
	if got := routes[0].LocalizedEvent; got != "work.requested" {
		t.Fatalf("localized event = %q, want work.requested", got)
	}
	if got := rt.Resolve("work.requested"); len(got) != 0 {
		t.Fatalf("Resolve(work.requested) = %#v, want no raw fallback", got)
	}

	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := events.NewProjectionEvent("evt-input-alias", "parent.lead_captured", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "worker-node" {
		t.Fatalf("routed recipients = %#v, want worker-node", plan.RoutedRecipients)
	}
	if got := plan.RoutedRecipients[0].LocalizedEvent; got != "work.requested" {
		t.Fatalf("plan localized event = %q, want work.requested", got)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.deliveries["evt-input-alias"]; len(got) != 1 || got[0] != "worker-node" {
		t.Fatalf("persisted deliveries = %#v, want worker-node", got)
	}
}

func TestImportBoundaryOutputAliasRoutesPackageOutputToParentEvent(t *testing.T) {
	source := loadBusImportBoundaryAliasSource(t)
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	routes := rt.Resolve("worker/work.completed")
	if len(routes) != 1 || routes[0].ID != "parent-listener" {
		t.Fatalf("Resolve(worker/work.completed) = %#v, want parent-listener", routes)
	}
	if got := routes[0].LocalizedEvent; got != "parent.lead_enriched" {
		t.Fatalf("localized event = %q, want parent.lead_enriched", got)
	}

	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := events.NewProjectionEvent("evt-output-alias", "worker/work.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "parent-listener" {
		t.Fatalf("routed recipients = %#v, want parent-listener", plan.RoutedRecipients)
	}
	if got := plan.RoutedRecipients[0].LocalizedEvent; got != "parent.lead_enriched" {
		t.Fatalf("plan localized event = %q, want parent.lead_enriched", got)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.deliveries["evt-output-alias"]; len(got) != 1 || got[0] != "parent-listener" {
		t.Fatalf("persisted deliveries = %#v, want parent-listener", got)
	}
}

func TestImportBoundaryOutputAliasRoutesPackageOutputToWildcardParentSubscriber(t *testing.T) {
	source := loadBusImportBoundaryAliasWildcardOutputSource(t)
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	routes := rt.Resolve("worker/work.completed")
	if len(routes) != 1 || routes[0].ID != "parent-listener" {
		t.Fatalf("Resolve(worker/work.completed) = %#v, want parent-listener", routes)
	}
	if got := routes[0].MatchPattern; got != "parent.*" {
		t.Fatalf("match pattern = %q, want parent.*", got)
	}
	if got := routes[0].LocalizedEvent; got != "parent.lead_enriched" {
		t.Fatalf("localized event = %q, want parent.lead_enriched", got)
	}

	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := events.NewProjectionEvent("evt-output-alias-wildcard", "worker/work.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "parent-listener" {
		t.Fatalf("routed recipients = %#v, want parent-listener", plan.RoutedRecipients)
	}
	if got := plan.RoutedRecipients[0].LocalizedEvent; got != "parent.lead_enriched" {
		t.Fatalf("plan localized event = %q, want parent.lead_enriched", got)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.deliveries["evt-output-alias-wildcard"]; len(got) != 1 || got[0] != "parent-listener" {
		t.Fatalf("persisted deliveries = %#v, want parent-listener", got)
	}
}

func TestImportBoundaryInputAliasMaterializesTemplateRoute(t *testing.T) {
	source := loadBusImportBoundaryAliasTemplateSource(t)
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	if got := rt.Resolve("parent.lead_captured"); len(got) != 0 {
		t.Fatalf("Resolve(parent.lead_captured) before materialization = %#v, want none", got)
	}
	if err := rt.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: runtimeflowidentity.DeriveRoute("worker", "inst-1"),
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	routes := rt.Resolve("parent.lead_captured")
	if len(routes) != 1 || routes[0].ID != "worker-node" {
		t.Fatalf("Resolve(parent.lead_captured) = %#v, want worker-node", routes)
	}
	if got := routes[0].Path; got != "worker/inst-1" {
		t.Fatalf("route path = %q, want worker/inst-1", got)
	}
	if got := routes[0].LocalizedEvent; got != "work.requested" {
		t.Fatalf("localized event = %q, want work.requested", got)
	}
	if got := rt.Resolve("work.requested"); len(got) != 0 {
		t.Fatalf("Resolve(work.requested) = %#v, want no raw template fallback", got)
	}
}

func (s *routePersistenceTestStore) InsertEventDeliveryRoutes(_ context.Context, eventID string, routes []events.DeliveryRoute) error {
	if s.deliveries == nil {
		s.deliveries = map[string][]string{}
	}
	recipients := make([]string, 0, len(routes))
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.SubscriberID != "" {
			recipients = append(recipients, route.SubscriberID)
		}
	}
	s.deliveries[eventID] = recipients
	return nil
}

func loadBusImportBoundaryAliasSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeBusImportBoundaryAliasFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadBusImportBoundaryAliasWildcardOutputSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeBusImportBoundaryAliasFixtureWithParentSubscription(t, "parent.*")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadBusImportBoundaryAliasTemplateSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeBusImportBoundaryAliasTemplateFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeBusImportBoundaryAliasFixture(t *testing.T) string {
	return writeBusImportBoundaryAliasFixtureWithParentSubscription(t, "parent.lead_enriched")
}

func writeBusImportBoundaryAliasFixtureWithParentSubscription(t *testing.T, parentSubscription string) string {
	t.Helper()
	root := t.TempDir()
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: bus-import-boundary-alias
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: worker
    flow: worker
    mode: static
    bind:
      inputs:
        work.requested: parent.lead_captured
      outputs:
        work.completed: parent.lead_enriched
`)
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: bus-import-boundary-alias\n")
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "events.yaml"), `
parent.lead_captured: {}
parent.lead_enriched: {}
`)
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
parent-listener:
  id: parent-listener
  execution_type: system_node
  subscribes_to: [`+parentSubscription+`]
  event_handlers:
    parent.lead_enriched: {}
`)
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), `
name: worker
version: "1.0.0"
requires:
  inputs: [work.requested]
  outputs: [work.completed]
`)
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
mode: static
pins:
  inputs:
    events: [work.requested]
  outputs:
    events: [work.completed]
`)
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), "{}\n")
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), "{}\n")
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), "work.completed: {}\n")
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "flows", "worker", "nodes.yaml"), `
worker-node:
  id: worker-node
  execution_type: system_node
  subscribes_to: [work.requested]
  produces: [work.completed]
  event_handlers:
    work.requested:
      emit: work.completed
`)
	return root
}

func writeBusImportBoundaryAliasTemplateFixture(t *testing.T) string {
	t.Helper()
	root := writeBusImportBoundaryAliasFixture(t)
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: bus-import-boundary-alias
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: worker
    flow: worker
    mode: template
    bind:
      inputs:
        work.requested: parent.lead_captured
      outputs:
        work.completed: parent.lead_enriched
`)
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
mode: template
pins:
  inputs:
    events: [work.requested]
  outputs:
    events: [work.completed]
`)
	return root
}

func writeBusImportBoundaryFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir fixture dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}
