package bus_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestImportBoundaryInputBindingDoesNotRouteWithoutConnect(t *testing.T) {
	source := loadBusImportBoundaryAliasSource(t)
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	routes := rt.Resolve("parent.lead_captured")
	if len(routes) != 0 {
		t.Fatalf("Resolve(parent.lead_captured) = %#v, want bind-only input to be inert", routes)
	}
}

func TestImportBoundaryOutputBindingDoesNotRouteWithoutConnect(t *testing.T) {
	source := loadBusImportBoundaryAliasSource(t)
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	routes := rt.Resolve("worker/work.completed")
	if len(routes) != 0 {
		t.Fatalf("Resolve(worker/work.completed) = %#v, want bind-only output to be inert", routes)
	}
}

func TestImportBoundaryOutputBindingDoesNotAuthorizeWildcardWithoutConnect(t *testing.T) {
	source := loadBusImportBoundaryAliasWildcardOutputSource(t)
	rt, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	routes := rt.Resolve("worker/work.completed")
	if len(routes) != 0 {
		t.Fatalf("Resolve(worker/work.completed) = %#v, want bind-only output to grant no wildcard route", routes)
	}
}

func TestImportBoundaryInputBindingDoesNotMaterializeTemplateRouteWithoutConnect(t *testing.T) {
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
	if len(routes) != 0 {
		t.Fatalf("Resolve(parent.lead_captured) = %#v, want bind-only template route to remain inert", routes)
	}
}

func TestImportBoundaryConnectConsumesBindingsForInputAndRootOutputDelivery(t *testing.T) {
	source := loadBusImportBoundaryConnectedSource(t)
	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 || len(plans) != 2 {
		t.Fatalf("connect plans = %#v, issues = %#v, want two valid plans", plans, issues)
	}
	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	for _, tc := range []struct {
		id        string
		eventType string
		recipient string
		envelope  events.EventEnvelope
	}{
		{id: "evt-input-connect", eventType: "parent.lead_captured", recipient: "worker-node"},
		{
			id:        "evt-output-connect",
			eventType: "worker/work.completed",
			recipient: "parent-listener",
			envelope:  events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: "root-entity"}),
		},
	} {
		evt := eventtest.RootIngress(tc.id, events.EventType(tc.eventType), "", "", []byte(`{}`), 0, "", "", tc.envelope, time.Now().UTC())
		plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
		if err != nil {
			t.Fatalf("CheckPublishRecipientPlan(%s): %v", tc.eventType, err)
		}
		if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != tc.recipient {
			t.Fatalf("routed recipients for %s = %#v, want %s", tc.eventType, plan.RoutedRecipients, tc.recipient)
		}
		if err := eb.Publish(context.Background(), evt); err != nil {
			t.Fatalf("Publish(%s): %v", tc.eventType, err)
		}
		if got := store.deliveries[tc.id]; len(got) != 1 || got[0] != tc.recipient {
			t.Fatalf("persisted deliveries for %s = %#v, want %s", tc.eventType, got, tc.recipient)
		}
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

func loadBusImportBoundaryConnectedSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeBusImportBoundaryConnectedFixture(t)
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
	return writeBusImportBoundaryAliasFixtureWithOptions(t, "parent.lead_enriched", false)
}

func writeBusImportBoundaryAliasFixtureWithParentSubscription(t *testing.T, parentSubscription string) string {
	return writeBusImportBoundaryAliasFixtureWithOptions(t, parentSubscription, false)
}

func writeBusImportBoundaryConnectedFixture(t *testing.T) string {
	return writeBusImportBoundaryAliasFixtureWithOptions(t, "parent.lead_enriched", true)
}

func writeBusImportBoundaryAliasFixtureWithOptions(t *testing.T, parentSubscription string, connected bool) string {
	t.Helper()
	root := t.TempDir()
	connect := ""
	rootSchema := "name: bus-import-boundary-alias\n"
	if connected {
		connect = `
connect:
  - from: .lead_captured
    to: worker.work_requested
  - from: worker.work_completed
    to: .lead_enriched
`
		rootSchema = `
name: bus-import-boundary-alias
pins:
  inputs:
    events:
      - name: lead_enriched
        event: parent.lead_enriched
  outputs:
    events:
      - name: lead_captured
        event: parent.lead_captured
`
	}
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: bus-import-boundary-alias
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: worker
    flow: worker
    mode: static
    bind:
      inputs:
        work.requested: parent.lead_captured
      outputs:
        work.completed: parent.lead_enriched
`+connect)
	writeBusImportBoundaryFixtureFile(t, filepath.Join(root, "schema.yaml"), rootSchema)
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
    events:
      - name: work_requested
        event: work.requested
  outputs:
    events:
      - name: work_completed
        event: work.completed
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
platform_version: ">=0.7.0 <0.8.0"
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
    events:
      - name: work_requested
        event: work.requested
  outputs:
    events:
      - name: work_completed
        event: work.completed
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
