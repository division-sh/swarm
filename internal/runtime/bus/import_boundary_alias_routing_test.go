package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
		{id: eventtest.UUID("evt-input-connect"), eventType: "parent.lead_captured", recipient: "worker-node"},
		{
			id:        eventtest.UUID("evt-output-connect"),
			eventType: "worker/work.completed",
			recipient: "parent-listener",
			envelope:  events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: eventtest.UUID("root-entity")}),
		},
	} {
		evt := eventtest.RunCreatingRootIngress(tc.id, events.EventType(tc.eventType), "", "", []byte(`{}`), 0, "", "", tc.envelope, time.Now().UTC())
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
	return loadBusImportBoundaryVariant(t, canonicalrouting.ImportBoundaryAliasBindOnly)
}

func loadBusImportBoundaryConnectedSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadBusImportBoundaryVariant(t, canonicalrouting.ImportBoundaryAliasConnected)
}

func loadBusImportBoundaryAliasWildcardOutputSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadBusImportBoundaryVariant(t, canonicalrouting.ImportBoundaryAliasBindOnlyWildcardOutput)
}

func loadBusImportBoundaryAliasTemplateSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadBusImportBoundaryVariant(t, canonicalrouting.ImportBoundaryAliasTemplateBindOnly)
}

func loadBusImportBoundaryVariant(t *testing.T, variant canonicalrouting.ImportBoundaryAliasVariant) semanticview.Source {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyImportBoundaryAlias(t, variant)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}
