package bus_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/sealedpackage"
)

func TestSealedFlowPackageFixture_EndToEndCompositionBoundary(t *testing.T) {
	source := sealedpackage.LoadSource(t, sealedpackage.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("sealed package fixture verify hard invalidities = %#v, want none", got)
	}

	assertSealedFlowPackageDependencies(t, source)
	assertSealedFlowPackageWildcardScope(t, source)
	assertSealedFlowPackageConnectPlan(t, source)
	assertSealedFlowPackageRuntimeDelivery(t, source)
}

func TestSealedFlowPackageFixture_FailsClosedForMissingBoundaryProofs(t *testing.T) {
	tests := []struct {
		name        string
		opts        sealedpackage.Options
		checkID     string
		wantMessage string
	}{
		{
			name: "unsatisfied required input",
			opts: sealedpackage.Options{
				OmitConsumerInputBind: true,
			},
			checkID:     "flow_package_import_completeness",
			wantMessage: "required input bindings control.start",
		},
		{
			name: "missing policy binding ignores ambient same-name parent policy",
			opts: sealedpackage.Options{
				OmitConsumerPolicyBind: true,
			},
			checkID:     "flow_package_dependency_binding",
			wantMessage: "declared package policy dependency has no import binding or package default",
		},
		{
			name: "missing credential binding ignores same-name credential key",
			opts: sealedpackage.Options{
				OmitConsumerCredentialBind: true,
			},
			checkID:     "flow_package_dependency_binding",
			wantMessage: "declared package credential dependency has no import binding",
		},
		{
			name: "forbidden sibling wildcard observation",
			opts: sealedpackage.Options{
				ForbiddenSiblingWildcard: true,
			},
			checkID:     "flow_package_wildcard_observe_grant",
			wantMessage: "no package-subtree candidate",
		},
		{
			name: "missing parent connect receiver input pin",
			opts: sealedpackage.Options{
				InvalidConnectReceiver: true,
			},
			checkID:     "composition_connect_validation",
			wantMessage: "receiver_input_pin_missing",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := sealedpackage.LoadSource(t, tc.opts)
			report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
			if !sealedFlowPackageFindingContains(report.HardInvalidities(), tc.checkID, tc.wantMessage) {
				t.Fatalf("expected %s containing %q, got %#v", tc.checkID, tc.wantMessage, report.HardInvalidities())
			}
		})
	}
}

func assertSealedFlowPackageDependencies(t *testing.T, source semanticview.Source) {
	t.Helper()

	if got, ok := semanticview.PolicyValueForFlow(source, "producer", "runtime.profile"); !ok || got.Value != "producer-bound" {
		t.Fatalf("producer runtime.profile = (%#v, %v), want producer-bound from explicit parent binding", got.Value, ok)
	}
	if got, ok := semanticview.PolicyValueForFlow(source, "consumer", "runtime.profile"); !ok || got.Value != "consumer-bound" {
		t.Fatalf("consumer runtime.profile = (%#v, %v), want consumer-bound from explicit parent binding", got.Value, ok)
	}
	if _, ok := semanticview.PolicyValueForFlow(source, "consumer", "runtime.ambient"); ok {
		t.Fatal("consumer inherited ambient parent policy without an import binding")
	}

	if got, mapped := semanticview.CredentialStoreKeyForFlow(source, "producer", "shared_token"); !mapped || got != "producer_deployment_token" {
		t.Fatalf("producer shared_token credential key = (%q, %v), want producer_deployment_token", got, mapped)
	}
	if got, mapped := semanticview.CredentialStoreKeyForFlow(source, "consumer", "shared_token"); !mapped || got != "consumer_deployment_token" {
		t.Fatalf("consumer shared_token credential key = (%q, %v), want consumer_deployment_token", got, mapped)
	}
	if got, mapped := semanticview.CredentialStoreKeyForFlow(source, "consumer", "ambient_token"); !mapped || got != "" {
		t.Fatalf("consumer ambient_token credential key = (%q, %v), want mapped fail-closed empty key", got, mapped)
	}
}

func assertSealedFlowPackageWildcardScope(t *testing.T, source semanticview.Source) {
	t.Helper()

	if owners := source.RuntimeEventOwners("producer/audit.seen"); len(owners) != 0 {
		t.Fatalf("RuntimeEventOwners(producer/audit.seen) = %#v, want no consumer wildcard sibling leakage", owners)
	}
	if _, ok := source.NodeEventHandler("consumer-node", "producer/audit.seen"); ok {
		t.Fatal("consumer handler matched producer/audit.seen through raw sibling wildcard fallback")
	}
	if _, ok := source.NodeEventHandler("consumer-node", "consumer/audit.seen"); !ok {
		t.Fatal("consumer handler did not match its own package-subtree wildcard event")
	}
}

func assertSealedFlowPackageConnectPlan(t *testing.T, source semanticview.Source) {
	t.Helper()

	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one parent connect plan", plans)
	}
	plan := plans[0]
	if got, want := plan.Source.ResolvedEvent, "producer/work.ready"; got != want {
		t.Fatalf("source resolved event = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.ResolvedEvent, "consumer/work.ready"; got != want {
		t.Fatalf("receiver resolved event = %q, want %q", got, want)
	}
	if plan.Address != nil {
		t.Fatalf("connect plan address = %#v, want canonical static target", plan.Address)
	}
	if plan.Target.FlowInstance != "consumer" || plan.Target.EntityID != runtimeflowidentity.EntityID("consumer") {
		t.Fatalf("connect plan target = %#v, want static consumer route", plan.Target)
	}
	if plan.RequiresRuntimeResolution {
		t.Fatal("static sealed package connect route unexpectedly requires runtime descriptor resolution")
	}
}

func assertSealedFlowPackageRuntimeDelivery(t *testing.T, source semanticview.Source) {
	t.Helper()

	store := &sealedFlowPackageRouteStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if carriers := eb.RouteTable().Resolve("consumer/work.ready"); !sealedFlowPackageSubscriberListContains(carriers, "consumer-node", "consumer", "receiver_carrier") {
		t.Fatalf("receiver carrier route consumer/work.ready = %#v, want consumer-node receiver_carrier", carriers)
	}

	wantRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node",
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: "consumer",
			EntityID:     runtimeflowidentity.EntityID("consumer"),
		},
	}
	evt := eventtest.ChildWithLineage(
		"evt-sealed-connect",
		events.EventType("producer/work.ready"),
		"producer",
		"",
		json.RawMessage(`{"work_id":"work-1"}`),
		1,
		events.EventLineage{RunID: "run-sealed-package", ParentEventID: "evt-sealed-parent", TaskID: "producer-node", ExecutionMode: executionmode.Live},
		events.EventEnvelope{},
		time.Now().UTC(),
	)

	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" {
		t.Fatalf("preflight target failure = %q, want none", preflight.TargetFailure)
	}
	if !sealedFlowPackageDeliveryRoutesContain(preflight.DeliveryRoutes, wantRoute) || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight delivery routes = %#v, want only %#v", preflight.DeliveryRoutes, wantRoute)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[evt.ID()]; !sealedFlowPackageDeliveryRoutesContain(routes, wantRoute) || len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want only %#v", routes, wantRoute)
	}

	sibling := eventtest.ChildWithLineage(
		"evt-sealed-sibling-wildcard",
		events.EventType("producer/audit.seen"),
		"producer",
		"",
		json.RawMessage(`{"flow_instance":"consumer"}`),
		1,
		events.EventLineage{RunID: "run-sealed-package", ParentEventID: "evt-sealed-parent", TaskID: "producer-node", ExecutionMode: executionmode.Live},
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	siblingPlan, err := eb.CheckPublishRecipientPlan(context.Background(), sibling)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan sibling wildcard: %v", err)
	}
	if len(siblingPlan.DeliveryRoutes) != 0 || len(siblingPlan.RoutedRecipients) != 0 || len(siblingPlan.Recipients) != 0 {
		t.Fatalf("sibling wildcard plan = routes:%#v routed:%#v recipients:%#v, want no executable sibling route", siblingPlan.DeliveryRoutes, siblingPlan.RoutedRecipients, siblingPlan.Recipients)
	}
	if err := eb.Publish(context.Background(), sibling); err != nil {
		t.Fatalf("Publish sibling wildcard: %v", err)
	}
	if routes := store.routes[sibling.ID()]; len(routes) != 0 {
		t.Fatalf("sibling wildcard persisted routes = %#v, want none", routes)
	}
}

type sealedFlowPackageRouteStore struct {
	events map[string]events.Event
	routes map[string][]events.DeliveryRoute
}

func (s *sealedFlowPackageRouteStore) AppendEvent(_ context.Context, evt events.Event) error {
	if s.events == nil {
		s.events = map[string]events.Event{}
	}
	s.events[evt.ID()] = evt
	return nil
}

func (s *sealedFlowPackageRouteStore) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	if s.routes == nil {
		s.routes = map[string][]events.DeliveryRoute{}
	}
	routes := make([]events.DeliveryRoute, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		routes = append(routes, events.DeliveryRoute{
			SubscriberType: "agent",
			SubscriberID:   agentID,
		})
	}
	s.routes[eventID] = events.NormalizeDeliveryRoutes(routes)
	return nil
}

func (s *sealedFlowPackageRouteStore) InsertEventDeliveryRoutes(_ context.Context, eventID string, routes []events.DeliveryRoute) error {
	if s.routes == nil {
		s.routes = map[string][]events.DeliveryRoute{}
	}
	s.routes[eventID] = events.NormalizeDeliveryRoutes(routes)
	return nil
}

func (s *sealedFlowPackageRouteStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	var out []string
	for _, route := range events.NormalizeDeliveryRoutes(s.routes[eventID]) {
		if route.SubscriberType == "agent" {
			out = append(out, route.SubscriberID)
		}
	}
	return out, nil
}

func sealedFlowPackageFindingContains(findings []runtimebootverify.Finding, checkID, substr string) bool {
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

func sealedFlowPackageDeliveryRoutesContain(routes []events.DeliveryRoute, want events.DeliveryRoute) bool {
	want = want.Normalized()
	for _, got := range events.NormalizeDeliveryRoutes(routes) {
		if got == want {
			return true
		}
	}
	return false
}

func sealedFlowPackageSubscriberListContains(subscribers []runtimebus.Subscriber, id, path, source string) bool {
	for _, subscriber := range subscribers {
		if subscriber.ID == id && subscriber.Path == path && subscriber.RouteSource == source {
			return true
		}
	}
	return false
}
