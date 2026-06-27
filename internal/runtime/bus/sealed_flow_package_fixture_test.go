package bus_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestSealedFlowPackageFixture_EndToEndCompositionBoundary(t *testing.T) {
	source := loadSealedFlowPackageFixtureSource(t, sealedFlowPackageFixtureOptions{})
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
		opts        sealedFlowPackageFixtureOptions
		checkID     string
		wantMessage string
	}{
		{
			name: "unsatisfied required input",
			opts: sealedFlowPackageFixtureOptions{
				omitConsumerInputBind: true,
			},
			checkID:     "flow_package_import_completeness",
			wantMessage: "required input bindings control.start",
		},
		{
			name: "missing policy binding ignores ambient same-name parent policy",
			opts: sealedFlowPackageFixtureOptions{
				omitConsumerPolicyBind: true,
			},
			checkID:     "flow_package_dependency_binding",
			wantMessage: "declared package policy dependency has no import binding or package default",
		},
		{
			name: "missing credential binding ignores same-name credential key",
			opts: sealedFlowPackageFixtureOptions{
				omitConsumerCredentialBind: true,
			},
			checkID:     "flow_package_dependency_binding",
			wantMessage: "declared package credential dependency has no import binding",
		},
		{
			name: "forbidden sibling wildcard observation",
			opts: sealedFlowPackageFixtureOptions{
				forbiddenSiblingWildcard: true,
			},
			checkID:     "flow_package_wildcard_observe_grant",
			wantMessage: "no package-subtree candidate",
		},
		{
			name: "missing parent connect receiver input pin",
			opts: sealedFlowPackageFixtureOptions{
				invalidConnectReceiver: true,
			},
			checkID:     "composition_connect_validation",
			wantMessage: "receiver_input_pin_missing",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := loadSealedFlowPackageFixtureSource(t, tc.opts)
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
	if _, ok := source.NodeEventHandler("consumer-handler", "producer/audit.seen"); ok {
		t.Fatal("consumer handler matched producer/audit.seen through raw sibling wildcard fallback")
	}
	if _, ok := source.NodeEventHandler("consumer-handler", "consumer/audit.seen"); !ok {
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
	if got, want := plan.Source.ResolvedEvent, "producer/shared.done"; got != want {
		t.Fatalf("source resolved event = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.ResolvedEvent, "consumer/shared.done"; got != want {
		t.Fatalf("receiver resolved event = %q, want %q", got, want)
	}
	if got, want := plan.Delivery, runtimepinrouting.ConnectDeliveryOne; got != want {
		t.Fatalf("delivery = %q, want %q", got, want)
	}
	if plan.Address == nil || plan.Address.By != "flow_instance" || plan.Address.Source != "payload.flow_instance" || plan.Address.Target != "instance.flow_instance" {
		t.Fatalf("connect plan address = %#v, want flow_instance payload/instance mapping", plan.Address)
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
	if carriers := eb.RouteTable().Resolve("consumer/shared.done"); !sealedFlowPackageSubscriberListContains(carriers, "consumer-handler", "consumer", "receiver_carrier") {
		t.Fatalf("receiver carrier route consumer/shared.done = %#v, want consumer-handler receiver_carrier", carriers)
	}

	wantRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-handler",
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: "consumer",
			EntityID:     runtimeflowidentity.EntityID("consumer"),
		},
	}
	evt := events.NewProjectionEvent("evt-sealed-connect",
		events.EventType("producer/shared.done"), "", "", json.RawMessage(`{"flow_instance":"consumer"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

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

	sibling := events.NewProjectionEvent("evt-sealed-sibling-wildcard",
		events.EventType("producer/audit.seen"), "", "", json.RawMessage(`{"flow_instance":"consumer"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
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

type sealedFlowPackageFixtureOptions struct {
	omitConsumerInputBind      bool
	omitConsumerPolicyBind     bool
	omitConsumerCredentialBind bool
	forbiddenSiblingWildcard   bool
	invalidConnectReceiver     bool
}

func loadSealedFlowPackageFixtureSource(t *testing.T, opts sealedFlowPackageFixtureOptions) semanticview.Source {
	t.Helper()
	repoRoot := sealedFlowPackageRepoRoot(t)
	fixtureRoot := writeSealedFlowPackageFixture(t, opts)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeSealedFlowPackageFixture(t *testing.T, opts sealedFlowPackageFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	connectTo := "consumer.shared_done"
	if opts.invalidConnectReceiver {
		connectTo = "consumer.missing_shared_done"
	}
	consumerSubscription := "**/audit.seen"
	if opts.forbiddenSiblingWildcard {
		consumerSubscription = "producer/**/audit.seen"
	}

	sealedFlowPackageWriteFile(t, filepath.Join(root, "package.yaml"), `
name: sealed-flow-package-fixture
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: producer
    flow: producer
    mode: static
    bind:
      inputs:
        source.start: parent.producer_start
      outputs:
        shared.done: parent.producer_done
      policy:
        runtime.profile: parent.policy.producer.runtime.profile
      credentials:
        shared_token: producer_deployment_token
  - id: consumer
    flow: consumer
    mode: static
`+sealedFlowPackageConsumerBindYAML(opts)+sealedFlowPackageConnectYAML(connectTo)+`
`)
	writeSealedFlowPackageRootFiles(t, root)
	writeSealedFlowPackageConsumer(t, root, consumerSubscription)
	return root
}

func sealedFlowPackageConnectYAML(connectTo string) string {
	return `connect:
  - from: producer.shared_done
    to: ` + connectTo + `
    delivery: one
    map:
      flow_instance:
        source: payload.flow_instance
        target: instance.flow_instance
`
}

func writeSealedFlowPackageRootFiles(t *testing.T, root string) {
	sealedFlowPackageWriteFile(t, filepath.Join(root, "schema.yaml"), "name: sealed-flow-package-fixture\n")
	sealedFlowPackageWriteFile(t, filepath.Join(root, "policy.yaml"), `
producer:
  runtime:
    profile: producer-bound
consumer:
  runtime:
    profile: consumer-bound
runtime:
  profile: ambient-root
  ambient: should-not-leak
`)
	sealedFlowPackageWriteFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	sealedFlowPackageWriteFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	sealedFlowPackageWriteFile(t, filepath.Join(root, "events.yaml"), `
parent.producer_start:
  flow_instance: string
parent.producer_done:
  flow_instance: string
parent.consumer_start:
  flow_instance: string
`)
	sealedFlowPackageWriteFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeSealedFlowPackageProducer(t, root)
}

func sealedFlowPackageConsumerBindYAML(opts sealedFlowPackageFixtureOptions) string {
	var b strings.Builder
	b.WriteString("    bind:\n")
	if !opts.omitConsumerInputBind {
		b.WriteString("      inputs:\n")
		b.WriteString("        control.start: parent.consumer_start\n")
	}
	if !opts.omitConsumerPolicyBind {
		b.WriteString("      policy:\n")
		b.WriteString("        runtime.profile: parent.policy.consumer.runtime.profile\n")
	}
	if !opts.omitConsumerCredentialBind {
		b.WriteString("      credentials:\n")
		b.WriteString("        shared_token: consumer_deployment_token\n")
	}
	return b.String()
}

func writeSealedFlowPackageProducer(t *testing.T, root string) {
	t.Helper()
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "producer", "package.yaml"), `
name: sealed-shared-component
version: "1.0.0"
requires:
  inputs: [source.start]
  outputs: [shared.done]
  policy: [runtime.profile]
  credentials: [shared_token]
`)
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: sealed-shared-component
mode: static
pins:
  inputs:
    events:
      - name: source_start
        event: source.start
  outputs:
    events:
      - name: shared_done
        event: shared.done
`)
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), `
runtime:
  profile: producer-local
`)
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "producer", "tools.yaml"), "{}\n")
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
source.start:
  flow_instance: string
shared.done:
  flow_instance: string
audit.seen:
  flow_instance: string
`)
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
producer-handler:
  id: producer-handler
  execution_type: system_node
  subscribes_to: [source.start]
  produces: [shared.done, audit.seen]
  event_handlers:
    source.start: {}
`)
}

func writeSealedFlowPackageConsumer(t *testing.T, root, wildcardSubscription string) {
	t.Helper()
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "consumer", "package.yaml"), `
name: sealed-shared-component
version: "1.0.0"
requires:
  inputs: [control.start]
  outputs: []
  policy: [runtime.profile]
  credentials: [shared_token]
`)
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: sealed-shared-component
mode: static
pins:
  inputs:
    events:
      - name: control_start
        event: control.start
      - name: shared_done
        event: shared.done
        address:
          by: flow_instance
          source: payload.flow_instance
          target: instance.flow_instance
          cardinality: one
`)
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), `
runtime:
  profile: consumer-local
`)
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "consumer", "tools.yaml"), "{}\n")
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
shared.done:
  flow_instance: string
control.start:
  flow_instance: string
audit.seen:
  flow_instance: string
`)
	sealedFlowPackageWriteFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-handler:
  id: consumer-handler
  execution_type: system_node
  subscribes_to: [shared.done, "`+wildcardSubscription+`"]
  event_handlers:
    shared.done: {}
    "`+wildcardSubscription+`": {}
`)
}

func sealedFlowPackageRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func sealedFlowPackageWriteFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir fixture dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
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
