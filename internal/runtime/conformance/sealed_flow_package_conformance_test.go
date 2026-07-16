package conformance

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

func TestSealedFlowPackageConformance_CoversBoundaryOwners(t *testing.T) {
	source := sealedpackage.LoadSource(t, sealedpackage.Options{})
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("sealed package conformance hard invalidities = %#v, want none", got)
	}

	assertSealedPackageConformanceDependencies(t, source)
	assertSealedPackageConformanceWildcardScope(t, source)
	assertSealedPackageConformanceConnectRoutePlan(t, source)
	assertSealedPackageConformancePublishPreflight(t, source)
}

func TestSealedFlowPackageConformance_FailClosedMatrix(t *testing.T) {
	tests := []struct {
		name        string
		opts        sealedpackage.Options
		checkID     string
		wantMessage string
	}{
		{
			name:        "missing required input bind",
			opts:        sealedpackage.Options{OmitConsumerInputBind: true},
			checkID:     "flow_package_import_completeness",
			wantMessage: "required input bindings control.start",
		},
		{
			name:        "missing policy bind",
			opts:        sealedpackage.Options{OmitConsumerPolicyBind: true},
			checkID:     "flow_package_dependency_binding",
			wantMessage: "declared package policy dependency has no import binding or package default",
		},
		{
			name:        "missing credential bind",
			opts:        sealedpackage.Options{OmitConsumerCredentialBind: true},
			checkID:     "flow_package_dependency_binding",
			wantMessage: "declared package credential dependency has no import binding",
		},
		{
			name:        "forbidden sibling wildcard",
			opts:        sealedpackage.Options{ForbiddenSiblingWildcard: true},
			checkID:     "flow_package_wildcard_observe_grant",
			wantMessage: "no package-subtree candidate",
		},
		{
			name:        "invalid parent connect receiver",
			opts:        sealedpackage.Options{InvalidConnectReceiver: true},
			checkID:     "composition_connect_validation",
			wantMessage: "receiver_input_pin_missing",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := sealedpackage.LoadSource(t, tc.opts)
			report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
			if !sealedPackageConformanceFindingContains(report.HardInvalidities(), tc.checkID, tc.wantMessage) {
				t.Fatalf("expected %s containing %q, got %#v", tc.checkID, tc.wantMessage, report.HardInvalidities())
			}
		})
	}
}

func assertSealedPackageConformanceDependencies(t *testing.T, source semanticview.Source) {
	t.Helper()

	if got, ok := semanticview.PolicyValueForFlow(source, "producer", "runtime.profile"); !ok || got.Value != "producer-bound" {
		t.Fatalf("producer runtime.profile = (%#v, %v), want explicit producer binding", got.Value, ok)
	}
	if got, ok := semanticview.PolicyValueForFlow(source, "consumer", "runtime.profile"); !ok || got.Value != "consumer-bound" {
		t.Fatalf("consumer runtime.profile = (%#v, %v), want explicit consumer binding", got.Value, ok)
	}
	if _, ok := semanticview.PolicyValueForFlow(source, "consumer", "runtime.ambient"); ok {
		t.Fatal("consumer inherited ambient parent policy without a declared import binding")
	}

	if got, mapped := semanticview.CredentialStoreKeyForFlow(source, "producer", "shared_token"); !mapped || got != "producer_deployment_token" {
		t.Fatalf("producer shared_token credential key = (%q, %v), want explicit producer credential binding", got, mapped)
	}
	if got, mapped := semanticview.CredentialStoreKeyForFlow(source, "consumer", "shared_token"); !mapped || got != "consumer_deployment_token" {
		t.Fatalf("consumer shared_token credential key = (%q, %v), want explicit consumer credential binding", got, mapped)
	}
	if got, mapped := semanticview.CredentialStoreKeyForFlow(source, "consumer", "ambient_token"); !mapped || got != "" {
		t.Fatalf("consumer ambient_token credential key = (%q, %v), want fail-closed empty key", got, mapped)
	}
}

func assertSealedPackageConformanceWildcardScope(t *testing.T, source semanticview.Source) {
	t.Helper()

	if _, ok := source.NodeEventHandler("consumer-node", "producer/audit.seen"); ok {
		t.Fatal("consumer handler matched producer/audit.seen through raw sibling wildcard fallback")
	}
	if _, ok := source.NodeEventHandler("consumer-node", "consumer/audit.seen"); !ok {
		t.Fatal("consumer handler did not match its own package-subtree wildcard event")
	}
}

func assertSealedPackageConformanceConnectRoutePlan(t *testing.T, source semanticview.Source) {
	t.Helper()

	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 3 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want three explicit connect route plans", plans)
	}
	var plan runtimepinrouting.ConnectRoutePlan
	for _, candidate := range plans {
		if candidate.Source.ResolvedEvent == "producer/work.ready" && candidate.Receiver.ResolvedEvent == "consumer/work.ready" {
			plan = candidate
			break
		}
	}
	if plan.Source.ResolvedEvent == "" {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, missing producer/work.ready -> consumer/work.ready", plans)
	}
	if got, want := plan.Source.ResolvedEvent, "producer/work.ready"; got != want {
		t.Fatalf("source resolved event = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.ResolvedEvent, "consumer/work.ready"; got != want {
		t.Fatalf("receiver resolved event = %q, want %q", got, want)
	}
	if plan.Target.FlowInstance != "consumer" || plan.Target.EntityID != runtimeflowidentity.EntityID("consumer") {
		t.Fatalf("connect plan target = %#v, want static consumer route", plan.Target)
	}
	if plan.RequiresRuntimeResolution {
		t.Fatal("static sealed package connect route unexpectedly requires runtime descriptor resolution")
	}
}

func assertSealedPackageConformancePublishPreflight(t *testing.T, source semanticview.Source) {
	t.Helper()

	eb, err := newScopedTestEventBus(t, nil, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if carriers := eb.RouteTable().Resolve("consumer/work.ready"); !sealedPackageConformanceSubscribersContain(carriers, "consumer-node", "consumer", "receiver_carrier") {
		t.Fatalf("receiver carrier route consumer/work.ready = %#v, want consumer-node receiver_carrier", carriers)
	}

	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node",
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: "consumer",
			EntityID:     runtimeflowidentity.EntityID("consumer"),
		},
	}
	evt := eventtest.ChildWithLineage(
		"evt-sealed-conformance-connect",
		events.EventType("producer/work.ready"),
		"producer",
		"",
		json.RawMessage(`{"work_id":"work-1"}`),
		1,
		events.EventLineage{RunID: "run-sealed-package-conformance", ParentEventID: "evt-sealed-parent", TaskID: "producer-node", ExecutionMode: executionmode.Live},
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	plan, err := eb.CheckPublishRecipientPlan(testAuthorActivityContext(context.Background()), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("preflight target failure = %q, want none", plan.TargetFailure)
	}
	if len(plan.DeliveryRoutes) != 1 || !sealedPackageConformanceRoutesContain(plan.DeliveryRoutes, want) {
		t.Fatalf("preflight delivery routes = %#v, want only %#v", plan.DeliveryRoutes, want)
	}

	sibling := eventtest.ChildWithLineage(
		"evt-sealed-conformance-sibling",
		events.EventType("producer/audit.seen"),
		"producer",
		"",
		json.RawMessage(`{"flow_instance":"consumer"}`),
		1,
		events.EventLineage{RunID: "run-sealed-package-conformance", ParentEventID: "evt-sealed-parent", TaskID: "producer-node", ExecutionMode: executionmode.Live},
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	siblingPlan, err := eb.CheckPublishRecipientPlan(testAuthorActivityContext(context.Background()), sibling)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan sibling wildcard: %v", err)
	}
	if len(siblingPlan.DeliveryRoutes) != 0 || len(siblingPlan.RoutedRecipients) != 0 || len(siblingPlan.Recipients) != 0 {
		t.Fatalf("sibling wildcard plan = routes:%#v routed:%#v recipients:%#v, want no executable sibling route", siblingPlan.DeliveryRoutes, siblingPlan.RoutedRecipients, siblingPlan.Recipients)
	}
}

func sealedPackageConformanceFindingContains(findings []runtimebootverify.Finding, checkID, substr string) bool {
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

func sealedPackageConformanceSubscribersContain(subscribers []runtimebus.Subscriber, id, path, source string) bool {
	for _, subscriber := range subscribers {
		if subscriber.ID == id && subscriber.Path == path && subscriber.RouteSource == source {
			return true
		}
	}
	return false
}

func sealedPackageConformanceRoutesContain(routes []events.DeliveryRoute, want events.DeliveryRoute) bool {
	want = want.Normalized()
	for _, got := range events.NormalizeDeliveryRoutes(routes) {
		if got == want {
			return true
		}
	}
	return false
}
