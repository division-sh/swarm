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
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateselectexisting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateselectorcreate"
)

func TestTemplateFlowPilotConformance_CoversInstanceCenteredAuthoringOwners(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{})
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("template-flow pilot hard invalidities = %#v, want none", got)
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("template-flow pilot source did not expose bundle")
	}
	primary, err := bundle.ResolveFlowPrimaryEntity("account")
	if err != nil {
		t.Fatalf("ResolveFlowPrimaryEntity(account): %v", err)
	}
	if primary.EntityType != "account_state" {
		t.Fatalf("account primary entity = %q, want account_state", primary.EntityType)
	}
	instance, err := bundle.ResolveFlowTemplateInstance("account")
	if err != nil {
		t.Fatalf("ResolveFlowTemplateInstance(account): %v", err)
	}
	if got := strings.Join(instance.By, ","); got != "account_id" {
		t.Fatalf("account instance fields = %q, want account_id", got)
	}
	output, ok := bundle.FlowOutputEventPin("producer", "account_ready")
	if !ok {
		t.Fatal("producer account_ready output pin missing")
	}
	if output.Event != "account.ready" {
		t.Fatalf("producer output event = %q, want account.ready", output.Event)
	}

	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one template route plan", plans)
	}
	plan := plans[0]
	if plan.ResolutionKind != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution {
		t.Fatalf("route plan resolution = %s runtime=%v, want select-or-create runtime resolution", plan.ResolutionKind, plan.RequiresRuntimeResolution)
	}
	if plan.Source.FlowID != "producer" || plan.Source.Pin != "account_ready" {
		t.Fatalf("route plan source = %#v, want producer.account_ready", plan.Source)
	}
	if plan.Receiver.FlowID != "account" || plan.Receiver.Pin != "account_ready" || plan.Receiver.Mode != "template" {
		t.Fatalf("route plan receiver = %#v, want template account.account_ready", plan.Receiver)
	}
	if plan.InstanceKey == nil || plan.InstanceKey.Mode != "select-or-create" || strings.Join(plan.InstanceKey.Fields, ",") != "account_id" {
		t.Fatalf("route plan instance key = %#v, want select-or-create/account_id", plan.InstanceKey)
	}
	if len(plan.InstanceKey.Mappings) != 1 || plan.InstanceKey.Mappings[0].Source != "account_id" || plan.InstanceKey.Mappings[0].Target != "account_id" || !plan.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("route plan instance key mappings = %#v, want explicit account_id -> account_id", plan.InstanceKey.Mappings)
	}
}

func TestTemplateFlowPilotConformance_FailClosedMatrix(t *testing.T) {
	tests := []struct {
		name        string
		opts        templateflowpilot.Options
		checkID     string
		wantMessage string
	}{
		{
			name:        "unsupported receiver select_entity on connected normal path",
			opts:        templateflowpilot.Options{UnsupportedReceiverSelection: true},
			checkID:     "redundant_in_topology_select_entity",
			wantMessage: "instance.by plus parent connect",
		},
		{
			name:        "bad connect instance key mapping",
			opts:        templateflowpilot.Options{BadConnectMapping: true},
			checkID:     "composition_connect_validation",
			wantMessage: "connect.using.instance is incompatible with input pin resolution",
		},
		{
			name:        "producer target cannot rescue common composition",
			opts:        templateflowpilot.Options{ProducerTarget: true},
			checkID:     "pin_target_resolution",
			wantMessage: "producer_target_common_path_forbidden",
		},
		{
			name:        "producer broadcast cannot replace parent connect authority",
			opts:        templateflowpilot.Options{ProducerBroadcast: true},
			checkID:     "pin_target_resolution",
			wantMessage: "producer_broadcast_common_path_forbidden",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := templateflowpilot.LoadSource(t, tc.opts)
			report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
			if !templateFlowPilotConformanceFindingContains(report.HardInvalidities(), tc.checkID, tc.wantMessage) {
				t.Fatalf("expected hard invalidity %s containing %q, got %#v", tc.checkID, tc.wantMessage, report.HardInvalidities())
			}
		})
	}
}

func TestTemplateSelectExistingConformance_CoversResolutionSelectOwner(t *testing.T) {
	source := templateselectexisting.LoadSource(t)
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("template-select-existing hard invalidities = %#v, want none", got)
	}

	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	var plan runtimepinrouting.ConnectRoutePlan
	for _, candidate := range plans {
		if candidate.InstanceKey != nil && candidate.InstanceKey.Mode == "select" {
			plan = candidate
		}
	}
	if plan.InstanceKey == nil {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want select route plan", plans)
	}
	if plan.Source.FlowID != templateselectexisting.ProducerFlowID || plan.Source.Pin != templateselectexisting.ProducerOutputPin {
		t.Fatalf("route plan source = %#v, want %s.%s", plan.Source, templateselectexisting.ProducerFlowID, templateselectexisting.ProducerOutputPin)
	}
	if plan.Receiver.FlowID != templateselectexisting.TemplateFlowID || plan.Receiver.Pin != templateselectexisting.TemplateInputPin || plan.Receiver.Mode != "template" {
		t.Fatalf("route plan receiver = %#v, want template %s.%s", plan.Receiver, templateselectexisting.TemplateFlowID, templateselectexisting.TemplateInputPin)
	}
	if plan.ResolutionKind != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution {
		t.Fatalf("route plan resolution = %s runtime=%v, want runtime instance-key select", plan.ResolutionKind, plan.RequiresRuntimeResolution)
	}
	if plan.InstanceKey == nil || plan.InstanceKey.Mode != "select" || strings.Join(plan.InstanceKey.Fields, ",") != templateselectexisting.TemplateInstanceBy {
		t.Fatalf("route plan instance key = %#v, want select/account_id", plan.InstanceKey)
	}
	if len(plan.InstanceKey.Mappings) != 1 || plan.InstanceKey.Mappings[0].Source != templateselectexisting.TemplateInstanceBy || plan.InstanceKey.Mappings[0].Target != templateselectexisting.TemplateInstanceBy || !plan.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("route plan mappings = %#v, want explicit account_id -> account_id", plan.InstanceKey.Mappings)
	}

	materialized := runtimepinrouting.MaterializeConnectRoutePlan(plan, runtimepinrouting.ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"payload.account_id": "acct-1"},
		Descriptors: []runtimepinrouting.Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "account/one",
			AddressFields: map[string]string{"entity.account_id": "acct-1"},
		}},
	})
	if materialized.Failure != "" {
		t.Fatalf("MaterializeConnectRoutePlan failure = %q, want none", materialized.Failure)
	}
	if materialized.Target.FlowInstance != "account/one" || materialized.Target.EntityID != "ent-1" {
		t.Fatalf("materialized target = %#v, want account/one ent-1", materialized.Target)
	}
}

func TestTemplateSelectOrCreateConformance_CoversResolutionSelectOrCreateOwner(t *testing.T) {
	source := templateselectorcreate.LoadSource(t)
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("template-select-or-create hard invalidities = %#v, want none", got)
	}

	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one select-or-create route plan", plans)
	}
	plan := plans[0]
	if plan.Source.FlowID != templateselectorcreate.ProducerFlowID || plan.Source.Pin != templateselectorcreate.ProducerOutputPin {
		t.Fatalf("route plan source = %#v, want %s.%s", plan.Source, templateselectorcreate.ProducerFlowID, templateselectorcreate.ProducerOutputPin)
	}
	if plan.Receiver.FlowID != templateselectorcreate.TemplateFlowID || plan.Receiver.Pin != templateselectorcreate.TemplateInputPin || plan.Receiver.Mode != "template" {
		t.Fatalf("route plan receiver = %#v, want template %s.%s", plan.Receiver, templateselectorcreate.TemplateFlowID, templateselectorcreate.TemplateInputPin)
	}
	if plan.ResolutionKind != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution {
		t.Fatalf("route plan resolution = %s runtime=%v, want runtime instance-key select-or-create", plan.ResolutionKind, plan.RequiresRuntimeResolution)
	}
	if plan.InstanceKey == nil || plan.InstanceKey.Mode != "select-or-create" || strings.Join(plan.InstanceKey.Fields, ",") != templateselectorcreate.TemplateInstanceBy {
		t.Fatalf("route plan instance key = %#v, want select-or-create/account_id", plan.InstanceKey)
	}
	if plan.InstanceKey.OnMissing != "create" || plan.InstanceKey.OnConflict != "reuse" {
		t.Fatalf("route plan lifecycle policy = %s/%s, want create/reuse", plan.InstanceKey.OnMissing, plan.InstanceKey.OnConflict)
	}
	if len(plan.InstanceKey.Mappings) != 1 || plan.InstanceKey.Mappings[0].Source != templateselectorcreate.TemplateInstanceBy || plan.InstanceKey.Mappings[0].Target != templateselectorcreate.TemplateInstanceBy || !plan.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("route plan mappings = %#v, want explicit account_id -> account_id", plan.InstanceKey.Mappings)
	}

	materialized := runtimepinrouting.MaterializeConnectRoutePlan(plan, runtimepinrouting.ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"payload.account_id": "acct-1"},
		Descriptors: []runtimepinrouting.Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "account/one",
			AddressFields: map[string]string{"entity.account_id": "acct-1"},
		}},
	})
	if materialized.Failure != "" {
		t.Fatalf("MaterializeConnectRoutePlan failure = %q, want none", materialized.Failure)
	}
	if materialized.Target.FlowInstance != "account/one" || materialized.Target.EntityID != "ent-1" {
		t.Fatalf("materialized target = %#v, want account/one ent-1", materialized.Target)
	}
}

func TestNotifyAllChildrenConformance_CoversTargetlessFanOutEmitRouteAuthority(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("examples/routing/notify-all-children"))
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("notify-all-children hard invalidities = %#v, want none", got)
	}

	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 2 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want registration and notification plans", plans)
	}
	var plan runtimepinrouting.ConnectRoutePlan
	for _, candidate := range plans {
		if candidate.Source.FlowID == notifyallchildren.OwnerFlowID && candidate.Source.Pin == notifyallchildren.OwnerOutputPin {
			plan = candidate
		}
	}
	if plan.Source.FlowID != notifyallchildren.OwnerFlowID || plan.Source.Pin != notifyallchildren.OwnerOutputPin || plan.Source.Key != "account_id" {
		t.Fatalf("route plan source = %#v, want portfolio.account_notify_requested keyed by account_id", plan.Source)
	}
	if plan.Receiver.FlowID != notifyallchildren.ChildFlowID || plan.Receiver.Pin != notifyallchildren.ChildInputPin || plan.Receiver.Mode != "template" {
		t.Fatalf("route plan receiver = %#v, want account.account_notify_requested template", plan.Receiver)
	}
	if plan.InstanceKey == nil || plan.InstanceKey.Mode != "select" || strings.Join(plan.InstanceKey.Fields, ",") != "account_id" {
		t.Fatalf("route plan instance key = %#v, want select/account_id", plan.InstanceKey)
	}

	handler, ok := source.NodeEventHandler("portfolio-coordinator", notifyallchildren.OwnerTriggerEvent)
	if !ok {
		t.Fatal("portfolio-coordinator notify handler missing")
	}
	exec, err := runtimeengine.NewExecutor(runtimeengine.RuntimeDependencies{
		Source:     source,
		StateRepo:  fanOutPinRouteStateRepo{},
		TxRunner:   fanOutPinRouteTxRunner{},
		Locker:     fanOutPinRouteLocker{},
		Outbox:     fanOutPinRouteOutbox{},
		Dispatcher: fanOutPinRouteDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor: %v", err)
	}
	parent := eventtest.RootIngress(
		"evt-notify-all-children-parent",
		events.EventType("portfolio/portfolio.notify.requested"),
		"",
		"",
		json.RawMessage(`{"portfolio_id":"portfolio","command":"refresh"}`),
		0,
		"run-notify-all-children",
		"",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID:       "portfolio",
			FlowInstance: "portfolio",
			EntityID:     "portfolio",
		}),
		time.Now().UTC(),
	)
	result, err := exec.Execute(testAuthorActivityContext(context.Background()), runtimeengine.ExecutionRequest{
		EntityID: "portfolio",
		NodeID:   "portfolio-coordinator",
		FlowID:   "portfolio",
		Event:    parent,
		Handler:  handler,
		State: runtimeengine.StateSnapshot{
			EntityID:     "portfolio",
			CurrentState: "active",
			StateCarrier: runtimeengine.NewStateCarrier(map[string]any{"account_ids": []any{"acct-a", "acct-b"}}, nil, nil),
		},
	})
	if err != nil {
		t.Fatalf("Execute fan_out: %v", err)
	}
	if result.Status != runtimeengine.OutcomeFannedOut || result.FanOutCount != 2 || len(result.EmitIntents) != 2 {
		t.Fatalf("fan_out result = status:%s count:%d intents:%d", result.Status, result.FanOutCount, len(result.EmitIntents))
	}

	store := &fanOutPinRouteMemoryStore{
		flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{
			{InstanceID: "acct-a", EntityID: "ent-a", FlowInstance: "account/acct-a", FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-a"}},
			{InstanceID: "acct-b", EntityID: "ent-b", FlowInstance: "account/acct-b", FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-b"}},
		},
	}
	eb, err := newScopedTestEventBus(t, store, runtimebus.EventBusOptions{
		ContractBundle: source,
		TemplateInstanceActivator: func(context.Context, runtimepipeline.FlowInstanceActivationRequest) error {
			t.Fatal("existing account route descriptors should satisfy fan-out delivery")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	for _, instanceID := range []string{"acct-a", "acct-b"} {
		if err := eb.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
			Identity: runtimeflowidentity.StoredRoute("account", instanceID, "account/"+instanceID),
		}); err != nil {
			t.Fatalf("AddFlowInstanceRoute(%s): %v", instanceID, err)
		}
	}

	want := map[string]events.RouteIdentity{
		"acct-a": {FlowID: "account", FlowInstance: "account/acct-a", EntityID: "ent-a"},
		"acct-b": {FlowID: "account", FlowInstance: "account/acct-b", EntityID: "ent-b"},
	}
	for idx, intent := range result.EmitIntents {
		evt := eventtest.Child(
			"evt-notify-all-children-child-"+string(rune('a'+idx)),
			intent.Event.Type(),
			intent.Event.SourceAgent(),
			intent.Event.TaskID(),
			intent.Event.Payload(),
			intent.Event.ChainDepth(),
			parent,
			intent.Event.Envelope(),
			intent.Event.CreatedAt(),
		)
		if got, wantType := string(evt.Type()), "portfolio/account.notify.requested"; got != wantType {
			t.Fatalf("fan_out emitted event type = %q, want %q", got, wantType)
		}
		if target := evt.TargetRoute(); !target.Empty() {
			t.Fatalf("engine fan_out emit pre-populated target = %#v, want EventBus RoutePlan ownership", target)
		}
		payload := map[string]any{}
		if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
			t.Fatalf("fan_out payload json: %v", err)
		}
		accountID, _ := payload["account_id"].(string)
		expected, ok := want[accountID]
		if !ok {
			t.Fatalf("unexpected account_id in fan_out payload: %#v", payload)
		}
		preflight, err := eb.CheckPublishRecipientPlan(testAuthorActivityContext(context.Background()), evt)
		if err != nil {
			t.Fatalf("CheckPublishRecipientPlan(%s): %v", accountID, err)
		}
		if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 ||
			!fanOutPinRouteDeliveryRoutesContain(preflight.DeliveryRoutes, expected) {
			t.Fatalf("preflight for %s = failure:%q routes:%#v, want only %#v", accountID, preflight.TargetFailure, preflight.DeliveryRoutes, expected)
		}
		if err := eb.Publish(testAuthorActivityContext(context.Background()), evt); err != nil {
			t.Fatalf("Publish fan_out event for %s: %v", accountID, err)
		}
		if routes := store.deliveryRoutes[evt.ID()]; len(routes) != 1 ||
			!fanOutPinRouteDeliveryRoutesContain(routes, expected) {
			t.Fatalf("persisted routes for %s = %#v, want only %#v", accountID, routes, expected)
		}
	}
}

func TestNotifyAllChildrenConformance_FailsClosedForRouteKeyGaps(t *testing.T) {
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
	tests := []struct {
		name          string
		payload       json.RawMessage
		flowInstances []runtimebus.ActiveFlowInstanceDescriptor
		wantFailure   string
	}{
		{
			name:        "missing account key",
			payload:     json.RawMessage(`{"portfolio_id":"portfolio","command":"refresh"}`),
			wantFailure: string(runtimepinrouting.ConnectFailureAddressValueMissing),
		},
		{
			name:    "ambiguous account key",
			payload: json.RawMessage(`{"portfolio_id":"portfolio","account_id":"acct-a","command":"refresh"}`),
			flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{
				{InstanceID: "acct-a-one", EntityID: "ent-a1", FlowInstance: "account/acct-a-one", FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-a"}},
				{InstanceID: "acct-a-two", EntityID: "ent-a2", FlowInstance: "account/acct-a-two", FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-a"}},
			},
			wantFailure: string(runtimepinrouting.ConnectFailureTargetAmbiguous),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fanOutPinRouteMemoryStore{flowInstances: tc.flowInstances}
			eb, err := newScopedTestEventBus(t, store, runtimebus.EventBusOptions{
				ContractBundle: source,
				TemplateInstanceActivator: func(context.Context, runtimepipeline.FlowInstanceActivationRequest) error {
					t.Fatal("fail-closed fan-out route should not activate an account instance")
					return nil
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			evt := eventtest.RootIngress(
				"evt-notify-all-children-negative",
				events.EventType("portfolio/account.notify.requested"),
				"",
				"",
				tc.payload,
				0,
				"run-notify-all-children",
				"",
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
					FlowID:       "portfolio",
					FlowInstance: "portfolio",
					EntityID:     "portfolio",
				}),
				time.Now().UTC(),
			)
			preflight, err := eb.CheckPublishRecipientPlan(testAuthorActivityContext(context.Background()), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if preflight.TargetFailure != tc.wantFailure {
				t.Fatalf("target failure = %q, want %q", preflight.TargetFailure, tc.wantFailure)
			}
			if len(preflight.DeliveryRoutes) != 0 || len(preflight.Recipients) != 0 ||
				len(preflight.RoutedRecipients) != 0 || len(preflight.SubscriptionRecipients) != 0 {
				t.Fatalf("fail-closed fan-out route exposed executable recipients: routes=%#v recipients=%#v routed=%#v subscriptions=%#v",
					preflight.DeliveryRoutes, preflight.Recipients, preflight.RoutedRecipients, preflight.SubscriptionRecipients)
			}
		})
	}
}

func templateFlowPilotConformanceFindingContains(findings []runtimebootverify.Finding, checkID, substr string) bool {
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

type fanOutPinRouteStateRepo struct{}

func (fanOutPinRouteStateRepo) LoadState(context.Context, runtimeidentity.EntityID) (runtimeengine.StateSnapshot, bool, error) {
	return runtimeengine.StateSnapshot{}, false, nil
}

func (fanOutPinRouteStateRepo) SaveState(context.Context, runtimeidentity.EntityID, runtimeengine.StateMutation) error {
	return nil
}

type fanOutPinRouteTxRunner struct{}
type fanOutPinRouteTx struct{ ctx context.Context }

func (fanOutPinRouteTxRunner) Run(ctx context.Context, fn func(runtimeengine.Tx) error) error {
	return fn(fanOutPinRouteTx{ctx: ctx})
}

func (t fanOutPinRouteTx) Context() context.Context {
	if t.ctx == nil {
		return testAuthorActivityContext(context.Background())
	}
	return t.ctx
}

type fanOutPinRouteLocker struct{}

func (fanOutPinRouteLocker) WithEntityLock(ctx context.Context, _ runtimeidentity.EntityID, fn func(context.Context) error) error {
	return fn(ctx)
}

type fanOutPinRouteOutbox struct{}

func (fanOutPinRouteOutbox) WriteOutbox(context.Context, []runtimeengine.EmitIntent) error {
	return nil
}

type fanOutPinRouteDispatcher struct{}

func (fanOutPinRouteDispatcher) DispatchPostCommit(context.Context, []runtimeengine.EmitIntent) error {
	return nil
}

type fanOutPinRouteMemoryStore struct {
	runtimebus.InMemoryEventStore
	flowInstances  []runtimebus.ActiveFlowInstanceDescriptor
	deliveryRoutes map[string][]events.DeliveryRoute
}

func (s *fanOutPinRouteMemoryStore) ListActiveFlowInstanceDescriptors(context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	return append([]runtimebus.ActiveFlowInstanceDescriptor(nil), s.flowInstances...), nil
}

func (s *fanOutPinRouteMemoryStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return runtimebustest.CommitPublish(ctx, plan, nil, func(_ context.Context, req runtimebus.CommitPublishRequest) error {
		if s.deliveryRoutes == nil {
			s.deliveryRoutes = map[string][]events.DeliveryRoute{}
		}
		s.deliveryRoutes[req.Event.ID()] = events.NormalizeDeliveryRoutes(req.DeliveryRoutes)
		return nil
	})
}

func fanOutPinRouteDeliveryRoutesContain(routes []events.DeliveryRoute, target events.RouteIdentity) bool {
	target = target.Normalized()
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.SubscriberType == "node" && route.SubscriberID == "account-node" && route.Target == target {
			return true
		}
	}
	return false
}
