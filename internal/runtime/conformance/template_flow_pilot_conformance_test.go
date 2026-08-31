package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
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
	if got := instance.Field.Path(); got != "account_id" {
		t.Fatalf("account instance fields = %q, want account_id", got)
	}
	output, ok := bundle.FlowOutputEventPin("producer", "account.ready")
	if !ok {
		t.Fatal("producer account_ready output pin missing")
	}
	if output.EventType() != "account.ready" {
		t.Fatalf("producer output event = %q, want account.ready", output.EventType())
	}

	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one template route plan", plans)
	}
	plan := plans[0]
	sourceEndpoint := plan.SourceEndpoint().Readback()
	receiverEndpoint := plan.ReceiverEndpoint().Readback()
	if plan.ResolutionKind() != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution() {
		t.Fatalf("route plan resolution = %s runtime=%v, want select-or-create runtime resolution", plan.ResolutionKind().Code(), plan.RequiresRuntimeResolution())
	}
	if sourceEndpoint.FlowID != "producer" || sourceEndpoint.Pin != "account.ready" {
		t.Fatalf("route plan source = %#v, want producer.account_ready", sourceEndpoint)
	}
	if receiverEndpoint.FlowID != "account" || receiverEndpoint.Pin != "account.ready" || !plan.ReceiverEndpoint().IsTemplate() {
		t.Fatalf("route plan receiver = %#v, want template account.account_ready", receiverEndpoint)
	}
	if plan.InstanceKey() == nil || plan.InstanceKey().Mode() != runtimecontracts.FlowInputResolutionModeSelectOrCreate || plan.InstanceKey().Field().Path() != "account_id" {
		t.Fatalf("route plan instance key = %#v, want select-or-create/account_id", plan.InstanceKey())
	}
	if key := plan.InstanceKey().Readback(); key.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || key.SourcePath != "payload.account_id" {
		t.Fatalf("route plan instance source = %#v, want payload.account_id", key)
	}
}

func TestTemplateFlowPilotConformance_FailClosedMatrix(t *testing.T) {
	tests := []struct {
		name        string
		opts        templateflowpilot.Options
		checkID     string
		wantMessage string
		loadError   bool
	}{
		{
			name:        "unsupported receiver select_entity on connected normal path",
			opts:        templateflowpilot.Options{UnsupportedReceiverSelection: true},
			checkID:     "redundant_in_topology_select_entity",
			wantMessage: "scalar receiver instance",
		},
		{
			name:        "producer target cannot rescue common composition",
			opts:        templateflowpilot.Options{ProducerTarget: true},
			wantMessage: "RETIRED-EMIT-ROUTING: emit.target",
			loadError:   true,
		},
		{
			name:        "producer broadcast cannot replace parent connect authority",
			opts:        templateflowpilot.Options{ProducerBroadcast: true},
			wantMessage: "RETIRED-EMIT-ROUTING: emit.broadcast",
			loadError:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := templateflowpilot.LoadBundleResult(t, tc.opts)
			if tc.loadError {
				if err == nil || !strings.Contains(err.Error(), tc.wantMessage) {
					t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want containing %q", err, tc.wantMessage)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			source := semanticview.Wrap(bundle)
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

	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	var plan runtimepinrouting.ConnectRoutePlan
	for _, candidate := range plans {
		if candidate.InstanceKey() != nil && candidate.InstanceKey().Mode() == runtimecontracts.FlowInputResolutionModeSelect {
			plan = candidate
		}
	}
	if plan.InstanceKey() == nil {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want select route plan", plans)
	}
	sourceEndpoint := plan.SourceEndpoint().Readback()
	receiverEndpoint := plan.ReceiverEndpoint().Readback()
	if sourceEndpoint.FlowID != templateselectexisting.ProducerFlowID || sourceEndpoint.Pin != templateselectexisting.ProducerOutputPin {
		t.Fatalf("route plan source = %#v, want %s.%s", sourceEndpoint, templateselectexisting.ProducerFlowID, templateselectexisting.ProducerOutputPin)
	}
	if receiverEndpoint.FlowID != templateselectexisting.TemplateFlowID || receiverEndpoint.Pin != templateselectexisting.TemplateInputPin || !plan.ReceiverEndpoint().IsTemplate() {
		t.Fatalf("route plan receiver = %#v, want template %s.%s", receiverEndpoint, templateselectexisting.TemplateFlowID, templateselectexisting.TemplateInputPin)
	}
	if plan.ResolutionKind() != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution() {
		t.Fatalf("route plan resolution = %s runtime=%v, want runtime instance-key select", plan.ResolutionKind().Code(), plan.RequiresRuntimeResolution())
	}
	if plan.InstanceKey() == nil || plan.InstanceKey().Mode() != runtimecontracts.FlowInputResolutionModeSelect || plan.InstanceKey().Field().Path() != templateselectexisting.TemplateInstanceBy {
		t.Fatalf("route plan instance key = %#v, want select/account_id", plan.InstanceKey())
	}
	if key := plan.InstanceKey().Readback(); key.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || key.SourcePath != "payload."+templateselectexisting.TemplateInstanceBy {
		t.Fatalf("route plan source = %#v, want payload.%s", key, templateselectexisting.TemplateInstanceBy)
	}

	materialized := runtimepinrouting.MaterializeConnectRoutePlan(plan, runtimepinrouting.ConnectRoutePlanMaterializationInput{
		MatchValues: runtimepinrouting.AdmitConnectRouteMatchValues(map[string]string{"payload.account_id": "acct-1"}),
		Descriptors: []runtimepinrouting.Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "account/one",
			AddressFields: map[string]string{"entity.account_id": "acct-1"},
		}},
	})
	if !materialized.Failure.Empty() {
		t.Fatalf("MaterializeConnectRoutePlan failure = %q, want none", materialized.Failure.Code())
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

	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one select-or-create route plan", plans)
	}
	plan := plans[0]
	sourceEndpoint := plan.SourceEndpoint().Readback()
	receiverEndpoint := plan.ReceiverEndpoint().Readback()
	if sourceEndpoint.FlowID != templateselectorcreate.ProducerFlowID || sourceEndpoint.Pin != templateselectorcreate.ProducerOutputPin {
		t.Fatalf("route plan source = %#v, want %s.%s", sourceEndpoint, templateselectorcreate.ProducerFlowID, templateselectorcreate.ProducerOutputPin)
	}
	if receiverEndpoint.FlowID != templateselectorcreate.TemplateFlowID || receiverEndpoint.Pin != templateselectorcreate.TemplateInputPin || !plan.ReceiverEndpoint().IsTemplate() {
		t.Fatalf("route plan receiver = %#v, want template %s.%s", receiverEndpoint, templateselectorcreate.TemplateFlowID, templateselectorcreate.TemplateInputPin)
	}
	if plan.ResolutionKind() != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution() {
		t.Fatalf("route plan resolution = %s runtime=%v, want runtime instance-key select-or-create", plan.ResolutionKind().Code(), plan.RequiresRuntimeResolution())
	}
	if plan.InstanceKey() == nil || plan.InstanceKey().Mode() != runtimecontracts.FlowInputResolutionModeSelectOrCreate || plan.InstanceKey().Field().Path() != templateselectorcreate.TemplateInstanceBy {
		t.Fatalf("route plan instance key = %#v, want select-or-create/account_id", plan.InstanceKey())
	}
	if key := plan.InstanceKey().Readback(); key.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || key.SourcePath != "payload."+templateselectorcreate.TemplateInstanceBy {
		t.Fatalf("route plan source = %#v, want payload.%s", key, templateselectorcreate.TemplateInstanceBy)
	}

	materialized := runtimepinrouting.MaterializeConnectRoutePlan(plan, runtimepinrouting.ConnectRoutePlanMaterializationInput{
		MatchValues: runtimepinrouting.AdmitConnectRouteMatchValues(map[string]string{"payload.account_id": "acct-1"}),
		Descriptors: []runtimepinrouting.Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "account/one",
			AddressFields: map[string]string{"entity.account_id": "acct-1"},
		}},
	})
	if !materialized.Failure.Empty() {
		t.Fatalf("MaterializeConnectRoutePlan failure = %q, want none", materialized.Failure.Code())
	}
	if materialized.Target.FlowInstance != "account/one" || materialized.Target.EntityID != "ent-1" {
		t.Fatalf("materialized target = %#v, want account/one ent-1", materialized.Target)
	}
}

func TestNotifyAllChildrenConformance_CoversTargetlessFanOutEmitRouteAuthority(t *testing.T) {
	portfolioEntityID := runtimeflowidentity.EntityID("portfolio")
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("examples/routing/notify-all-children"))
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("notify-all-children hard invalidities = %#v, want none", got)
	}

	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 2 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want registration and notification plans", plans)
	}
	var plan runtimepinrouting.ConnectRoutePlan
	for _, candidate := range plans {
		sourceEndpoint := candidate.SourceEndpoint().Readback()
		if sourceEndpoint.FlowID == notifyallchildren.OwnerFlowID && sourceEndpoint.Pin == notifyallchildren.OwnerOutputPin {
			plan = candidate
		}
	}
	sourceEndpoint := plan.SourceEndpoint().Readback()
	receiverEndpoint := plan.ReceiverEndpoint().Readback()
	if sourceEndpoint.FlowID != notifyallchildren.OwnerFlowID || sourceEndpoint.Pin != notifyallchildren.OwnerOutputPin || sourceEndpoint.PinDigest == "" {
		t.Fatalf("route plan source = %#v, want portfolio.account_notify_requested keyed by account_id", sourceEndpoint)
	}
	if receiverEndpoint.FlowID != notifyallchildren.ChildFlowID || receiverEndpoint.Pin != notifyallchildren.ChildInputPin || !plan.ReceiverEndpoint().IsTemplate() {
		t.Fatalf("route plan receiver = %#v, want account.account_notify_requested template", receiverEndpoint)
	}
	if plan.InstanceKey() == nil || plan.InstanceKey().Mode() != runtimecontracts.FlowInputResolutionModeSelect || plan.InstanceKey().Field().Path() != "account_id" {
		t.Fatalf("route plan instance key = %#v, want select/account_id", plan.InstanceKey())
	}

	portfolioNode := conformanceNode(t, "portfolio", "portfolio-coordinator")
	handler, ok := source.ExecutableNodeEventHandler(portfolioNode, notifyallchildren.OwnerTriggerEvent)
	if !ok {
		t.Fatal("portfolio-coordinator notify handler missing")
	}
	exec, err := runtimeengine.NewExecutor(runtimeengine.RuntimeDependencies{
		Source:        source,
		StateRepo:     fanOutPinRouteStateRepo{},
		MutationOwner: fanOutPinRouteMutationOwner{},
		Locker:        fanOutPinRouteLocker{},
		Dispatcher:    fanOutPinRouteDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor: %v", err)
	}
	parent := eventtest.RunCreatingRootIngress(
		eventtest.UUID("evt-notify-all-children-parent"),
		events.EventType("portfolio/portfolio.notify.requested"),
		"",
		"",
		json.RawMessage(`{"portfolio_id":"portfolio","command":"refresh"}`),
		0,
		eventtest.UUID("run-notify-all-children"),
		"",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID:       "portfolio",
			FlowInstance: "portfolio",
			EntityID:     portfolioEntityID,
		}),
		time.Now().UTC(),
	)
	claim, err := runtimedelivery.AdmitPersistedClaim(
		eventtest.UUID("delivery-notify-all-children"),
		parent.RunID(),
		"notify-all-children:"+portfolioNode.Key(),
		eventtest.UUID("claim-notify-all-children"),
		1,
		runtimedelivery.SubscriberNode,
		portfolioNode.Key(),
	)
	if err != nil {
		t.Fatalf("admit fan-out delivery claim: %v", err)
	}
	executionCtx := runtimedelivery.WithClaim(testAuthorActivityContext(context.Background()), claim)
	result, err := exec.Execute(executionCtx, runtimeengine.ExecutionRequest{
		EntityID:        runtimeidentity.EntityID(portfolioEntityID),
		Node:            portfolioNode,
		ExecutionFlowID: runtimeidentity.FlowID(notifyallchildren.OwnerFlowID),
		Route:           runtimeflowidentity.DeriveRoute(notifyallchildren.OwnerFlowID, parent.RunID()),
		Event:           parent,
		ProducerSource:  parent.RoutingSource(),
		HandlerEventKey: notifyallchildren.OwnerTriggerEvent,
		Handler:         handler,
		FanOutPlans:     source.FanOutPlansForHandler(portfolioNode, notifyallchildren.OwnerTriggerEvent),
		State: runtimeengine.StateSnapshot{
			EntityID:     runtimeidentity.EntityID(portfolioEntityID),
			CurrentState: "active",
			StateCarrier: runtimeengine.NewStateCarrier(map[string]any{"account_ids": []any{"acct-a", "acct-b"}}, nil, nil),
		},
	})
	if err != nil {
		t.Fatalf("Execute fan_out: %v", err)
	}
	if result.Status != runtimeengine.OutcomeFannedOut || result.FanOutCount != 2 || result.FanOutIntent == nil || len(result.EmitIntents) != 0 {
		t.Fatalf("fan_out result = status:%s count:%d obligation:%#v eager:%d", result.Status, result.FanOutCount, result.FanOutIntent, len(result.EmitIntents))
	}
	now := time.Now().UTC()
	intent := fanoutobligation.Intent{
		Request: *result.FanOutIntent, Source: result.FanOutIntent.Source,
		Status: fanoutobligation.StatusOpen, NextChunkSize: fanoutobligation.InitialChunkSize,
		CreatedAt: now, UpdatedAt: now,
	}
	if intent.Source.Kind == fanoutobligation.SourceEntityField {
		intent.Source.MutationID = eventtest.UUID("notify-all-children-source-revision")
	}
	itemIntents := make([]runtimeengine.EmitIntent, 0, result.FanOutCount)
	for ordinal, item := range []any{"acct-a", "acct-b"} {
		emit, evalErr := exec.EvaluateFanOutOrdinal(testAuthorActivityContext(context.Background()), intent, parent, item, ordinal)
		if evalErr != nil {
			t.Fatalf("EvaluateFanOutOrdinal(%d): %v", ordinal, evalErr)
		}
		itemIntents = append(itemIntents, emit)
	}

	accountAEntityID := runtimeflowidentity.EntityID("account/acct-a")
	accountBEntityID := runtimeflowidentity.EntityID("account/acct-b")
	store := &fanOutPinRouteMemoryStore{
		flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{
			{InstanceID: "acct-a", EntityID: accountAEntityID, FlowInstance: "account/acct-a", FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-a"}},
			{InstanceID: "acct-b", EntityID: accountBEntityID, FlowInstance: "account/acct-b", FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-b"}},
		},
		activeAgents: []runtimebus.ActiveAgentDescriptor{
			{Identity: agentidentitytest.Declared(t, "account-worker", "notify-all-children/account", "account", "acct-a", "account/acct-a"), EntityID: accountAEntityID},
			{Identity: agentidentitytest.Declared(t, "account-worker", "notify-all-children/account", "account", "acct-b", "account/acct-b"), EntityID: accountBEntityID},
		},
	}
	eb, err := newScopedTestEventBus(t, store, runtimebus.EventBusOptions{
		ContractBundle: source,
		Durable: runtimebus.DurableDependencies{
			ActiveAgents:      store,
			ActiveFlows:       store,
			FlowRouteTopology: store,
		},
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
	agentDeliveries := make(map[string]<-chan *runtimebus.LocalDelivery, len(store.activeAgents))
	for _, descriptor := range store.activeAgents {
		admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(source, semanticview.FlowOwnedAgentSubscriptionRequest{
			AgentID:       descriptor.Identity.AgentID(),
			FlowID:        "account",
			FlowPath:      descriptor.Identity.FlowInstance(),
			Subscriptions: []string{"account.notify.requested"},
		})
		if err != nil {
			t.Fatalf("AdmitFlowOwnedAgentSubscriptions(%s): %v", descriptor.Identity, err)
		}
		agentDeliveries[descriptor.Identity.FlowInstance()] = runtimebustest.SubscribeIdentity(t, eb, descriptor.Identity, admission.CarrierOnly())
	}

	want := map[string]events.RouteIdentity{
		"acct-a": {FlowID: "account", FlowInstance: "account/acct-a", EntityID: accountAEntityID},
		"acct-b": {FlowID: "account", FlowInstance: "account/acct-b", EntityID: accountBEntityID},
	}
	for idx, intent := range itemIntents {
		evt := eventtest.Child(
			eventtest.UUID("evt-notify-all-children-child-"+string(rune('a'+idx))),
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
		expectedAgent := agentidentitytest.Declared(
			t,
			"account-worker",
			"notify-all-children/account",
			"account",
			accountID,
			"account/"+accountID,
		)
		preflight, err := eb.CheckPublishRecipientPlan(testAuthorActivityContext(context.Background()), evt)
		if err != nil {
			t.Fatalf("CheckPublishRecipientPlan(%s): %v", accountID, err)
		}
		if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 2 ||
			!fanOutPinRouteDeliveryRoutesContain(preflight.DeliveryRoutes, expected, expectedAgent) {
			t.Fatalf("preflight for %s = failure:%q routes:%#v, want node and exact agent at %#v", accountID, preflight.TargetFailure, preflight.DeliveryRoutes, expected)
		}
		if err := eb.Publish(testAuthorActivityContext(context.Background()), evt); err != nil {
			t.Fatalf("Publish fan_out event for %s: %v", accountID, err)
		}
		select {
		case delivery := <-agentDeliveries[expectedAgent.FlowInstance()]:
			if err := delivery.Complete(); err != nil {
				t.Fatalf("complete exact account agent delivery for %s: %v", accountID, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for exact account agent delivery for %s", accountID)
		}
		if routes := store.deliveryRoutes[evt.ID()]; len(routes) != 2 ||
			!fanOutPinRouteDeliveryRoutesContain(routes, expected, expectedAgent) {
			t.Fatalf("persisted routes for %s = %#v, want node and exact agent at %#v", accountID, routes, expected)
		}
	}
}

func TestNotifyAllChildrenConformance_FailsClosedForRouteKeyGaps(t *testing.T) {
	portfolioEntityID := runtimeflowidentity.EntityID("portfolio")
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
	tests := []struct {
		name               string
		payload            json.RawMessage
		flowInstances      []runtimebus.ActiveFlowInstanceDescriptor
		wantFailure        string
		wantAdmissionError string
	}{
		{
			name:               "missing account key",
			payload:            json.RawMessage(`{"command":"refresh"}`),
			wantAdmissionError: "$.account_id is required",
		},
		{
			name:    "ambiguous account key",
			payload: json.RawMessage(`{"account_id":"acct-a","command":"refresh"}`),
			flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{
				{InstanceID: "acct-a-one", EntityID: runtimeflowidentity.EntityID("ent-a1"), FlowInstance: "account/acct-a-one", FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-a"}},
				{InstanceID: "acct-a-two", EntityID: runtimeflowidentity.EntityID("ent-a2"), FlowInstance: "account/acct-a-two", FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-a"}},
			},
			wantFailure: runtimepinrouting.ConnectFailureTargetAmbiguous.Code(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fanOutPinRouteMemoryStore{flowInstances: tc.flowInstances}
			eb, err := newScopedTestEventBus(t, store, runtimebus.EventBusOptions{
				ContractBundle: source,
				Durable: runtimebus.DurableDependencies{
					ActiveAgents: store,
					ActiveFlows:  store,
				},
				TemplateInstanceActivator: func(context.Context, runtimepipeline.FlowInstanceActivationRequest) error {
					t.Fatal("fail-closed fan-out route should not activate an account instance")
					return nil
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			evt := eventtest.RunCreatingRootIngress(
				eventtest.UUID("evt-notify-all-children-negative-"+tc.name),
				events.EventType("portfolio/account.notify.requested"),
				"",
				"",
				tc.payload,
				0,
				eventtest.UUID("run-notify-all-children"),
				"",
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
					FlowID:       "portfolio",
					FlowInstance: "portfolio",
					EntityID:     portfolioEntityID,
				}),
				time.Now().UTC(),
			)
			preflight, err := eb.CheckPublishRecipientPlan(testAuthorActivityContext(context.Background()), evt)
			if tc.wantAdmissionError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantAdmissionError) {
					t.Fatalf("CheckPublishRecipientPlan error = %v, want %q", err, tc.wantAdmissionError)
				}
				return
			}
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

func (fanOutPinRouteStateRepo) LoadState(context.Context, runtimeengine.StateAddress) (runtimeengine.StateSnapshot, bool, error) {
	return runtimeengine.StateSnapshot{}, false, nil
}

func (fanOutPinRouteStateRepo) SaveState(context.Context, runtimeengine.StateAddress, runtimeengine.StateMutation) error {
	return nil
}

type fanOutPinRouteMutationOwner struct{}

func (fanOutPinRouteMutationOwner) CommitEngineMutation(_ context.Context, mutation runtimeengine.EngineMutation) (runtimeengine.CommittedEngineMutation, error) {
	return runtimeengine.CommittedEngineMutation{EmitIntents: mutation.EmitIntents, ActivityIntents: mutation.ActivityIntents}, nil
}

type fanOutPinRouteLocker struct{}

func (fanOutPinRouteLocker) WithEntityLock(ctx context.Context, _ runtimeidentity.EntityID, fn func(context.Context) error) error {
	return fn(ctx)
}

type fanOutPinRouteDispatcher struct{}

func (fanOutPinRouteDispatcher) DispatchPostCommit(context.Context, []runtimeengine.EmitIntent) error {
	return nil
}

type fanOutPinRouteMemoryStore struct {
	runtimebus.InMemoryEventStore
	flowInstances  []runtimebus.ActiveFlowInstanceDescriptor
	activeAgents   []runtimebus.ActiveAgentDescriptor
	deliveryRoutes map[string][]events.DeliveryRoute
}

func (s *fanOutPinRouteMemoryStore) ReplaceFlowInstanceRouteTopology(_ context.Context, sets []runtimebus.FlowInstanceRouteRecordSet) error {
	for _, set := range sets {
		if !set.Identity.Valid() {
			return fmt.Errorf("invalid flow-instance route identity: %#v", set.Identity)
		}
	}
	return nil
}

func (s *fanOutPinRouteMemoryStore) ListActiveFlowInstanceDescriptors(context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	bundleHash, bundleSource := authorActivityTestBundleSourceFact.StorageValues()
	descriptors := append([]runtimebus.ActiveFlowInstanceDescriptor(nil), s.flowInstances...)
	for i := range descriptors {
		if descriptors[i].BundleHash == "" {
			descriptors[i].BundleHash = bundleHash
		}
		if descriptors[i].BundleSource == "" {
			descriptors[i].BundleSource = bundleSource
		}
		if descriptors[i].WorkflowVersion == "" {
			descriptors[i].WorkflowVersion = "1.0.0"
		}
	}
	return descriptors, nil
}

func (s *fanOutPinRouteMemoryStore) ListActiveAgentDescriptors(context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	return append([]runtimebus.ActiveAgentDescriptor(nil), s.activeAgents...), nil
}

func (s *fanOutPinRouteMemoryStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublish(ctx, command, nil, func(_ context.Context, req runtimebus.CommitPublishRequest) error {
		if s.deliveryRoutes == nil {
			s.deliveryRoutes = map[string][]events.DeliveryRoute{}
		}
		s.deliveryRoutes[req.Event.ID()] = events.NormalizeDeliveryRoutes(req.DeliveryRoutes)
		return nil
	})
}

func fanOutPinRouteDeliveryRoutesContain(routes []events.DeliveryRoute, target events.RouteIdentity, agentIdentity agentidentity.Identity) bool {
	target = target.Normalized()
	accountNode, err := runtimeidentity.AdmitExecutableNodeDeclaration(runtimeidentity.RootPackageKey, "account", "account-node")
	if err != nil {
		return false
	}
	nodeFound := false
	agentFound := false
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.Recipient.IsNode() && route.Recipient.ID() == accountNode.Key() && route.Target.Route() == target {
			nodeFound = true
		}
		if route.Recipient.IsAgent() && route.Recipient.ID() == "account-worker" &&
			route.AgentIdentity == agentIdentity && route.Target.Route() == target {
			agentFound = true
		}
	}
	return nodeFound && agentFound
}
