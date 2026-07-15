package bus

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
	"github.com/google/uuid"
)

type finalFlowInstanceAuthoringLifecycleStore struct {
	*targetRouteMemoryStore
	bus                         *EventBus
	flowInstances               []ActiveFlowInstanceDescriptor
	flowInstanceDescriptorCalls int
	activations                 []runtimepipeline.FlowInstanceActivationRequest
}

func (s *finalFlowInstanceAuthoringLifecycleStore) ListActiveFlowInstanceDescriptors(context.Context) ([]ActiveFlowInstanceDescriptor, error) {
	s.flowInstanceDescriptorCalls++
	return append([]ActiveFlowInstanceDescriptor(nil), s.flowInstances...), nil
}

func (s *finalFlowInstanceAuthoringLifecycleStore) Activate(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	s.activations = append(s.activations, req)
	accountID, _ := req.Metadata[finalflowinstanceauthoring.TemplateInstanceBy].(string)
	s.flowInstances = append(s.flowInstances, ActiveFlowInstanceDescriptor{
		InstanceID:    req.Instance.InstanceID,
		EntityID:      req.Instance.EntityID,
		FlowInstance:  req.Instance.InstancePath,
		FlowTemplate:  req.Instance.TemplateID,
		AddressFields: map[string]string{"entity.account_id": accountID},
	})
	if s.bus == nil {
		return nil
	}
	return s.bus.AddFlowInstanceRouteContext(ctx, FlowInstanceRouteMaterializationRequest{
		Identity: req.Instance.Route(),
	})
}

func TestEventBusFinalFlowInstanceAuthoringFixture_RenamedConnectRoutePersistsReplayableTemplateTarget(t *testing.T) {
	source := finalflowinstanceauthoring.LoadSource(t, finalflowinstanceauthoring.Options{})
	store := &finalFlowInstanceAuthoringLifecycleStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb

	evt := finalFlowInstanceAuthoringAccountReadyEvent(uuid.NewString(), "acct-42")
	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(store.activations) != 0 {
		t.Fatalf("preflight activations = %d, want none", len(store.activations))
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight failure/routes = %q/%#v, want one deterministic template route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if !preflight.UsesCanonicalRouteAuthority() {
		t.Fatalf("preflight route authority is not canonical connect-route authority")
	}
	preview := preflight.DeliveryRoutes[0].Target
	if preview.FlowID != finalflowinstanceauthoring.TemplateFlowID || preview.FlowInstance == "" || preview.EntityID == "" {
		t.Fatalf("preflight target = %#v, want %s template route", preview, finalflowinstanceauthoring.TemplateFlowID)
	}
	if routes := eb.RouteTable().MaterializedRoutes(runtimeflowidentity.StoredRoute(finalflowinstanceauthoring.TemplateFlowID, runtimeflowidentity.LogicalInstanceID(preview.FlowInstance), preview.FlowInstance)); len(routes) != 0 {
		t.Fatalf("preflight leaked materialized route table state: %#v", routes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish account ready: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	if activation.Config[finalflowinstanceauthoring.TemplateInstanceBy] != "acct-42" ||
		activation.Metadata[finalflowinstanceauthoring.TemplateInstanceBy] != "acct-42" {
		t.Fatalf("activation config/metadata = %#v/%#v, want account_id from receiver carry", activation.Config, activation.Metadata)
	}
	if activation.Metadata["entity_type"] != finalflowinstanceauthoring.TemplateEntityType ||
		activation.Metadata["instance_kind"] != "template" ||
		activation.Metadata["last_source_event"] != evt.ID() {
		t.Fatalf("activation metadata = %#v, want entity_type/instance_kind/last_source_event", activation.Metadata)
	}
	persistedRoutes := store.routes[evt.ID()]
	if len(persistedRoutes) != 1 || persistedRoutes[0].SubscriberID != finalflowinstanceauthoring.TemplateNodeID {
		t.Fatalf("persisted delivery routes = %#v, want one %s subscriber", persistedRoutes, finalflowinstanceauthoring.TemplateNodeID)
	}
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   persistedRoutes[0].SubscriberID,
		Target: events.RouteIdentity{
			FlowID:       finalflowinstanceauthoring.TemplateFlowID,
			FlowInstance: activation.Instance.InstancePath,
			EntityID:     activation.Instance.EntityID,
		},
	}
	if !deliveryRoutesContain(persistedRoutes, want) {
		t.Fatalf("persisted delivery routes = %#v, want lifecycle-created template route %#v", persistedRoutes, want)
	}
	if got := store.scopes[evt.ID()]; got != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("post-publish planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("post-publish route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}

	retryTarget := eb.SubscribeInternal(persistedRoutes[0].SubscriberID)
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish same-event retry: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("same-event retry activations = %d, want committed replay without second activation", len(store.activations))
	}
	requireNoBusEvent(t, retryTarget, "same-event publish retry")

	store.flowInstances = []ActiveFlowInstanceDescriptor{{
		InstanceID:    "drift",
		EntityID:      "ent-drift",
		FlowInstance:  finalflowinstanceauthoring.TemplateFlowID + "/drift",
		FlowTemplate:  finalflowinstanceauthoring.TemplateFlowID,
		AddressFields: map[string]string{"entity.account_id": "acct-42"},
	}}
	store.flowInstanceDescriptorCalls = 0
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{
		Identity: runtimeflowidentity.DeriveRoute(finalflowinstanceauthoring.TemplateFlowID, "drift"),
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(drift): %v", err)
	}
	if err := eb.PublishPersistedRecipients(context.Background(), evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients: %v", err)
	}
	replayed := requireBusEvent(t, retryTarget, "persisted replay after descriptor drift")
	if replayed.FlowInstance() != activation.Instance.InstancePath || replayed.EntityID() != activation.Instance.EntityID {
		t.Fatalf("drift replay target = flow_instance:%q entity:%q, want persisted %q/%q",
			replayed.FlowInstance(), replayed.EntityID(), activation.Instance.InstancePath, activation.Instance.EntityID)
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("replay descriptor calls = %d, want 0 because persisted delivery target route is authoritative", got)
	}
}

func TestEventBusFinalFlowInstanceAuthoringFixture_FailsClosedForMissingAndAmbiguousKeys(t *testing.T) {
	source := finalflowinstanceauthoring.LoadSource(t, finalflowinstanceauthoring.Options{})
	tests := []struct {
		name          string
		payload       json.RawMessage
		flowInstances []ActiveFlowInstanceDescriptor
		wantFailure   string
	}{
		{
			name:        "missing renamed producer key",
			payload:     json.RawMessage(`{"score":"91","decision":"approved"}`),
			wantFailure: string(runtimepinrouting.ConnectFailureAddressValueMissing),
		},
		{
			name:    "ambiguous receiver key",
			payload: json.RawMessage(`{"account_id":"acct-42","score":"91","decision":"approved"}`),
			flowInstances: []ActiveFlowInstanceDescriptor{
				{InstanceID: "one", EntityID: "ent-1", FlowInstance: finalflowinstanceauthoring.TemplateFlowID + "/one", FlowTemplate: finalflowinstanceauthoring.TemplateFlowID, AddressFields: map[string]string{"entity.account_id": "acct-42"}},
				{InstanceID: "two", EntityID: "ent-2", FlowInstance: finalflowinstanceauthoring.TemplateFlowID + "/two", FlowTemplate: finalflowinstanceauthoring.TemplateFlowID, AddressFields: map[string]string{"entity.account_id": "acct-42"}},
			},
			wantFailure: string(runtimepinrouting.ConnectFailureTargetAmbiguous),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &finalFlowInstanceAuthoringLifecycleStore{
				targetRouteMemoryStore: newTargetRouteMemoryStore(),
				flowInstances:          tc.flowInstances,
			}
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: source,
				TemplateInstanceActivator: func(context.Context, runtimepipeline.FlowInstanceActivationRequest) error {
					t.Fatal("fail-closed route must not activate a template instance")
					return nil
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			raw := eb.Subscribe("raw-source-listener", events.EventType("producer/account.ready"), events.EventType("account.ready"))
			defer eb.Unsubscribe("raw-source-listener")
			evt := eventtest.RootIngress(uuid.NewString(),
				events.EventType("producer/account.ready"),
				"",
				"",
				tc.payload,
				0,
				"",
				"",
				events.EventEnvelope{},
				time.Now().UTC(),
			)

			plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if plan.TargetFailure != tc.wantFailure {
				t.Fatalf("target failure = %q, want %q", plan.TargetFailure, tc.wantFailure)
			}
			if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
				len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("fail-closed route exposed executable plan: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
					plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
			}
			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish fail-closed event: %v", err)
			}
			if routes := store.routes[evt.ID()]; len(routes) != 0 {
				t.Fatalf("persisted delivery routes = %#v, want none", routes)
			}
			select {
			case got := <-raw:
				t.Fatalf("raw subscriber received fail-closed event with flow_instance=%q entity=%q", got.FlowInstance(), got.EntityID())
			default:
			}
		})
	}
}

func finalFlowInstanceAuthoringAccountReadyEvent(eventID, accountID string) events.Event {
	return eventtest.RootIngress(
		eventID,
		events.EventType("producer/account.ready"),
		"",
		"",
		json.RawMessage(`{"account_id":"`+accountID+`","score":"91","decision":"approved"}`),
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
}
