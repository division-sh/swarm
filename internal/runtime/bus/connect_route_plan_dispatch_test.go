package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type connectRoutePlanTestFlow struct {
	id           string
	mode         string
	inputs       []runtimecontracts.FlowInputEventPin
	outputs      []runtimecontracts.FlowOutputEventPin
	nodes        map[string]runtimecontracts.SystemNodeContract
	entityFields map[string]runtimecontracts.EntityFieldDecl
}

type connectRoutePlanDescriptorStore struct {
	*targetRouteMemoryStore
	flowInstances               []ActiveFlowInstanceDescriptor
	flowInstanceDescriptorCalls int
}

type connectRoutePlanFailingAgentDescriptorStore struct {
	*targetRouteMemoryStore
}

type connectRoutePlanMutationStore struct {
	*targetRouteMemoryStore
}

type connectRoutePlanLifecycleStore struct {
	*connectRoutePlanDescriptorStore
	bus         *EventBus
	activations []runtimepipeline.FlowInstanceActivationRequest
}

type targetRouteMemoryEventMutation struct {
	ctx   context.Context
	store *connectRoutePlanMutationStore
}

func (s *connectRoutePlanMutationStore) RunEventMutation(ctx context.Context, fn func(EventMutation) error) error {
	mutation := &targetRouteMemoryEventMutation{store: s}
	mutation.ctx = WithEventMutationContext(ctx, mutation)
	return fn(mutation)
}

func (m *targetRouteMemoryEventMutation) Context() context.Context {
	if m == nil || m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func (m *targetRouteMemoryEventMutation) AppendEvent(ctx context.Context, evt events.Event) error {
	return m.store.AppendEvent(ctx, evt)
}

func (m *targetRouteMemoryEventMutation) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	return m.store.InsertEventDeliveries(ctx, eventID, agentIDs)
}

func (m *targetRouteMemoryEventMutation) InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	return m.InsertEventDeliveryRoutes(ctx, eventID, deliveryRoutesFromTargetMap(agentIDs, routePlanSubscriberAgent, deliveryTargets))
}

func (m *targetRouteMemoryEventMutation) InsertEventDeliveryRoutes(_ context.Context, eventID string, deliveryRoutes []events.DeliveryRoute) error {
	if m.store.routes == nil {
		m.store.routes = map[string][]events.DeliveryRoute{}
	}
	m.store.routes[eventID] = events.NormalizeDeliveryRoutes(deliveryRoutes)
	return nil
}

func (m *targetRouteMemoryEventMutation) UpsertCommittedReplayScope(_ context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	m.store.scopes[eventID] = scope
	return nil
}

func (m *targetRouteMemoryEventMutation) UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
	return m.store.UpsertPipelineReceipt(ctx, eventID, status, errText)
}

func (*targetRouteMemoryEventMutation) RecordDeadLetter(context.Context, runtimedeadletters.Record) error {
	return nil
}

func (s *connectRoutePlanDescriptorStore) ListActiveFlowInstanceDescriptors(context.Context) ([]ActiveFlowInstanceDescriptor, error) {
	s.flowInstanceDescriptorCalls++
	return append([]ActiveFlowInstanceDescriptor(nil), s.flowInstances...), nil
}

func (s *connectRoutePlanLifecycleStore) Activate(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	for _, descriptor := range s.flowInstances {
		descriptor = descriptor.Normalized()
		if descriptor.InstanceID == req.Instance.InstanceID || descriptor.FlowInstance == req.Instance.InstancePath {
			return errors.New("flow instance already exists")
		}
	}
	s.activations = append(s.activations, req)
	s.flowInstances = append(s.flowInstances, ActiveFlowInstanceDescriptor{
		InstanceID:    req.Instance.InstanceID,
		EntityID:      req.Instance.EntityID,
		FlowInstance:  req.Instance.InstancePath,
		FlowTemplate:  req.Instance.TemplateID,
		AddressFields: connectRoutePlanActivationAddressFields(req.Metadata),
	})
	if s.bus == nil {
		return nil
	}
	return s.bus.AddFlowInstanceRouteContext(ctx, FlowInstanceRouteMaterializationRequest{
		Identity:            req.Instance.Route(),
		ActivationVariables: connectRoutePlanActivationVariables(req),
	})
}

func connectRoutePlanActivationAddressFields(metadata map[string]any) map[string]string {
	out := map[string]string{}
	for key, raw := range metadata {
		key = strings.TrimSpace(key)
		if key == "" || key == "entity_type" || key == "instance_kind" || key == "last_source_event" {
			continue
		}
		value := strings.TrimSpace(fmt.Sprint(raw))
		if value != "" {
			out["entity."+key] = value
		}
	}
	return out
}

func connectRoutePlanActivationVariables(req runtimepipeline.FlowInstanceActivationRequest) map[string]string {
	out := map[string]string{}
	for _, values := range []map[string]any{req.Config, req.Metadata} {
		for key, raw := range values {
			key = strings.TrimSpace(key)
			value := strings.TrimSpace(fmt.Sprint(raw))
			if key != "" && value != "" {
				out[key] = value
			}
		}
	}
	return out
}

func (s *targetRouteMemoryStore) UpsertCommittedReplayScope(_ context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	if s.scopes == nil {
		s.scopes = map[string]runtimereplayclaim.CommittedReplayScope{}
	}
	s.scopes[eventID] = scope
	return nil
}

func (s *connectRoutePlanFailingAgentDescriptorStore) ListActiveAgentDescriptors(context.Context) ([]ActiveAgentDescriptor, error) {
	return nil, errors.New("legacy active-agent descriptor path should not run for static connect route plans")
}

func TestEventBusPublish_ConnectRoutePlanPersistsSingularTargetWithoutLiveSubscriber(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From:     "producer.deploy_done",
		To:       "consumer.deploy_completed",
		Delivery: "one",
	})
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	receiverCarriers := eb.RouteTable().Resolve("consumer/deploy.completed")
	if !subscriberListContainsRouteSource(receiverCarriers, "consumer-node", "consumer", "receiver_carrier") {
		t.Fatalf("receiver carrier route consumer/deploy.completed = %#v, want consumer-node receiver_carrier", receiverCarriers)
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"ignored":"yes"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node",
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: "consumer",
			EntityID:     runtimeflowidentity.EntityID("consumer"),
		},
	}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), want) {
		t.Fatalf("route plan delivery routes = %#v, want %#v", routePlan.DeliveryRoutes(), want)
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, want) {
		t.Fatalf("preflight delivery routes = %#v, want %#v", plan.DeliveryRoutes, want)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.scopes[eventID]; got != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	if got := store.receipts[eventID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q, want processed", got)
	}
	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, runtimereplayclaim.CommittedReplayScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if !containsString(live, "consumer-node") || !containsString(internal, "consumer-node") {
		t.Fatalf("replay live=%#v internal=%#v, want consumer-node from persisted connect route", live, internal)
	}
	if !deliveryRoutesContain(replayRoutes, want) {
		t.Fatalf("replay delivery routes = %#v, want %#v", replayRoutes, want)
	}
}

func TestEventBusPublish_RootConnectRoutePlanPersistsSingularTarget(t *testing.T) {
	source := connectRoutePlanRootProducerStaticSource()
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	receiverCarriers := eb.RouteTable().Resolve("consumer/root.ready")
	if !subscriberListContainsRouteSource(receiverCarriers, "consumer-node", "consumer", "receiver_carrier") {
		t.Fatalf("receiver carrier route consumer/root.ready = %#v, want consumer-node receiver_carrier", receiverCarriers)
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("root.ready"), "", "", json.RawMessage(`{"entity_id":"entity-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node",
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: "consumer",
			EntityID:     runtimeflowidentity.EntityID("consumer"),
		},
	}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched root connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), want) {
		t.Fatalf("route plan delivery routes = %#v, want %#v", routePlan.DeliveryRoutes(), want)
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, want) {
		t.Fatalf("preflight delivery routes = %#v, want %#v", plan.DeliveryRoutes, want)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.scopes[eventID]; got != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	if got := store.receipts[eventID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q, want processed", got)
	}
}

func TestEventBusPublish_RootConnectRoutePlanDoesNotCaptureChildScopedSameNameEvent(t *testing.T) {
	source := connectRoutePlanRootProducerStaticSource()
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(
		eventID,
		events.EventType("root.ready"),
		"",
		"",
		json.RawMessage(`{"entity_id":"entity-1"}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, "producer/child-1"),
		time.Now().UTC(),
	)

	forbidden := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node",
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: "consumer",
			EntityID:     runtimeflowidentity.EntityID("consumer"),
		},
	}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityOwner == routePlanSourceConnectRoutePlan || routePlan.AuthorityState == RoutePlanAuthorityCanonicalMatched {
		t.Fatalf("route plan authority = %q/%q, root connect must not match child-scoped same-name event", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if deliveryRoutesContain(routePlan.DeliveryRoutes(), forbidden) {
		t.Fatalf("route plan delivery routes = %#v, must not include root-connect receiver for child-scoped same-name event", routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if deliveryRoutesContain(plan.DeliveryRoutes, forbidden) {
		t.Fatalf("preflight delivery routes = %#v, must not include root-connect receiver for child-scoped same-name event", plan.DeliveryRoutes)
	}
}

func TestEventBusCheckPublishRecipientPlan_ConnectRoutePlanPrecedesLegacyDescriptorPolicy(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From:     "producer.deploy_done",
		To:       "consumer.deploy_completed",
		Delivery: "one",
	})
	store := &connectRoutePlanFailingAgentDescriptorStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, connectRoutePlanStaticDeliveryRoute()) {
		t.Fatalf("preflight delivery routes = %#v, want static connect route", plan.DeliveryRoutes)
	}
}

func TestConnectRoutePlanReceiverCarrierKeysUseSelectedReceiverIdentity(t *testing.T) {
	plan := runtimepinrouting.ConnectRoutePlan{
		Source: runtimepinrouting.ConnectRoutePlanEndpoint{
			FlowID:        "producer",
			FlowPath:      "producer",
			Event:         "deploy.done",
			ResolvedEvent: "producer/deploy.done",
		},
		Receiver: runtimepinrouting.ConnectRoutePlanEndpoint{
			FlowID:        "consumer",
			FlowPath:      "consumer",
			Event:         "deploy.completed",
			ResolvedEvent: "consumer/deploy.completed",
		},
	}
	target := events.RouteIdentity{
		FlowID:       "consumer",
		FlowInstance: "consumer/alpha",
		EntityID:     "entity-alpha",
	}

	keys := connectReceiverCarrierRouteKeys(plan, target)
	for _, want := range []string{"consumer/alpha/deploy.completed", "consumer/deploy.completed"} {
		if !containsString(keys, want) {
			t.Fatalf("carrier keys = %#v, want selected receiver key %q", keys, want)
		}
	}
	for _, forbidden := range []string{"producer/deploy.done", "deploy.done"} {
		if containsString(keys, forbidden) {
			t.Fatalf("carrier keys = %#v, must not include source endpoint key %q", keys, forbidden)
		}
	}
	if got, want := len(keys), len(uniqueStrings(keys)); got != want {
		t.Fatalf("carrier keys = %#v, want unique selected receiver keys", keys)
	}
}

func TestConnectRoutePlanDescriptorsLoadOnlyForRuntimeResolution(t *testing.T) {
	calls := 0
	resolver := connectRoutePlanResolver{
		loadDescriptors: func(context.Context) ([]runtimepinrouting.Descriptor, error) {
			calls++
			return []runtimepinrouting.Descriptor{{ID: "alpha", EntityID: "team-a", FlowInstance: "worker/alpha"}}, nil
		},
	}

	if _, err := resolver.descriptorsForPlans(context.Background(), []runtimepinrouting.ConnectRoutePlan{{
		RequiresRuntimeResolution: false,
	}}); err != nil {
		t.Fatalf("descriptorsForPlans static: %v", err)
	}
	if calls != 0 {
		t.Fatalf("descriptor loader calls after static plan = %d, want 0", calls)
	}

	if _, err := resolver.descriptorsForPlans(context.Background(), []runtimepinrouting.ConnectRoutePlan{{
		RequiresRuntimeResolution: true,
	}}); err != nil {
		t.Fatalf("descriptorsForPlans runtime: %v", err)
	}
	if calls != 1 {
		t.Fatalf("descriptor loader calls after runtime-resolution plan = %d, want 1", calls)
	}
}

func TestEventBusPublishInMutation_ConnectRoutePlanPersistsSharedRoutePlan(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From:     "producer.deploy_done",
		To:       "consumer.deploy_completed",
		Delivery: "one",
	})
	store := &connectRoutePlanMutationStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := connectRoutePlanStaticDeliveryRoute()
	postCommitActions := make([]func(), 0, 1)
	ctx := runtimepipeline.WithPipelinePostCommitActions(context.Background(), &postCommitActions)

	if err := store.RunEventMutation(ctx, func(mutation EventMutation) error {
		return eb.PublishInMutation(mutation.Context(), evt)
	}); err != nil {
		t.Fatalf("PublishInMutation: %v", err)
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.scopes[eventID]; got != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	if got := store.receipts[eventID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q, want processed", got)
	}
}

func TestEngineOutbox_ConnectRoutePlanPersistsSharedRoutePlan(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From:     "producer.deploy_done",
		To:       "consumer.deploy_completed",
		Delivery: "one",
	})
	store := &connectRoutePlanMutationStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := connectRoutePlanStaticDeliveryRoute()

	planned, err := (engineOutbox{bus: eb}).deliveryPlanForIntent(context.Background(), runtimeengine.EmitIntent{Event: evt})
	if err != nil {
		t.Fatalf("deliveryPlanForIntent: %v", err)
	}
	if planned.AuthorityState != RoutePlanAuthorityCanonicalMatched || planned.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("outbox route plan authority = %q/%q, want matched connect route plan", planned.AuthorityState, planned.AuthorityOwner)
	}
	if !deliveryRoutesContain(planned.DeliveryRoutes(), want) {
		t.Fatalf("outbox route plan delivery routes = %#v, want %#v", planned.DeliveryRoutes(), want)
	}

	if err := store.RunEventMutation(context.Background(), func(mutation EventMutation) error {
		return eb.EngineOutbox().WriteOutbox(mutation.Context(), []runtimeengine.EmitIntent{{Event: evt}})
	}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.scopes[eventID]; got != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
}

func TestEventBusPlan_ConnectRoutePlanPreservesReplyLineage(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From:     "producer.deploy_done",
		To:       "consumer.deploy_completed",
		Delivery: "reply",
		Reply: map[string]string{
			"correlation_id": "event.correlation_id",
		},
	})
	eb, err := NewEventBusWithOptions(newTargetRouteMemoryStore(), EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	plan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	reply, ok := plan.ExtraDetail["connect_route_plan_reply"].(map[string]string)
	if !ok {
		t.Fatalf("connect_route_plan_reply = %#v, want map[string]string", plan.ExtraDetail["connect_route_plan_reply"])
	}
	if got, want := reply["correlation_id"], "event.correlation_id"; got != want {
		t.Fatalf("reply correlation_id = %q, want %q", got, want)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes(), connectRoutePlanStaticDeliveryRoute()) {
		t.Fatalf("delivery routes = %#v, want static connected route", plan.DeliveryRoutes())
	}
}

func TestEventBusPublish_ConnectRoutePlanPersistsTargetSetFanout(t *testing.T) {
	source := connectRoutePlanFanoutSource()
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{
			{InstanceID: "alpha", EntityID: "team-a", FlowInstance: "worker/alpha"},
			{InstanceID: "beta", EntityID: "team-a", FlowInstance: "worker/beta"},
			{InstanceID: "gamma", EntityID: "team-b", FlowInstance: "worker/gamma"},
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	for _, instanceID := range []string{"alpha", "beta", "gamma"} {
		if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{
			Identity: runtimeflowidentity.DeriveRoute("worker", instanceID),
		}); err != nil {
			t.Fatalf("AddFlowInstanceRoute(%s): %v", instanceID, err)
		}
	}
	resolvedAlpha := eb.RouteTable().Resolve("worker/alpha/ticket.ready")
	if !subscriberListContainsRouteSource(resolvedAlpha, "worker-alpha", "worker/alpha", "receiver_carrier") {
		t.Fatalf("receiver carrier route worker/alpha/ticket.ready = %#v, want worker-alpha receiver_carrier", resolvedAlpha)
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/ticket.ready"), "", "", json.RawMessage(`{"team_entity":"team-a"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	wantAlpha := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "worker-alpha", Target: events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/alpha", EntityID: "team-a"}}
	wantBeta := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "worker-beta", Target: events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/beta", EntityID: "team-a"}}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if store.flowInstanceDescriptorCalls == 0 {
		t.Fatalf("flow-instance descriptor loader was not called for descriptor-backed connect fanout")
	}
	routes := store.routes[eventID]
	if !deliveryRoutesContain(routes, wantAlpha) || !deliveryRoutesContain(routes, wantBeta) {
		t.Fatalf("persisted delivery routes = %#v, want alpha and beta", routes)
	}
	if len(routes) != 2 {
		t.Fatalf("persisted delivery routes = %#v, want exactly two team-a fanout routes", routes)
	}
}

func TestEventBusResetInMemoryStateRefreshesConnectRoutePlanner(t *testing.T) {
	source := connectRoutePlanFanoutSource()
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances:          []ActiveFlowInstanceDescriptor{{InstanceID: "alpha", EntityID: "team-a", FlowInstance: "worker/alpha"}},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{
		Identity: runtimeflowidentity.DeriveRoute("worker", "alpha"),
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(alpha): %v", err)
	}

	if err := eb.ResetInMemoryState(); err != nil {
		t.Fatalf("ResetInMemoryState: %v", err)
	}
	store.flowInstances = []ActiveFlowInstanceDescriptor{{InstanceID: "beta", EntityID: "team-a", FlowInstance: "worker/beta"}}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{
		Identity: runtimeflowidentity.DeriveRoute("worker", "beta"),
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(beta): %v", err)
	}

	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/ticket.ready"), "", "", json.RawMessage(`{"team_entity":"team-a"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	wantBeta := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "worker-beta", Target: events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/beta", EntityID: "team-a"}}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan after reset: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), wantBeta) || len(routePlan.DeliveryRoutes()) != 1 {
		t.Fatalf("route plan delivery routes = %#v, want only refreshed beta route", routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan after reset: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, wantBeta) || len(plan.DeliveryRoutes) != 1 {
		t.Fatalf("preflight delivery routes = %#v, want only refreshed beta route", plan.DeliveryRoutes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish after reset: %v", err)
	}
	routes := store.routes[eventID]
	if !deliveryRoutesContain(routes, wantBeta) {
		t.Fatalf("persisted delivery routes = %#v, want refreshed beta route", routes)
	}
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want only refreshed beta route", routes)
	}
}

func TestEventBusPublish_ConnectRoutePlanPersistsIndexedBusinessFieldTarget(t *testing.T) {
	source := connectRoutePlanBusinessFieldSource("one", true)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      "ent-1",
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "consumer-node-one", Target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: "ent-1"}}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), want) || len(routePlan.DeliveryRoutes()) != 1 {
		t.Fatalf("route plan delivery routes = %#v, want indexed business-field route %#v", routePlan.DeliveryRoutes(), want)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want indexed business-field route %#v", store.routes[eventID], want)
	}
}

func TestEventBusPublish_ConnectRoutePlanPersistsTemplateInstanceKeyTarget(t *testing.T) {
	source := connectRoutePlanInstanceKeySource(t)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      "ent-1",
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "consumer-node-one", Target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: "ent-1"}}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), want) || len(routePlan.DeliveryRoutes()) != 1 {
		t.Fatalf("route plan delivery routes = %#v, want instance-key route %#v", routePlan.DeliveryRoutes(), want)
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, want) || len(plan.DeliveryRoutes) != 1 {
		t.Fatalf("preflight delivery routes = %#v, want instance-key route %#v", plan.DeliveryRoutes, want)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want instance-key route %#v", store.routes[eventID], want)
	}
	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, runtimereplayclaim.CommittedReplayScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if !containsString(live, "consumer-node-one") || !containsString(internal, "consumer-node-one") {
		t.Fatalf("replay live=%#v internal=%#v, want consumer-node-one from persisted connect route", live, internal)
	}
	if !deliveryRoutesContain(replayRoutes, want) {
		t.Fatalf("replay delivery routes = %#v, want %#v", replayRoutes, want)
	}
}

func TestEventBusPublish_ConnectRoutePlanCreatesTemplateInstanceOnMissingCreate(t *testing.T) {
	source := connectRoutePlanInstanceKeySourceWithPolicy(t, "create", "reuse")
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(store.activations) != 0 {
		t.Fatalf("preflight activations = %d, want 0", len(store.activations))
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight failure/routes = %q/%#v, want one preview route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	previewTarget := preflight.DeliveryRoutes[0].Target
	if previewTarget.FlowID != "consumer" || previewTarget.FlowInstance == "" || previewTarget.EntityID == "" {
		t.Fatalf("preview target = %#v, want deterministic consumer flow instance", previewTarget)
	}
	previewIdentity := runtimeflowidentity.StoredRoute("consumer", runtimeflowidentity.LogicalInstanceID(previewTarget.FlowInstance), previewTarget.FlowInstance)
	if routes := eb.RouteTable().MaterializedRoutes(previewIdentity); len(routes) != 0 {
		t.Fatalf("preview route table state leaked after preflight: %#v", routes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	if activation.Config["vertical_id"] != "v-1" || activation.Metadata["vertical_id"] != "v-1" {
		t.Fatalf("activation config/metadata = %#v/%#v, want vertical_id v-1", activation.Config, activation.Metadata)
	}
	if activation.Metadata["entity_type"] != "deployment" || activation.Metadata["instance_kind"] != "template" || activation.Metadata["last_source_event"] != evt.ID() {
		t.Fatalf("activation metadata = %#v, want entity_type/instance_kind/last_source_event proof", activation.Metadata)
	}
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node-" + activation.Instance.InstanceID,
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: activation.Instance.InstancePath,
			EntityID:     activation.Instance.EntityID,
		},
	}
	if !deliveryRoutesContain(store.routes[evt.ID()], want) || len(store.routes[evt.ID()]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want created instance route %#v", store.routes[evt.ID()], want)
	}

	retry := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eb.Publish(context.Background(), retry); err != nil {
		t.Fatalf("Publish retry: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("retry activations = %d, want idempotent reuse without a second activation", len(store.activations))
	}
	if !deliveryRoutesContain(store.routes[retry.ID()], want) || len(store.routes[retry.ID()]) != 1 {
		t.Fatalf("retry delivery routes = %#v, want reused instance route %#v", store.routes[retry.ID()], want)
	}

	replayTarget := eb.SubscribeInternal(want.SubscriberID)
	store.flowInstances = []ActiveFlowInstanceDescriptor{{
		InstanceID:    "drift",
		EntityID:      "ent-drift",
		FlowInstance:  "consumer/drift",
		AddressFields: map[string]string{"entity.vertical_id": "v-1"},
	}}
	store.flowInstanceDescriptorCalls = 0
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "drift")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(drift): %v", err)
	}
	if err := eb.PublishPersistedRecipients(context.Background(), evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients: %v", err)
	}
	replayed := requireBusEvent(t, replayTarget, "persisted replay after lifecycle-created descriptor drift")
	if replayed.FlowInstance() != activation.Instance.InstancePath || replayed.EntityID() != activation.Instance.EntityID {
		t.Fatalf("replayed delivery target = flow_instance:%q entity:%q, want persisted lifecycle-created %q/%q",
			replayed.FlowInstance(), replayed.EntityID(), activation.Instance.InstancePath, activation.Instance.EntityID)
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("replay descriptor calls = %d, want 0 because lifecycle-created persisted route/scope is authoritative", got)
	}
}

func TestEventBusPublish_ConnectRoutePlanCreateResolutionMintsUUIDAndCarriesInstanceKey(t *testing.T) {
	source := connectRoutePlanCreateResolutionSource(t, runtimecontracts.FlowInputResolutionMintUUID)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight failure/routes = %q/%#v, want one preview route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if got := len(store.activations); got != 0 {
		t.Fatalf("preflight activations = %d, want 0", got)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	minted, _ := activation.Metadata["validation_case_id"].(string)
	if _, err := uuid.Parse(minted); err != nil {
		t.Fatalf("minted validation_case_id = %q, want uuid: %v", minted, err)
	}
	if minted == eventID {
		t.Fatalf("minted validation_case_id = source event id %q, want deterministic uuid mint distinct from event_id mint", minted)
	}
	if activation.Config["validation_case_id"] != minted || activation.Metadata["validation_case_id"] != minted {
		t.Fatalf("activation config/metadata = %#v/%#v, want carried validation_case_id %q", activation.Config, activation.Metadata, minted)
	}
	if got := activation.Metadata["last_source_event"]; got != eventID {
		t.Fatalf("last_source_event = %v, want %q", got, eventID)
	}
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "validator-node-" + minted,
		Target: events.RouteIdentity{
			FlowID:       "validator",
			FlowInstance: activation.Instance.InstancePath,
			EntityID:     activation.Instance.EntityID,
		},
	}
	if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want create-resolution route %#v", store.routes[eventID], want)
	}

	replayTarget := eb.SubscribeInternal(want.SubscriberID)
	store.flowInstanceDescriptorCalls = 0
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish same event retry: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("same-event retry activations = %d, want committed replay without a second activation", len(store.activations))
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("same-event retry descriptor calls = %d, want committed replay without descriptor lookup", got)
	}
	replayed := requireBusEvent(t, replayTarget, "create resolution same-event committed replay")
	if replayed.FlowInstance() != activation.Instance.InstancePath || replayed.EntityID() != activation.Instance.EntityID {
		t.Fatalf("same-event replay target = flow_instance:%q entity:%q, want persisted %q/%q",
			replayed.FlowInstance(), replayed.EntityID(), activation.Instance.InstancePath, activation.Instance.EntityID)
	}
}

func TestEventBusPublish_ConnectRoutePlanCreateResolutionCanMintFromEventID(t *testing.T) {
	source := connectRoutePlanCreateResolutionSource(t, runtimecontracts.FlowInputResolutionMintEventID)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	if activation.Metadata["validation_case_id"] != eventID || activation.Config["validation_case_id"] != eventID {
		t.Fatalf("activation config/metadata = %#v/%#v, want event_id-minted validation_case_id %q", activation.Config, activation.Metadata, eventID)
	}
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "validator-node-" + eventID,
		Target: events.RouteIdentity{
			FlowID:       "validator",
			FlowInstance: activation.Instance.InstancePath,
			EntityID:     activation.Instance.EntityID,
		},
	}
	if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want event_id create-resolution route %#v", store.routes[eventID], want)
	}
}

func TestEventBusPublish_ConnectRoutePlanLifecycleCreateRefreshesDescriptorsForLaterPlans(t *testing.T) {
	source := connectRoutePlanInstanceKeyMultiInputSourceWithPolicy(t, "create", "reuse")
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want exactly one lifecycle create for both connect plans", len(store.activations))
	}
	activation := store.activations[0]
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node-" + activation.Instance.InstanceID,
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: activation.Instance.InstancePath,
			EntityID:     activation.Instance.EntityID,
		},
	}
	if !deliveryRoutesContain(store.routes[evt.ID()], want) || len(store.routes[evt.ID()]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want one deduplicated route %#v", store.routes[evt.ID()], want)
	}
}

func TestEventBusPublish_ConnectRoutePlanCreateRejectSameEventRetryUsesCommittedReplay(t *testing.T) {
	source := connectRoutePlanInstanceKeySourceWithPolicy(t, "create", "reject")
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish initial: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("initial activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	replayTarget := eb.SubscribeInternal("consumer-node-" + activation.Instance.InstanceID)
	store.flowInstanceDescriptorCalls = 0

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish retry: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("retry activations = %d, want committed replay without a second activation", len(store.activations))
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("retry descriptor calls = %d, want 0 because committed replay is authoritative", got)
	}
	replayed := requireBusEvent(t, replayTarget, "same-event retry committed replay")
	if replayed.FlowInstance() != activation.Instance.InstancePath || replayed.EntityID() != activation.Instance.EntityID {
		t.Fatalf("retry delivery target = flow_instance:%q entity:%q, want persisted %q/%q",
			replayed.FlowInstance(), replayed.EntityID(), activation.Instance.InstancePath, activation.Instance.EntityID)
	}
}

func TestEventBusPublish_ConnectRoutePlanDefaultedPoliciesMatchCreateReject(t *testing.T) {
	source := connectRoutePlanInstanceKeySourceWithDefaultedPolicies(t)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish defaulted policies: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	if activation.Config["template_instance_on_missing"] != "create" || activation.Config["template_instance_on_conflict"] != "reject" {
		t.Fatalf("activation policy config = %#v, want defaulted create/reject", activation.Config)
	}
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node-" + activation.Instance.InstanceID,
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: activation.Instance.InstancePath,
			EntityID:     activation.Instance.EntityID,
		},
	}
	if !deliveryRoutesContain(store.routes[evt.ID()], want) || len(store.routes[evt.ID()]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want defaulted create/reject route %#v", store.routes[evt.ID()], want)
	}
}

func TestEventBusPublish_ConnectRoutePlanCreatesRenamedTemplateInstanceKeyTarget(t *testing.T) {
	source := connectRoutePlanRenamedInstanceKeySourceWithPolicy(t, "create", "reuse")
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"source_vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	if activation.Config["vertical_id"] != "v-1" || activation.Metadata["vertical_id"] != "v-1" {
		t.Fatalf("renamed activation config/metadata = %#v/%#v, want receiver vertical_id from adapter source_vertical_id", activation.Config, activation.Metadata)
	}
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node-" + activation.Instance.InstanceID,
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: activation.Instance.InstancePath,
			EntityID:     activation.Instance.EntityID,
		},
	}
	if !deliveryRoutesContain(store.routes[evt.ID()], want) || len(store.routes[evt.ID()]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want renamed-key created route %#v", store.routes[evt.ID()], want)
	}
}

func TestEventBusPublish_ConnectRoutePlanRejectsCreateConflict(t *testing.T) {
	source := connectRoutePlanInstanceKeySourceWithPolicy(t, "create", "reject")
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "one",
				EntityID:      "ent-1",
				FlowInstance:  "consumer/one",
				AddressFields: map[string]string{"entity.vertical_id": "v-1"},
			}},
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if got, want := routePlan.TargetFailure, runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureInstanceConflict); got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("delivery routes = %#v, want none on create conflict", routePlan.DeliveryRoutes())
	}
	if len(store.activations) != 0 {
		t.Fatalf("activations = %d, want 0 on conflict", len(store.activations))
	}
}

func TestEventBusPublish_ConnectRoutePlanLifecycleUnavailableBlocksLowerPrecedenceRescue(t *testing.T) {
	source := connectRoutePlanInstanceKeySourceWithPolicy(t, "create", "reuse")
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
	}
	materializerCalled := false
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle: source,
		RecipientPlanMaterializer: func(context.Context, events.Event, PublishRecipientPlan) ([]events.DeliveryRoute, error) {
			materializerCalled = true
			return []events.DeliveryRoute{{
				SubscriberType: "node",
				SubscriberID:   "bogus-node",
				Target:         events.RouteIdentity{FlowID: "bogus", FlowInstance: "bogus/one", EntityID: "bogus"},
			}}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	raw := eb.Subscribe("raw-source-listener", events.EventType("producer/deploy.done"), events.EventType("deploy.done"))
	defer eb.Unsubscribe("raw-source-listener")
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if materializerCalled {
		t.Fatalf("recipient plan materializer was called for lifecycle-unavailable canonical failure")
	}
	if got, want := routePlan.TargetFailure, runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureLifecycleUnavailable); got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("delivery routes = %#v, want none on lifecycle-unavailable failure", routePlan.DeliveryRoutes())
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if materializerCalled {
		t.Fatalf("recipient plan materializer was called during publish for lifecycle-unavailable canonical failure")
	}
	requireNoConnectRoutePlanBusEvent(t, raw, "source/raw subscriber fallback")
}

func TestEventBusReplay_ConnectRoutePlanUsesPersistedInstanceKeyRouteAfterDescriptorDrift(t *testing.T) {
	source := connectRoutePlanInstanceKeySource(t)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      "ent-1",
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(one): %v", err)
	}
	consumerOne := eb.SubscribeInternal("consumer-node-one")
	consumerTwo := eb.SubscribeInternal("consumer-node-two")
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	wantOne := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "consumer-node-one", Target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: "ent-1"}}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, consumerOne, "initial instance-key publish")
	if got.FlowInstance() != "consumer/one" || got.EntityID() != "ent-1" {
		t.Fatalf("initial delivery target = flow_instance:%q entity:%q, want consumer/one ent-1", got.FlowInstance(), got.EntityID())
	}
	if !deliveryRoutesContain(store.routes[eventID], wantOne) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want instance-key route %#v", store.routes[eventID], wantOne)
	}

	store.flowInstances = []ActiveFlowInstanceDescriptor{{
		InstanceID:    "two",
		EntityID:      "ent-2",
		FlowInstance:  "consumer/two",
		AddressFields: map[string]string{"entity.vertical_id": "v-1"},
	}}
	store.flowInstanceDescriptorCalls = 0
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "two")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(two): %v", err)
	}

	if err := eb.PublishPersistedRecipients(context.Background(), evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients: %v", err)
	}
	got = requireBusEvent(t, consumerOne, "persisted replay after descriptor drift")
	if got.FlowInstance() != "consumer/one" || got.EntityID() != "ent-1" {
		t.Fatalf("replayed delivery target = flow_instance:%q entity:%q, want persisted consumer/one ent-1", got.FlowInstance(), got.EntityID())
	}
	select {
	case evt := <-consumerTwo:
		t.Fatalf("descriptor drift recipient received replay: flow_instance:%q entity:%q", evt.FlowInstance(), evt.EntityID())
	default:
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("replay descriptor calls = %d, want 0 because persisted route/scope is authoritative", got)
	}
}

func TestEventBusPublish_ConnectRoutePlanPersistsRenamedTemplateInstanceKeyTarget(t *testing.T) {
	source := connectRoutePlanRenamedInstanceKeySource(t)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      "ent-1",
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"source_vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "consumer-node-one", Target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: "ent-1"}}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), want) || len(routePlan.DeliveryRoutes()) != 1 {
		t.Fatalf("route plan delivery routes = %#v, want renamed instance-key route %#v", routePlan.DeliveryRoutes(), want)
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, want) || len(plan.DeliveryRoutes) != 1 {
		t.Fatalf("preflight delivery routes = %#v, want renamed instance-key route %#v", plan.DeliveryRoutes, want)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want renamed instance-key route %#v", store.routes[eventID], want)
	}
	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, runtimereplayclaim.CommittedReplayScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if !containsString(live, "consumer-node-one") || !containsString(internal, "consumer-node-one") {
		t.Fatalf("replay live=%#v internal=%#v, want consumer-node-one from persisted connect route", live, internal)
	}
	if !deliveryRoutesContain(replayRoutes, want) {
		t.Fatalf("replay delivery routes = %#v, want %#v", replayRoutes, want)
	}
}

func TestEventBusPublish_ConnectRoutePlanFailsClosedForRenamedTemplateInstanceKeySourceGap(t *testing.T) {
	source := connectRoutePlanRenamedInstanceKeySource(t)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      "ent-1",
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	raw := eb.Subscribe("raw-source-listener", events.EventType("producer/deploy.done"), events.EventType("deploy.done"))
	defer eb.Unsubscribe("raw-source-listener")
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"nested":{"source_vertical_id":"v-1"}}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if routePlan.TargetFailure != runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureAddressValueMissing) {
		t.Fatalf("target failure = %q, want %q", routePlan.TargetFailure, runtimepinrouting.ConnectFailureAddressValueMissing)
	}
	if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.RoutedRecipients) != 0 ||
		len(routePlan.SubscribedRecipients) != 0 || len(routePlan.RecipientIDs()) != 0 ||
		len(routePlan.PersistedRecipientIDs()) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("fail-closed renamed instance-key route exposed lower-precedence fallback: live=%#v intents=%#v routed=%#v subscriptions=%#v recipients=%#v persisted=%#v routes=%#v",
			routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.RoutedRecipients, routePlan.SubscribedRecipients,
			routePlan.RecipientIDs(), routePlan.PersistedRecipientIDs(), routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got, want := plan.TargetFailure, string(runtimepinrouting.ConnectFailureAddressValueMissing); got != want {
		t.Fatalf("preflight target failure = %q, want %q", got, want)
	}
	if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
		len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
		t.Fatalf("preflight exposed lower-precedence fallback: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
			plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; len(routes) != 0 {
		t.Fatalf("persisted delivery routes = %#v, want none for fail-closed renamed instance-key route", routes)
	}
	requireNoConnectRoutePlanBusEvent(t, raw, "source/raw subscriber fallback")
}

func TestEventBusPublish_ConnectRoutePlanBroadcastIgnoresInstanceKeyFiltering(t *testing.T) {
	source := connectRoutePlanInstanceKeyBroadcastSource(t)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{
			{InstanceID: "one", EntityID: "ent-1", FlowInstance: "consumer/one", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
			{InstanceID: "two", EntityID: "ent-2", FlowInstance: "consumer/two", AddressFields: map[string]string{"entity.vertical_id": "v-2"}},
			{InstanceID: "other", EntityID: "ent-3", FlowInstance: "other/three", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	for _, instanceID := range []string{"one", "two"} {
		if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", instanceID)}); err != nil {
			t.Fatalf("AddFlowInstanceRoute(%s): %v", instanceID, err)
		}
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	wantOne := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "consumer-node-one", Target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: "ent-1"}}
	wantTwo := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "consumer-node-two", Target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/two", EntityID: "ent-2"}}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), wantOne) || !deliveryRoutesContain(routePlan.DeliveryRoutes(), wantTwo) || len(routePlan.DeliveryRoutes()) != 2 {
		t.Fatalf("route plan delivery routes = %#v, want broadcast routes %#v and %#v", routePlan.DeliveryRoutes(), wantOne, wantTwo)
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, wantOne) || !deliveryRoutesContain(plan.DeliveryRoutes, wantTwo) || len(plan.DeliveryRoutes) != 2 {
		t.Fatalf("preflight delivery routes = %#v, want broadcast routes %#v and %#v", plan.DeliveryRoutes, wantOne, wantTwo)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !deliveryRoutesContain(store.routes[eventID], wantOne) || !deliveryRoutesContain(store.routes[eventID], wantTwo) || len(store.routes[eventID]) != 2 {
		t.Fatalf("persisted delivery routes = %#v, want broadcast routes %#v and %#v", store.routes[eventID], wantOne, wantTwo)
	}
}

func TestEventBusPublish_ConnectRoutePlanFailsClosedForTemplateInstanceKeyGaps(t *testing.T) {
	tests := []struct {
		name          string
		payload       string
		flowInstances []ActiveFlowInstanceDescriptor
		addRoutes     []string
		wantFailure   runtimepinrouting.TargetFailure
	}{
		{
			name:        "missing source key value",
			payload:     `{}`,
			wantFailure: runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureAddressValueMissing),
		},
		{
			name:    "no receiver instance under rejecting policy",
			payload: `{"vertical_id":"v-1"}`,
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "two",
				EntityID:      "ent-2",
				FlowInstance:  "consumer/two",
				AddressFields: map[string]string{"entity.vertical_id": "v-2"},
			}},
			addRoutes:   []string{"two"},
			wantFailure: runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureTargetUnresolved),
		},
		{
			name:    "ambiguous receiver instance key",
			payload: `{"vertical_id":"v-1"}`,
			flowInstances: []ActiveFlowInstanceDescriptor{
				{InstanceID: "one", EntityID: "ent-1", FlowInstance: "consumer/one", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
				{InstanceID: "two", EntityID: "ent-2", FlowInstance: "consumer/two", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
			},
			addRoutes:   []string{"one", "two"},
			wantFailure: runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureTargetAmbiguous),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := connectRoutePlanInstanceKeySource(t)
			store := &connectRoutePlanDescriptorStore{
				targetRouteMemoryStore: newTargetRouteMemoryStore(),
				flowInstances:          tc.flowInstances,
			}
			eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			for _, instanceID := range tc.addRoutes {
				if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", instanceID)}); err != nil {
					t.Fatalf("AddFlowInstanceRoute(%s): %v", instanceID, err)
				}
			}
			raw := eb.Subscribe("raw-source-listener", events.EventType("producer/deploy.done"), events.EventType("deploy.done"))
			defer eb.Unsubscribe("raw-source-listener")
			eventID := uuid.NewString()
			evt := eventtest.RootIngress(eventID,
				events.EventType("producer/deploy.done"), "", "", json.RawMessage(tc.payload), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

			routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
			if err != nil {
				t.Fatalf("planSubscribedRoutePlan: %v", err)
			}
			if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
				t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
			}
			if routePlan.TargetFailure != tc.wantFailure {
				t.Fatalf("target failure = %q, want %q", routePlan.TargetFailure, tc.wantFailure)
			}
			if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.RoutedRecipients) != 0 ||
				len(routePlan.SubscribedRecipients) != 0 || len(routePlan.RecipientIDs()) != 0 ||
				len(routePlan.PersistedRecipientIDs()) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
				t.Fatalf("fail-closed instance-key route exposed lower-precedence fallback: live=%#v intents=%#v routed=%#v subscriptions=%#v recipients=%#v persisted=%#v routes=%#v",
					routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.RoutedRecipients, routePlan.SubscribedRecipients,
					routePlan.RecipientIDs(), routePlan.PersistedRecipientIDs(), routePlan.DeliveryRoutes())
			}

			plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if got, want := plan.TargetFailure, string(tc.wantFailure); got != want {
				t.Fatalf("preflight target failure = %q, want %q", got, want)
			}
			if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
				len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("preflight exposed lower-precedence fallback: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
					plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
			}
			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if routes := store.routes[eventID]; len(routes) != 0 {
				t.Fatalf("persisted delivery routes = %#v, want none for fail-closed instance-key route", routes)
			}
			requireNoConnectRoutePlanBusEvent(t, raw, "source/raw subscriber fallback")
		})
	}
}

func TestEventBusPublish_ConnectRoutePlanPersistsIndexedBusinessFieldTargetSet(t *testing.T) {
	source := connectRoutePlanBusinessFieldSource("many", true)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{
			{InstanceID: "one", EntityID: "ent-1", FlowInstance: "consumer/one", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
			{InstanceID: "two", EntityID: "ent-2", FlowInstance: "consumer/two", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
			{InstanceID: "other", EntityID: "ent-3", FlowInstance: "consumer/other", AddressFields: map[string]string{"entity.vertical_id": "v-2"}},
		},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	for _, id := range []string{"one", "two", "other"} {
		if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", id)}); err != nil {
			t.Fatalf("AddFlowInstanceRoute(%s): %v", id, err)
		}
	}
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	wantOne := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "consumer-node-one", Target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: "ent-1"}}
	wantTwo := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "consumer-node-two", Target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/two", EntityID: "ent-2"}}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	routes := store.routes[eventID]
	if !deliveryRoutesContain(routes, wantOne) || !deliveryRoutesContain(routes, wantTwo) || len(routes) != 2 {
		t.Fatalf("persisted target_set delivery routes = %#v, want one/two only", routes)
	}
}

func TestEventBusPublish_ConnectRoutePlanFailsClosedForBusinessFieldDescriptorGaps(t *testing.T) {
	tests := []struct {
		name          string
		source        semanticview.Source
		payload       string
		flowInstances []ActiveFlowInstanceDescriptor
		wantFailure   runtimepinrouting.TargetFailure
	}{
		{
			name:        "missing source value",
			source:      connectRoutePlanBusinessFieldSource("one", true),
			payload:     `{}`,
			wantFailure: runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureAddressValueMissing),
		},
		{
			name:    "no descriptor match",
			source:  connectRoutePlanBusinessFieldSource("one", true),
			payload: `{"vertical_id":"v-1"}`,
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "one",
				EntityID:      "ent-1",
				FlowInstance:  "consumer/one",
				AddressFields: map[string]string{"entity.vertical_id": "v-2"},
			}},
			wantFailure: runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureTargetUnresolved),
		},
		{
			name:    "ambiguous singular target",
			source:  connectRoutePlanBusinessFieldSource("one", true),
			payload: `{"vertical_id":"v-1"}`,
			flowInstances: []ActiveFlowInstanceDescriptor{
				{InstanceID: "one", EntityID: "ent-1", FlowInstance: "consumer/one", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
				{InstanceID: "two", EntityID: "ent-2", FlowInstance: "consumer/two", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
			},
			wantFailure: runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureTargetAmbiguous),
		},
		{
			name:    "unsupported unindexed target",
			source:  connectRoutePlanBusinessFieldSource("one", false),
			payload: `{"vertical_id":"v-1"}`,
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "one",
				EntityID:      "ent-1",
				FlowInstance:  "consumer/one",
				AddressFields: map[string]string{"entity.vertical_id": "v-1"},
			}},
			wantFailure: runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureTargetUnsupported),
		},
		{
			name:    "unsupported nested target",
			source:  connectRoutePlanBusinessFieldSourceWithTarget("one", true, "entity.profile.vertical_id"),
			payload: `{"vertical_id":"v-1"}`,
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "one",
				EntityID:      "ent-1",
				FlowInstance:  "consumer/one",
				AddressFields: map[string]string{"entity.profile.vertical_id": "v-1"},
			}},
			wantFailure: runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureTargetUnsupported),
		},
		{
			name:    "wrong receiver scope",
			source:  connectRoutePlanBusinessFieldSource("one", true),
			payload: `{"vertical_id":"v-1"}`,
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "one",
				EntityID:      "ent-1",
				FlowInstance:  "other/one",
				AddressFields: map[string]string{"entity.vertical_id": "v-1"},
			}},
			wantFailure: runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureTargetUnresolved),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &connectRoutePlanDescriptorStore{
				targetRouteMemoryStore: newTargetRouteMemoryStore(),
				flowInstances:          tc.flowInstances,
			}
			eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: tc.source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			evt := eventtest.RootIngress(uuid.NewString(),
				events.EventType("producer/deploy.done"), "", "", json.RawMessage(tc.payload), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

			routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
			if err != nil {
				t.Fatalf("planSubscribedRoutePlan: %v", err)
			}
			if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
				t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
			}
			if routePlan.TargetFailure != tc.wantFailure {
				t.Fatalf("target failure = %q, want %q", routePlan.TargetFailure, tc.wantFailure)
			}
			if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.RoutedRecipients) != 0 ||
				len(routePlan.RecipientIDs()) != 0 || len(routePlan.PersistedRecipientIDs()) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
				t.Fatalf("fail-closed business-field route exposed executable routes: live=%#v intents=%#v routed=%#v recipients=%#v persisted=%#v routes=%#v",
					routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.RoutedRecipients, routePlan.RecipientIDs(), routePlan.PersistedRecipientIDs(), routePlan.DeliveryRoutes())
			}
		})
	}
}

func TestEventBusPublish_ConnectRoutePlanFailsClosedForUnsupportedBusinessFieldTarget(t *testing.T) {
	source := connectRoutePlanBusinessFieldSource("one", false)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      "ent-1",
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if got, want := routePlan.TargetFailure, runtimepinrouting.TargetFailure(runtimepinrouting.ConnectFailureTargetUnsupported); got != want {
		t.Fatalf("route plan target failure = %q, want %q", got, want)
	}
	if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.RoutedRecipients) != 0 ||
		len(routePlan.RecipientIDs()) != 0 || len(routePlan.PersistedRecipientIDs()) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("unsupported target exposed executable routes: live=%#v intents=%#v routed=%#v recipients=%#v persisted=%#v routes=%#v",
			routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.RoutedRecipients, routePlan.RecipientIDs(), routePlan.PersistedRecipientIDs(), routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got, want := plan.TargetFailure, "route_plan_target_unsupported"; got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
		len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
		t.Fatalf("unsupported target preflight exposed executable routes: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
			plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
	}
}

func TestEventBusPublish_ConnectRoutePlanWithOnlySourceAndRawSubscribersFailsClosed(t *testing.T) {
	source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"producer-node": {
					ID:            "producer-node",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"deploy.done": {}},
				},
			},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:     "producer.deploy_done",
		To:       "consumer.deploy_completed",
		Delivery: "one",
	}}))
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	sourceCh := eb.Subscribe("source-raw-listener", events.EventType("producer/deploy.done"), events.EventType("deploy.done"))
	defer eb.Unsubscribe("source-raw-listener")
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID,
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if got, want := routePlan.TargetFailure, runtimepinrouting.FailureTargetNotSubscribed; got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.RoutedRecipients) != 0 ||
		len(routePlan.SubscribedRecipients) != 0 || len(routePlan.RecipientIDs()) != 0 ||
		len(routePlan.PersistedRecipientIDs()) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("fail-closed connect route exposed lower-precedence fallback: live=%#v intents=%#v routed=%#v subscriptions=%#v recipients=%#v persisted=%#v routes=%#v",
			routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.RoutedRecipients, routePlan.SubscribedRecipients,
			routePlan.RecipientIDs(), routePlan.PersistedRecipientIDs(), routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got, want := plan.TargetFailure, string(runtimepinrouting.FailureTargetNotSubscribed); got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
		len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
		t.Fatalf("preflight exposed lower-precedence fallback: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
			plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; len(routes) != 0 {
		t.Fatalf("persisted delivery routes = %#v, want none when matched connect receiver is unsubscribed", routes)
	}
	if got := store.scopes[eventID]; got != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	if got := store.receipts[eventID]; got != "dead_letter" {
		t.Fatalf("pipeline receipt = %q, want dead_letter target-delivery receipt", got)
	}
	if got, want := store.receiptErrs[eventID], "pin routing target delivery failed: target_not_subscribed"; got != want {
		t.Fatalf("pipeline receipt error = %q, want %q", got, want)
	}
	requireNoConnectRoutePlanBusEvent(t, sourceCh, "source/raw subscriber fallback")
}

func TestEventBusPublish_ConnectRoutePlanFailsClosedForInvalidLoweredPlan(t *testing.T) {
	source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:     "consumer",
			mode:   "static",
			inputs: []runtimecontracts.FlowInputEventPin{{Name: "deploy_completed", Event: "deploy.completed"}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:     "producer.deploy_done",
		To:       "consumer.missing_input",
		Delivery: "one",
	}}))
	eb, err := NewEventBusWithOptions(newTargetRouteMemoryStore(), EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if got, want := routePlan.TargetFailure, runtimepinrouting.TargetFailure("receiver_input_pin_missing"); got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("fail-closed plan has executable routes: live=%#v intents=%#v routes=%#v", routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got, want := plan.TargetFailure, "receiver_input_pin_missing"; got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(plan.DeliveryRoutes) != 0 {
		t.Fatalf("delivery routes = %#v, want none when lowered plan is invalid", plan.DeliveryRoutes)
	}
}

func TestEventBusPublish_ConnectRoutePlanFailureSkipsRecipientPlanMaterializer(t *testing.T) {
	source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:     "consumer",
			mode:   "static",
			inputs: []runtimecontracts.FlowInputEventPin{{Name: "deploy_completed", Event: "deploy.completed"}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:     "producer.deploy_done",
		To:       "consumer.missing_input",
		Delivery: "one",
	}}))
	materializerCalled := false
	eb, err := NewEventBusWithOptions(newTargetRouteMemoryStore(), EventBusOptions{
		ContractBundle: source,
		RecipientPlanMaterializer: func(context.Context, events.Event, PublishRecipientPlan) ([]events.DeliveryRoute, error) {
			materializerCalled = true
			return []events.DeliveryRoute{{
				SubscriberType: "node",
				SubscriberID:   "bogus-node",
				Target:         events.RouteIdentity{FlowID: "bogus", FlowInstance: "bogus", EntityID: "bogus"},
			}}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if materializerCalled {
		t.Fatalf("recipient plan materializer was called for matched lowered connect failure")
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("fail-closed plan has executable routes: live=%#v intents=%#v routes=%#v", routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if materializerCalled {
		t.Fatalf("recipient plan materializer was called for matched lowered connect failure")
	}
	if got, want := plan.TargetFailure, "receiver_input_pin_missing"; got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(plan.DeliveryRoutes) != 0 {
		t.Fatalf("delivery routes = %#v, want none when matched lowered plan fails", plan.DeliveryRoutes)
	}
}

func TestEventBusPlan_UnmatchedCanonicalRouteUsesLowerPrecedenceFallback(t *testing.T) {
	eb, err := NewEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("legacy-agent", events.EventType("legacy.event"))
	defer eb.Unsubscribe("legacy-agent")
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("legacy.event"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityLowerPrecedence || routePlan.AuthorityOwner != routePlanSourceAgentPolicy {
		t.Fatalf("route plan authority = %q/%q, want lower-precedence agent policy", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !containsString(routePlan.RecipientIDs(), "legacy-agent") {
		t.Fatalf("route plan recipients = %#v, want legacy-agent", routePlan.RecipientIDs())
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	requireBusEvent(t, ch, "legacy fallback delivery")
}

func TestRoutePlanCanonicalFailClosedDropsExecutableRoutes(t *testing.T) {
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan := newRoutePlan(evt)
	routePlan.MarkCanonicalRouteFailedClosed(routeIntentProducerConnectRoutePlan, runtimepinrouting.FailureTargetNotSubscribed)
	routePlan.AddLiveRecipients(RoutePlanLiveRecipient{
		RecipientID:       "bogus-node",
		SubscriberType:    "node",
		PersistAsDelivery: true,
		Producer:          routeIntentProducerRecipientMaterializer,
	})
	routePlan.AddDeliveryIntents(RoutePlanDeliveryIntent{
		SubscriberType: "node",
		SubscriberID:   "bogus-node",
		Target:         events.RouteIdentity{FlowID: "bogus", FlowInstance: "bogus", EntityID: "bogus"},
		Producer:       routeIntentProducerRecipientMaterializer,
		Persist:        true,
	})

	got := routePlan.Normalized()
	if got.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || got.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", got.AuthorityState, got.AuthorityOwner)
	}
	if got.TargetFailure != runtimepinrouting.FailureTargetNotSubscribed {
		t.Fatalf("target failure = %q, want %q", got.TargetFailure, runtimepinrouting.FailureTargetNotSubscribed)
	}
	if len(got.LiveRecipients) != 0 || len(got.DeliveryIntents) != 0 || len(got.DeliveryRoutes()) != 0 || len(got.PersistedRecipientIDs()) != 0 {
		t.Fatalf("fail-closed plan exposed executable routes: live=%#v intents=%#v routes=%#v persisted=%#v", got.LiveRecipients, got.DeliveryIntents, got.DeliveryRoutes(), got.PersistedRecipientIDs())
	}
}

func TestRoutePlanNormalizationPreservesAuthorityState(t *testing.T) {
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan := newRoutePlan(evt)
	routePlan.MarkCanonicalRouteMatched(routeIntentProducerConnectRoutePlan)
	routePlan.AddDeliveryIntents(RoutePlanDeliveryIntent{
		SubscriberType: "node",
		SubscriberID:   "consumer-node",
		Target:         connectRoutePlanStaticDeliveryRoute().Target,
		Producer:       routeIntentProducerConnectRoutePlan,
		Persist:        true,
	})

	got := routePlan.Normalized()
	if got.AuthorityState != RoutePlanAuthorityCanonicalMatched || got.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("normalized route plan authority = %q/%q, want matched connect route plan", got.AuthorityState, got.AuthorityOwner)
	}
	if !deliveryRoutesContain(got.DeliveryRoutes(), connectRoutePlanStaticDeliveryRoute()) {
		t.Fatalf("normalized route plan delivery routes = %#v, want static connect route", got.DeliveryRoutes())
	}
}

func connectRoutePlanInstanceKeySource(t testing.TB) semanticview.Source {
	t.Helper()
	return connectRoutePlanInstanceKeySourceWithPolicy(t, "reject", "reject")
}

func connectRoutePlanInstanceKeySourceWithDefaultedPolicies(t testing.TB) semanticview.Source {
	t.Helper()
	return connectRoutePlanInstanceKeySourceWithPolicyLines(t, "")
}

func connectRoutePlanInstanceKeySourceWithPolicy(t testing.TB, onMissing, onConflict string) semanticview.Source {
	t.Helper()
	return connectRoutePlanInstanceKeySourceWithPolicyLines(t, "  on_missing: "+onMissing+"\n  on_conflict: "+onConflict+"\n")
}

func connectRoutePlanInstanceKeySourceWithPolicyLines(t testing.TB, policyLines string) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeConnectRoutePlanInstanceKeyFixtureWithPolicyLines(t, "one", policyLines)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func connectRoutePlanInstanceKeyMultiInputSourceWithPolicy(t testing.TB, onMissing, onConflict string) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeConnectRoutePlanInstanceKeyMultiInputFixtureWithPolicy(t, onMissing, onConflict)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func connectRoutePlanRenamedInstanceKeySource(t testing.TB) semanticview.Source {
	t.Helper()
	return connectRoutePlanRenamedInstanceKeySourceWithPolicy(t, "reject", "reject")
}

func connectRoutePlanRenamedInstanceKeySourceWithPolicy(t testing.TB, onMissing, onConflict string) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeConnectRoutePlanRenamedInstanceKeyFixtureWithPolicy(t, onMissing, onConflict)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func connectRoutePlanInstanceKeyBroadcastSource(t testing.TB) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeConnectRoutePlanInstanceKeyFixtureWithDelivery(t, "broadcast")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func connectRoutePlanCreateResolutionSource(t testing.TB, mint string) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeConnectRoutePlanCreateResolutionFixture(t, mint)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeConnectRoutePlanInstanceKeyFixture(t testing.TB) string {
	return writeConnectRoutePlanInstanceKeyFixtureWithDelivery(t, "one")
}

func writeConnectRoutePlanCreateResolutionFixture(t testing.TB, mint string) string {
	t.Helper()
	root := t.TempDir()
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: create-resolution-connect-route-plan-bus
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: validator
    flow: validator
    mode: template
connect:
  - from: producer.validation_requested
    to: validator.validation_requested
    delivery: one
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: create-resolution-connect-route-plan-bus\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: validation_requested
        event: validation.requested
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), "validation.requested:\n  candidate: string\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "validator", "schema.yaml"), `
name: validator
mode: template
instance:
  by: validation_case_id
  on_missing: reject
  on_conflict: reject
pins:
  inputs:
    events:
      - name: validation_requested
        event: validation.requested
        resolution:
          mode: create
          instance_key:
            mint: `+mint+`
            as: validation_case_id
        carries:
          validation_case_id:
            from: instance.key.validation_case_id
            type: uuid
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "validator", "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "validator", "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "validator", "events.yaml"), "validation.requested:\n  candidate: string\n  validation_case_id: uuid\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "validator", "entities.yaml"), `
validation_case:
  validation_case_id:
    type: uuid
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "validator", "nodes.yaml"), `
validator-node:
  id: validator-node-{validation_case_id}
  execution_type: system_node
  event_handlers:
    validation.requested: {}
`)
	return root
}

func writeConnectRoutePlanRenamedInstanceKeyFixture(t testing.TB) string {
	return writeConnectRoutePlanRenamedInstanceKeyFixtureWithPolicy(t, "reject", "reject")
}

func writeConnectRoutePlanRenamedInstanceKeyFixtureWithPolicy(t testing.TB, onMissing, onConflict string) string {
	t.Helper()
	root := t.TempDir()
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: renamed-instance-key-connect-route-plan-bus
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: template
connect:
  - from: producer.deploy_done
    to: consumer.deploy_completed
    delivery: one
    using:
      instance:
        source: source_vertical_id
        target: vertical_id
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: renamed-instance-key-connect-route-plan-bus\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: source_vertical_id
        carries: [source_vertical_id]
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), "deploy.done:\n  source_vertical_id: string\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: template
instance:
  by: vertical_id
  on_missing: `+onMissing+`
  on_conflict: `+onConflict+`
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), "deploy.done:\n  source_vertical_id: string\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: string
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-node:
  id: consumer-node-{instance_id}
  execution_type: system_node
  event_handlers:
    deploy.done: {}
`)
	return root
}

func writeConnectRoutePlanInstanceKeyFixtureWithDelivery(t testing.TB, delivery string) string {
	return writeConnectRoutePlanInstanceKeyFixtureWithPolicy(t, delivery, "reject", "reject")
}

func writeConnectRoutePlanInstanceKeyFixtureWithPolicy(t testing.TB, delivery, onMissing, onConflict string) string {
	t.Helper()
	return writeConnectRoutePlanInstanceKeyFixtureWithPolicyLines(t, delivery, "  on_missing: "+onMissing+"\n  on_conflict: "+onConflict+"\n")
}

func writeConnectRoutePlanInstanceKeyFixtureWithPolicyLines(t testing.TB, delivery, policyLines string) string {
	t.Helper()
	root := t.TempDir()
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: instance-key-connect-route-plan-bus
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: template
connect:
  - from: producer.deploy_done
    to: consumer.deploy_completed
    delivery: `+delivery+`
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: instance-key-connect-route-plan-bus\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: vertical_id
        carries: [vertical_id]
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), "deploy.done:\n  vertical_id: string\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: template
instance:
  by: vertical_id
`+policyLines+`pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), "deploy.done:\n  vertical_id: string\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: string
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-node:
  id: consumer-node-{instance_id}
  execution_type: system_node
  event_handlers:
    deploy.done: {}
`)
	return root
}

func writeConnectRoutePlanInstanceKeyMultiInputFixtureWithPolicy(t testing.TB, onMissing, onConflict string) string {
	t.Helper()
	root := t.TempDir()
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: instance-key-connect-route-plan-bus
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: template
connect:
  - from: producer.deploy_done
    to: consumer.deploy_completed
    delivery: one
  - from: producer.deploy_done
    to: consumer.deploy_audited
    delivery: one
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: instance-key-connect-route-plan-bus\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: vertical_id
        carries: [vertical_id]
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), "deploy.done:\n  vertical_id: string\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: template
instance:
  by: vertical_id
  on_missing: `+onMissing+`
  on_conflict: `+onConflict+`
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
      - name: deploy_audited
        event: deploy.done
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), "deploy.done:\n  vertical_id: string\n")
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: string
`)
	writeConnectRoutePlanBusFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-node:
  id: consumer-node-{instance_id}
  execution_type: system_node
  event_handlers:
    deploy.done: {}
`)
	return root
}

func writeConnectRoutePlanBusFixtureFile(t testing.TB, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func connectRoutePlanStaticSource(connect runtimecontracts.FlowPackageConnect) semanticview.Source {
	return semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"consumer-node": {
					ID:            "consumer-node",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"deploy.completed": {}},
				},
			},
		},
	}, []runtimecontracts.FlowPackageConnect{connect}))
}

func connectRoutePlanRootProducerStaticSource() semanticview.Source {
	bundle := connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "ready",
				Event: "root.ready",
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"consumer-node": {
					ID:            "consumer-node",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"root.ready": {}},
				},
			},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:     ".root_ready",
		To:       "consumer.ready",
		Delivery: "one",
	}})
	bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{
		Pins: runtimecontracts.FlowPins{
			Outputs: runtimecontracts.FlowOutputPins{
				EventPins: []runtimecontracts.FlowOutputEventPin{{
					Name:  "root_ready",
					Event: "root.ready",
				}},
			},
		},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"root.ready": {},
	}
	return semanticview.Wrap(bundle)
}

func connectRoutePlanStaticDeliveryRoute() events.DeliveryRoute {
	return events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node",
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: "consumer",
			EntityID:     runtimeflowidentity.EntityID("consumer"),
		},
	}
}

func connectRoutePlanFanoutSource() semanticview.Source {
	return semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "ticket_ready",
				Event: "ticket.ready",
			}},
		},
		{
			id:   "worker",
			mode: "template",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "ticket_ready",
				Event: "ticket.ready",
				Address: &runtimecontracts.FlowInputPinAddress{
					By:          "team_entity",
					Source:      "payload.team_entity",
					Target:      "_entity.id",
					Cardinality: "many",
				},
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"worker-node": {
					ID:            "worker-{instance_id}",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"ticket.ready": {}},
				},
			},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:     "producer.ticket_ready",
		To:       "worker.ticket_ready",
		Delivery: "many",
	}}))
}

func connectRoutePlanBusinessFieldSource(cardinality string, indexed bool) semanticview.Source {
	return connectRoutePlanBusinessFieldSourceWithTarget(cardinality, indexed, "entity.vertical_id")
}

func connectRoutePlanBusinessFieldSourceWithTarget(cardinality string, indexed bool, target string) semanticview.Source {
	return semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "template",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
				Address: &runtimecontracts.FlowInputPinAddress{
					By:          "vertical_id",
					Source:      "payload.vertical_id",
					Target:      target,
					Cardinality: cardinality,
				},
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"consumer-node": {
					ID:            "consumer-node-{instance_id}",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"deploy.completed": {}},
				},
			},
			entityFields: map[string]runtimecontracts.EntityFieldDecl{
				"vertical_id": {Type: "string", Indexed: indexed},
			},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:     "producer.deploy_done",
		To:       "consumer.deploy_completed",
		Delivery: cardinality,
	}}))
}

func connectRoutePlanTestBundle(flows []connectRoutePlanTestFlow, connects []runtimecontracts.FlowPackageConnect) *runtimecontracts.WorkflowContractBundle {
	children := make([]runtimecontracts.FlowContractView, 0, len(flows))
	byID := make(map[string]*runtimecontracts.FlowContractView, len(flows))
	flowSchemas := make(map[string]runtimecontracts.FlowSchemaDocument, len(flows))
	flowInputs := make(map[string][]string, len(flows))
	flowOutputs := make(map[string][]string, len(flows))
	flowInputPins := make(map[string][]runtimecontracts.FlowInputEventPin, len(flows))
	flowOutputPins := make(map[string][]runtimecontracts.FlowOutputEventPin, len(flows))
	nodeHandlers := map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	workflowName := ""
	rootEntities := runtimecontracts.EntityContractsDocument{}
	for _, flow := range flows {
		schema := runtimecontracts.FlowSchemaDocument{
			Mode: flow.mode,
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events:    connectRoutePlanInputEvents(flow.inputs),
					EventPins: flow.inputs,
				},
				Outputs: runtimecontracts.FlowOutputPins{
					Events:    connectRoutePlanOutputEvents(flow.outputs),
					EventPins: flow.outputs,
				},
			},
		}
		view := runtimecontracts.FlowContractView{
			Paths:  runtimecontracts.FlowContractPaths{ID: flow.id, Flow: flow.id},
			Schema: schema,
			Path:   flow.id,
			Nodes:  flow.nodes,
		}
		children = append(children, view)
		viewCopy := view
		byID[flow.id] = &viewCopy
		flowSchemas[flow.id] = schema
		flowInputs[flow.id] = append([]string(nil), schema.Pins.Inputs.Events...)
		flowOutputs[flow.id] = append([]string(nil), schema.Pins.Outputs.Events...)
		flowInputPins[flow.id] = append([]runtimecontracts.FlowInputEventPin(nil), flow.inputs...)
		flowOutputPins[flow.id] = append([]runtimecontracts.FlowOutputEventPin(nil), flow.outputs...)
		for _, node := range flow.nodes {
			if len(node.EventHandlers) > 0 {
				nodeHandlers[node.ID] = node.EventHandlers
			}
		}
		if len(flow.entityFields) > 0 && workflowName == "" {
			workflowName = flow.id
			rootEntities["test_entity"] = runtimecontracts.EntityContract{Fields: flow.entityFields}
		}
	}
	root := runtimecontracts.FlowContractView{Children: children}
	return &runtimecontracts.WorkflowContractBundle{
		RootEntities: rootEntities,
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: byID,
		},
		FlowSchemas: flowSchemas,
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:                workflowName,
			FlowInputs:          flowInputs,
			FlowOutputs:         flowOutputs,
			FlowInputEventPins:  flowInputPins,
			FlowOutputEventPins: flowOutputPins,
			CompositionConnects: connects,
			NodeHandlers:        nodeHandlers,
		},
	}
}

func connectRoutePlanInputEvents(pins []runtimecontracts.FlowInputEventPin) []string {
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}

func connectRoutePlanOutputEvents(pins []runtimecontracts.FlowOutputEventPin) []string {
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}

func requireNoConnectRoutePlanBusEvent(t testing.TB, ch <-chan events.Event, context string) {
	t.Helper()
	select {
	case evt := <-ch:
		t.Fatalf("%s: unexpected lower-precedence bus event: %#v", context, evt)
	default:
	}
}

func subscriberListContains(in []Subscriber, id, path string) bool {
	for _, subscriber := range in {
		if subscriber.ID == id && subscriber.Path == path {
			return true
		}
	}
	return false
}

func subscriberListContainsRouteSource(in []Subscriber, id, path, routeSource string) bool {
	for _, subscriber := range in {
		if subscriber.ID == id && subscriber.Path == path && subscriber.RouteSource == routeSource {
			return true
		}
	}
	return false
}
