package bus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
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
	id      string
	mode    string
	inputs  []runtimecontracts.FlowInputEventPin
	outputs []runtimecontracts.FlowOutputEventPin
	nodes   map[string]runtimecontracts.SystemNodeContract
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
	evt := events.NewProjectionEvent(eventID,
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
	evt := events.NewProjectionEvent(uuid.NewString(),
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
	evt := events.NewProjectionEvent(eventID,
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
	evt := events.NewProjectionEvent(eventID,
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
	evt := events.NewProjectionEvent(uuid.NewString(),
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
	if !subscriberListContains(resolvedAlpha, "worker-alpha", "worker/alpha") {
		t.Fatalf("receiver carrier route worker/alpha/ticket.ready = %#v, want worker-alpha", resolvedAlpha)
	}
	eventID := uuid.NewString()
	evt := events.NewProjectionEvent(eventID,
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
	evt := events.NewProjectionEvent(eventID,
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

func TestEventBusPublish_ConnectRoutePlanFailsClosedForUnsupportedBusinessFieldTarget(t *testing.T) {
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
			id:   "consumer",
			mode: "template",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
				Address: &runtimecontracts.FlowInputPinAddress{
					By:          "vertical_id",
					Source:      "payload.vertical_id",
					Target:      "entity.vertical_id",
					Cardinality: "one",
				},
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:     "producer.deploy_done",
		To:       "consumer.deploy_completed",
		Delivery: "one",
	}}))
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances:          []ActiveFlowInstanceDescriptor{{InstanceID: "one", EntityID: "ent-1", FlowInstance: "consumer/one"}},
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := events.NewProjectionEvent(uuid.NewString(),
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
	evt := events.NewProjectionEvent(eventID,
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
	evt := events.NewProjectionEvent(uuid.NewString(),
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
	evt := events.NewProjectionEvent(uuid.NewString(),
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
	evt := events.NewProjectionEvent(uuid.NewString(),
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
	evt := events.NewProjectionEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	routePlan := newRoutePlan(evt)
	routePlan.MarkCanonicalRouteFailedClosed(routePlanSourceConnectRoutePlan, runtimepinrouting.FailureTargetNotSubscribed)
	routePlan.AddLiveRecipients(RoutePlanLiveRecipient{
		RecipientID:       "bogus-node",
		SubscriberType:    "node",
		PersistAsDelivery: true,
		Source:            routePlanSourceRecipientMaterializer,
		Reason:            routePlanReasonMaterializedRoute,
	})
	routePlan.AddDeliveryIntents(RoutePlanDeliveryIntent{
		SubscriberType: "node",
		SubscriberID:   "bogus-node",
		Target:         events.RouteIdentity{FlowID: "bogus", FlowInstance: "bogus", EntityID: "bogus"},
		Source:         routePlanSourceRecipientMaterializer,
		Reason:         routePlanReasonMaterializedRoute,
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

func TestRoutePlanProjectionPreservesAuthorityState(t *testing.T) {
	evt := events.NewProjectionEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	routePlan := newRoutePlan(evt)
	routePlan.MarkCanonicalRouteMatched(routePlanSourceConnectRoutePlan)
	routePlan.AddDeliveryIntents(RoutePlanDeliveryIntent{
		SubscriberType: "node",
		SubscriberID:   "consumer-node",
		Target:         connectRoutePlanStaticDeliveryRoute().Target,
		Source:         routePlanSourceConnectRoutePlan,
		Reason:         routePlanReasonLoweredConnectRoutePlan,
		Persist:        true,
	})

	projected := routePlan.EventDeliveryPlan()
	canonical := projected.CanonicalRoutePlan()
	if canonical.AuthorityState != RoutePlanAuthorityCanonicalMatched || canonical.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("canonical route plan authority = %q/%q, want matched connect route plan", canonical.AuthorityState, canonical.AuthorityOwner)
	}

	replaced := projected.WithCanonicalRoutePlan(routePlan).CanonicalRoutePlan()
	if replaced.AuthorityState != RoutePlanAuthorityCanonicalMatched || replaced.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("replaced route plan authority = %q/%q, want matched connect route plan", replaced.AuthorityState, replaced.AuthorityOwner)
	}
	if !deliveryRoutesContain(replaced.DeliveryRoutes(), connectRoutePlanStaticDeliveryRoute()) {
		t.Fatalf("replaced route plan delivery routes = %#v, want static connect route", replaced.DeliveryRoutes())
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
					Target:      "entity.entity_id",
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

func connectRoutePlanTestBundle(flows []connectRoutePlanTestFlow, connects []runtimecontracts.FlowPackageConnect) *runtimecontracts.WorkflowContractBundle {
	children := make([]runtimecontracts.FlowContractView, 0, len(flows))
	byID := make(map[string]*runtimecontracts.FlowContractView, len(flows))
	flowSchemas := make(map[string]runtimecontracts.FlowSchemaDocument, len(flows))
	flowInputs := make(map[string][]string, len(flows))
	flowOutputs := make(map[string][]string, len(flows))
	flowInputPins := make(map[string][]runtimecontracts.FlowInputEventPin, len(flows))
	flowOutputPins := make(map[string][]runtimecontracts.FlowOutputEventPin, len(flows))
	nodeHandlers := map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
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
	}
	root := runtimecontracts.FlowContractView{Children: children}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: byID,
		},
		FlowSchemas: flowSchemas,
		Semantics: runtimecontracts.WorkflowSemanticView{
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
