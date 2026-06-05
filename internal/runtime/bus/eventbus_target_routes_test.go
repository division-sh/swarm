package bus

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type targetRouteMemoryStore struct {
	events      map[string]events.Event
	routes      map[string][]events.DeliveryRoute
	scopes      map[string]replayclaim.CommittedReplayScope
	missing     []events.PersistedReplayEvent
	receipts    map[string]string
	receiptErrs map[string]string
	claimed     map[string]bool
}

func newTargetRouteMemoryStore() *targetRouteMemoryStore {
	return &targetRouteMemoryStore{
		events: map[string]events.Event{},
		routes: map[string][]events.DeliveryRoute{},
		scopes: map[string]replayclaim.CommittedReplayScope{},
	}
}

func (s *targetRouteMemoryStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events[evt.ID] = evt
	return nil
}

func (s *targetRouteMemoryStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}

func (s *targetRouteMemoryStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	var out []string
	for _, route := range s.routes[eventID] {
		if route.SubscriberType == "agent" {
			out = append(out, route.SubscriberID)
		}
	}
	return uniqueStrings(out), nil
}

func (s *targetRouteMemoryStore) SupportsPersistedReplay() bool { return true }

func (s *targetRouteMemoryStore) PersistEventWithDeliveryRouteSetAndScope(_ context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute, scope replayclaim.CommittedReplayScope) error {
	s.events[evt.ID] = evt
	s.routes[evt.ID] = events.NormalizeDeliveryRoutes(deliveryRoutes)
	s.scopes[evt.ID] = scope
	return nil
}

func (s *targetRouteMemoryStore) ListEventDeliveryRoutes(_ context.Context, eventID string) ([]events.DeliveryRoute, error) {
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...), nil
}

func (s *targetRouteMemoryStore) UpsertPipelineReceipt(_ context.Context, eventID, status, errText string) error {
	if s.receipts == nil {
		s.receipts = map[string]string{}
	}
	if s.receiptErrs == nil {
		s.receiptErrs = map[string]string{}
	}
	s.receipts[eventID] = status
	s.receiptErrs[eventID] = errText
	return nil
}

func (s *targetRouteMemoryStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return append([]events.PersistedReplayEvent(nil), s.missing...), nil
}

func (s *targetRouteMemoryStore) ClaimPipelineReplay(_ context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	if s.claimed == nil {
		s.claimed = map[string]bool{}
	}
	if s.claimed[eventID] {
		return nil, false, nil
	}
	s.claimed[eventID] = true
	return targetRouteMemoryLease{store: s, eventID: eventID}, true, nil
}

type targetRouteMemoryLease struct {
	store   *targetRouteMemoryStore
	eventID string
}

func (l targetRouteMemoryLease) Release(context.Context) error {
	if l.store != nil && l.store.claimed != nil {
		delete(l.store.claimed, l.eventID)
	}
	return nil
}

type materializedRoutePersistedBeforeInterceptor struct {
	t       *testing.T
	store   *targetRouteMemoryStore
	eventID string
	want    events.DeliveryRoute
}

func (i materializedRoutePersistedBeforeInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	i.t.Helper()
	if evt.ID != i.eventID {
		i.t.Fatalf("interceptor event_id = %q, want %q", evt.ID, i.eventID)
	}
	routes, err := i.store.ListEventDeliveryRoutes(ctx, i.eventID)
	if err != nil {
		i.t.Fatalf("ListEventDeliveryRoutes: %v", err)
	}
	if !deliveryRoutesContain(routes, i.want) {
		i.t.Fatalf("persisted routes before interceptor = %#v, want %#v", routes, i.want)
	}
	return true, nil, nil
}

func TestEventBusRecipientPlanMaterializerPersistsRoutesBeforeInterceptors(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "target-node",
		Target: events.RouteIdentity{
			FlowInstance: "review/inst-1",
		},
	}
	guardSawMaterializedRoute := false
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		RecipientPlanMaterializer: func(ctx context.Context, evt events.Event, plan PublishRecipientPlan) ([]events.DeliveryRoute, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if evt.ID != eventID {
				t.Fatalf("materializer event_id = %q, want %q", evt.ID, eventID)
			}
			if len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("pre-materialized delivery routes = %#v, want none", plan.DeliveryRoutes)
			}
			return []events.DeliveryRoute{want}, nil
		},
		RecipientPlanGuard: func(ctx context.Context, evt events.Event, plan PublishRecipientPlan) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !deliveryRoutesContain(plan.DeliveryRoutes, want) {
				t.Fatalf("guard delivery routes = %#v, want materialized %#v", plan.DeliveryRoutes, want)
			}
			guardSawMaterializedRoute = true
			return nil
		},
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    want,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		ID:        eventID,
		Type:      events.EventType("review/inst-1/task.started"),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !guardSawMaterializedRoute {
		t.Fatal("recipient plan guard did not see materialized route")
	}
}

func TestEventBusRecipientPlanMaterializerNormalizesIntoCanonicalRoutePlan(t *testing.T) {
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "target-node",
		Target: events.RouteIdentity{
			FlowInstance: "review/inst-1",
		},
	}
	eb, err := NewEventBusWithOptions(InMemoryEventStore{}, EventBusOptions{
		RecipientPlanMaterializer: func(ctx context.Context, evt events.Event, plan PublishRecipientPlan) ([]events.DeliveryRoute, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("pre-materialized delivery routes = %#v, want none", plan.DeliveryRoutes)
			}
			return []events.DeliveryRoute{want}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := events.Event{ID: uuid.NewString(), Type: events.EventType("review/inst-1/task.started")}

	plan, err := eb.materializePublishRecipientPlan(context.Background(), evt, newRoutePlan(evt).EventDeliveryPlan())
	if err != nil {
		t.Fatalf("materializePublishRecipientPlan: %v", err)
	}
	if got, wantLen := len(plan.RoutePlan.DeliveryIntents), 1; got != wantLen {
		t.Fatalf("route plan delivery intents = %d, want %d", got, wantLen)
	}
	intent := plan.RoutePlan.DeliveryIntents[0]
	if intent.SubscriberType != want.SubscriberType || intent.SubscriberID != want.SubscriberID || intent.Target != want.Target {
		t.Fatalf("route plan delivery intent = %#v, want route %#v", intent, want)
	}
	if intent.Source != routePlanSourceRecipientMaterializer || intent.Reason != routePlanReasonMaterializedRoute {
		t.Fatalf("route plan materializer source/reason = %q/%q, want materializer route", intent.Source, intent.Reason)
	}
	projected := eb.publishRecipientPlan(evt, plan.CanonicalRoutePlan())
	if !deliveryRoutesContain(projected.DeliveryRoutes, want) {
		t.Fatalf("projected delivery routes = %#v, want materialized route %#v", projected.DeliveryRoutes, want)
	}
}

func TestEventBusAgentDispatchIgnoresSameIDNodeRouteTargets(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("shared-subscriber", events.EventType("review/inst-1/task.started"))
	defer eb.Unsubscribe("shared-subscriber")

	evt := events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("review/inst-1/task.started"),
		CreatedAt: time.Now().UTC(),
	}
	if err := eb.deliverToRecipientsWithRoutes(context.Background(), evt, []string{"shared-subscriber"}, []events.DeliveryRoute{{
		SubscriberType: "node",
		SubscriberID:   "shared-subscriber",
		Target: events.RouteIdentity{
			FlowInstance: "review/inst-1",
		},
	}}); err != nil {
		t.Fatalf("deliverToRecipientsWithRoutes: %v", err)
	}
	got := requireBusEvent(t, ch, "same-id agent delivery")
	if got.HasTargetRoute() || got.FlowInstance() != "" {
		t.Fatalf("agent delivery target = route:%#v flow:%q, want no node target leakage", got.TargetRoute(), got.FlowInstance())
	}
}

func deliveryRoutesContain(routes []events.DeliveryRoute, want events.DeliveryRoute) bool {
	want = want.Normalized()
	for _, got := range events.NormalizeDeliveryRoutes(routes) {
		if got == want {
			return true
		}
	}
	return false
}

func (s *targetRouteMemoryStore) LoadCommittedReplayScope(_ context.Context, eventID string) (replayclaim.CommittedReplayScope, error) {
	scope := s.scopes[eventID]
	if scope == "" {
		return "", replayclaim.ErrMissingCommittedReplayScope
	}
	return scope, nil
}

func nodeOnlyDeliveryPlanner(nodeID string) deliveryPlanner {
	return newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{ID: nodeID, Type: "node"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: nodeID, PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)
}

func mixedNodeAgentDeliveryPlanner(nodeID, agentID string) deliveryPlanner {
	return newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{ID: nodeID, Type: "node"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{
					{ID: nodeID, PersistAsDelivery: false},
					{ID: agentID, PersistAsDelivery: true},
				}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{agentID: {AgentID: agentID}}, true, nil
			},
		},
	)
}

func nodeOnlyDeliveryPlan(evt events.Event, nodeID string) RoutePlan {
	plan := newRoutePlan(evt)
	plan.AddLiveRecipients(RoutePlanLiveRecipient{
		RecipientID:       nodeID,
		SubscriberType:    routePlanSubscriberInternal,
		PersistAsDelivery: false,
		Source:            "test",
		Reason:            "test",
	})
	plan.AddDeliveryIntents(RoutePlanDeliveryIntent{
		SubscriberType: "node",
		SubscriberID:   nodeID,
		Source:         "test",
		Reason:         "test",
		Persist:        true,
	})
	return plan.Normalized()
}

func TestEventBusPublish_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = nodeOnlyDeliveryPlanner("workflow-node")
	evt := events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("custom.node_only"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish node-only route without agent channel: %v", err)
	}
	if got := store.receipts[evt.ID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%q, want processed", got, store.receiptErrs[evt.ID])
	}
	if routes := store.routes[evt.ID]; len(routes) != 1 || routes[0].SubscriberType != "node" || routes[0].SubscriberID != "workflow-node" {
		t.Fatalf("delivery routes = %#v, want node/workflow-node", routes)
	}
}

func TestEventBusPublishTxDispatch_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("custom.node_only_tx"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}

	eb.completePublishTxDispatch(context.Background(), evt, nodeOnlyDeliveryPlan(evt, "workflow-node"))

	if got := store.receipts[evt.ID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%q, want processed", got, store.receiptErrs[evt.ID])
	}
}

func TestEngineDispatcher_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("custom.node_only_outbox"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}
	store.events[evt.ID] = evt
	store.routes[evt.ID] = []events.DeliveryRoute{{SubscriberType: "node", SubscriberID: "workflow-node"}}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{{Event: evt}}); err != nil {
		t.Fatalf("DispatchPostCommit node-only route without agent channel: %v", err)
	}
	if got := store.receipts[evt.ID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%q, want processed", got, store.receiptErrs[evt.ID])
	}
}

func TestSweepUndispatched_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	evt := events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("custom.node_only_sweep"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}
	store.events[evt.ID] = evt
	store.routes[evt.ID] = []events.DeliveryRoute{{SubscriberType: "node", SubscriberID: "workflow-node"}}
	store.scopes[evt.ID] = replayclaim.CommittedReplayScopeSubscribed
	store.missing = []events.PersistedReplayEvent{{Event: evt}}
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = nodeOnlyDeliveryPlanner("workflow-node")

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched node-only route without agent channel: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	if got := store.receipts[evt.ID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%q, want processed", got, store.receiptErrs[evt.ID])
	}
}

func TestEventBusPublish_MixedNodeAgentRouteStillRequiresAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = mixedNodeAgentDeliveryPlanner("workflow-node", "agent-missing")
	evt := events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("custom.mixed_node_agent"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}

	err = eb.Publish(context.Background(), evt)
	if err == nil {
		t.Fatal("Publish succeeded, want missing agent-channel failure")
	}
	if got := err.Error(); !strings.Contains(got, "missing=agent-missing") || strings.Contains(got, "workflow-node") {
		t.Fatalf("Publish error = %q, want missing agent only", got)
	}
}

func TestEventBusPublish_TargetSetInternalDeliveryUsesPerTargetRoutes(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{
					{ID: "child-a-listener", Type: "node", Path: "child-a/inst-1"},
					{ID: "child-b-listener", Type: "node", Path: "child-b/inst-1"},
				}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("child/output.done"))
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("child/output.done"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithTargetSet([]events.RouteIdentity{
		{FlowInstance: "child-a/inst-1", EntityID: "ent-a"},
		{FlowInstance: "child-b/inst-1", EntityID: "ent-b"},
	})

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertTargetRouteDeliveries(t, ch, "ent-a", "ent-b")

	persisted := store.events[evt.ID]
	if got := persisted.EntityID(); got != "" {
		t.Fatalf("persisted EntityID() = %q, want empty target_set projection", got)
	}
	if got := persisted.FlowInstance(); got != "" {
		t.Fatalf("persisted FlowInstance() = %q, want empty target_set projection", got)
	}
	if got := store.routes[evt.ID]; len(got) != 2 {
		t.Fatalf("persisted delivery routes = %#v, want 2", got)
	}
	for _, route := range store.routes[evt.ID] {
		if route.SubscriberType != "node" || route.SubscriberID != "workflow-runtime" {
			t.Fatalf("delivery route = %#v, want node/workflow-runtime", route)
		}
		if route.Target.Empty() {
			t.Fatalf("delivery route target is empty: %#v", route)
		}
	}

	if err := eb.PublishPersistedRecipients(context.Background(), evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients: %v", err)
	}
	assertTargetRouteDeliveries(t, ch, "ent-a", "ent-b")
}

func TestEventBusPublish_NoTargetConcreteRoutedNodePersistsSemanticNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(routedNodeTemplateBundle())
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("operating/inst-1/opco.product_initialization_requested"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-operating").WithFlowInstance("operating/inst-1")

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "concrete routed node event delivery")
	if got.FlowInstance() != "operating/inst-1" {
		t.Fatalf("delivered flow instance = %q, want operating/inst-1", got.FlowInstance())
	}

	routes := store.routes[evt.ID]
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want one lifecycle-orchestrator semantic route", routes)
	}
	route := routes[0]
	if route.SubscriberType != "node" || route.SubscriberID != "lifecycle-orchestrator" {
		t.Fatalf("delivery route = %#v, want node/lifecycle-orchestrator semantic authority", route)
	}
	if route.Target.FlowInstance != "operating/inst-1" || route.Target.EntityID != "ent-operating" {
		t.Fatalf("delivery target = %#v, want operating/inst-1 ent-operating", route.Target)
	}

	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, replayclaim.CommittedReplayScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if len(live) != 1 || live[0] != "workflow-runtime" {
		t.Fatalf("replay live recipients = %#v, want workflow-runtime", live)
	}
	if len(internal) != 1 || internal[0] != "workflow-runtime" {
		t.Fatalf("replay internal recipients = %#v, want workflow-runtime", internal)
	}
	if !deliveryRoutesContain(replayRoutes, events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "lifecycle-orchestrator",
		Target: events.RouteIdentity{
			FlowInstance: "operating/inst-1",
			EntityID:     "ent-operating",
		},
	}) {
		t.Fatalf("replay routes = %#v, want lifecycle-orchestrator semantic route", replayRoutes)
	}
	if !deliveryRoutesContain(replayRoutes, events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "workflow-runtime",
	}) {
		t.Fatalf("replay routes = %#v, want workflow-runtime replay carrier route", replayRoutes)
	}
}

func TestEventBusPublish_SemanticScopeFlowInstanceResolvesConcreteRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(routedNodeTemplateBundle())
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("operating/opco.product_initialization_requested"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-operating").WithFlowInstance("operating/inst-1")

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].Path != "operating/inst-1" {
		t.Fatalf("routed recipients = %#v, want concrete operating/inst-1 route", plan.RoutedRecipients)
	}
	if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target.FlowInstance != "operating/inst-1" {
		t.Fatalf("delivery routes = %#v, want one concrete operating route", plan.DeliveryRoutes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "semantic-scope routed node event delivery")
	if got.FlowInstance() != "operating/inst-1" {
		t.Fatalf("delivered flow instance = %q, want operating/inst-1", got.FlowInstance())
	}
	routes := store.routes[evt.ID]
	if len(routes) != 1 || routes[0].Target.FlowInstance != "operating/inst-1" {
		t.Fatalf("persisted delivery routes = %#v, want concrete operating route", routes)
	}
}

func TestEventBusCheckPublishRecipientPlan_SemanticScopeFlowInstanceMaterializesNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedNodeStaticValidationBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("validation/thing.reviewed"))
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("validation/thing.reviewed"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-validation").WithFlowInstance("validation/inst-1")

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal workflow node carrier", plan.PersistedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want one concrete node route", got)
	}
	route := plan.DeliveryRoutes[0]
	if route.SubscriberType != "node" || route.SubscriberID != "workflow-runtime" {
		t.Fatalf("delivery route = %#v, want node/workflow-runtime carrier", route)
	}
	if route.Target.FlowInstance != "validation/inst-1" || route.Target.EntityID != "ent-validation" {
		t.Fatalf("delivery route target = %#v, want validation/inst-1 ent-validation", route.Target)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "entity-writer" || plan.RoutedRecipients[0].Path != "validation" {
		t.Fatalf("routed recipients = %#v, want entity-writer at semantic validation scope", plan.RoutedRecipients)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "semantic-scope concrete route event delivery")
	if got.FlowInstance() != "validation/inst-1" || got.EntityID() != "ent-validation" {
		t.Fatalf("delivered route identity flow=%q entity=%q, want validation/inst-1 ent-validation", got.FlowInstance(), got.EntityID())
	}
	routes := store.routes[evt.ID]
	if len(routes) != 1 || routes[0].Target.FlowInstance != "validation/inst-1" {
		t.Fatalf("persisted routes = %#v, want concrete validation route", routes)
	}
}

func TestEventBusCheckPublishRecipientPlan_SemanticScopeFlowInstanceMaterializesSystemNodeRouteWithoutLiveSubscription(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedNodeStaticValidationBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("validation/thing.reviewed"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-validation").WithFlowInstance("validation/inst-1")

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 {
		t.Fatalf("recipients=%#v persisted=%#v, want no live subscriber recipients", plan.Recipients, plan.PersistedRecipients)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "entity-writer" || plan.RoutedRecipients[0].Path != "validation" || plan.RoutedRecipients[0].LocalizedEvent != "thing.reviewed" {
		t.Fatalf("routed recipients = %#v, want entity-writer local thing.reviewed at semantic validation scope", plan.RoutedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want one route-table system-node route", got)
	}
	route := plan.DeliveryRoutes[0]
	if route.SubscriberType != "node" || route.SubscriberID != "entity-writer" {
		t.Fatalf("delivery route = %#v, want node/entity-writer", route)
	}
	if route.Target.FlowInstance != "validation/inst-1" || route.Target.EntityID != "ent-validation" {
		t.Fatalf("delivery route target = %#v, want validation/inst-1 ent-validation", route.Target)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	routes := store.routes[evt.ID]
	if len(routes) != 1 || routes[0].SubscriberID != "entity-writer" || routes[0].Target.FlowInstance != "validation/inst-1" {
		t.Fatalf("persisted routes = %#v, want entity-writer concrete validation route", routes)
	}
}

func TestRouteTableRootInputFlowNodeResolvesRootInputRoute(t *testing.T) {
	rt, err := DeriveRouteTable(semanticview.Wrap(routedRootInputFlowNodeBundle()))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := rt.Resolve("thing.created")
	if len(got) != 1 {
		t.Fatalf("Resolve(thing.created) = %#v, want one root-input flow node route", got)
	}
	if got[0].ID != "entity-writer" || got[0].Type != "node" || got[0].Path != "validation" {
		t.Fatalf("resolved subscriber = %#v, want validation/entity-writer node", got[0])
	}
	if got[0].MatchPattern != "thing.created" || got[0].RouteSource != "root_input_flow" {
		t.Fatalf("resolved subscriber metadata = %#v, want root_input_flow thing.created", got[0])
	}
}

func TestEventBusPublish_RootInputFlowNodePersistsRouteBeforeDispatch(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "entity-writer",
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedRootInputFlowNodeBundle()),
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    want,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("thing.created"))
	evt := (events.Event{
		ID:        eventID,
		Type:      events.EventType("thing.created"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-root-input")

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal root-input node carrier", plan.PersistedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 || got[0].SubscriberType != "node" || got[0].SubscriberID != "entity-writer" || !got[0].Target.Empty() {
		t.Fatalf("delivery routes = %#v, want empty-target node/entity-writer route", got)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "entity-writer" || plan.RoutedRecipients[0].Path != "validation" || plan.RoutedRecipients[0].RouteSource != "root_input_flow" {
		t.Fatalf("routed recipients = %#v, want root-input validation/entity-writer", plan.RoutedRecipients)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "root-input flow-node carrier delivery")
	if got.FlowInstance() != "" || got.EntityID() != "ent-root-input" {
		t.Fatalf("delivered root input identity flow=%q entity=%q, want root ent-root-input", got.FlowInstance(), got.EntityID())
	}
	routes := store.routes[evt.ID]
	if !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.scopes[evt.ID]; got != replayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
}

func TestEventBusPublish_RootInputFlowNodePersistsRouteBeforeInterceptorWithoutInternalCarrier(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "entity-writer",
	}
	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedRootInputFlowNodeBundle()),
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    want,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := (events.Event{
		ID:        eventID,
		Type:      events.EventType("thing.created"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-root-input")

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 {
		t.Fatalf("recipients=%#v persisted=%#v, want no live carrier recipients", plan.Recipients, plan.PersistedRecipients)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "entity-writer" || plan.RoutedRecipients[0].Path != "validation" || plan.RoutedRecipients[0].RouteSource != "root_input_flow" {
		t.Fatalf("routed recipients = %#v, want root-input validation/entity-writer", plan.RoutedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 || !deliveryRoutesContain(got, want) {
		t.Fatalf("delivery routes = %#v, want empty-target node/entity-writer route", got)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[evt.ID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.scopes[evt.ID]; got != replayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
}

func TestEventBusPublish_LoadedRootInputProjectEventPersistsRouteBeforeDispatch(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "entity-writer",
	}
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, routedRootInputProjectEventFixtureFiles()))
	rt, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	resolved := rt.Resolve("thing.created")
	if len(resolved) != 1 || resolved[0].ID != "entity-writer" || resolved[0].Path != "validation" || resolved[0].RouteSource != "root_input_flow" {
		t.Fatalf("resolved subscribers = %#v, want root_input_flow validation/entity-writer", resolved)
	}

	eb, err := NewEventBusWithOptions(store, EventBusOptions{
		ContractBundle: source,
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    want,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("thing.created"))
	evt := (events.Event{
		ID:        eventID,
		Type:      events.EventType("thing.created"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-loaded-root-input")

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal root-input node carrier", plan.PersistedRecipients)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "entity-writer" || plan.RoutedRecipients[0].Path != "validation" || plan.RoutedRecipients[0].RouteSource != "root_input_flow" {
		t.Fatalf("routed recipients = %#v, want loaded root-input validation/entity-writer", plan.RoutedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 || !deliveryRoutesContain(got, want) {
		t.Fatalf("delivery routes = %#v, want empty-target node/entity-writer route", got)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "loaded root-input flow-node carrier delivery")
	if got.FlowInstance() != "" || got.EntityID() != "ent-loaded-root-input" {
		t.Fatalf("delivered root input identity flow=%q entity=%q, want root ent-loaded-root-input", got.FlowInstance(), got.EntityID())
	}
	if routes := store.routes[evt.ID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
}

func TestEventBusPublish_NoTargetRootRoutedNodeUsesSemanticNodeDeliveryRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, routedRootNodeFixtureFiles()))
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.SubscribeInternal("portfolio-node", events.EventType("opco.spinup_requested"))
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("opco.spinup_requested"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-root")

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "root routed node event delivery")
	if got.FlowInstance() != "" {
		t.Fatalf("delivered flow instance = %q, want root event", got.FlowInstance())
	}

	routes := store.routes[evt.ID]
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want one semantic root node route", routes)
	}
	route := routes[0]
	if route.SubscriberType != "node" || route.SubscriberID != "portfolio-node" {
		t.Fatalf("delivery route = %#v, want node/portfolio-node", route)
	}
	if !route.Target.Empty() {
		t.Fatalf("delivery target = %#v, want empty root target", route.Target)
	}

	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, replayclaim.CommittedReplayScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if len(live) != 1 || live[0] != "portfolio-node" {
		t.Fatalf("replay live recipients = %#v, want portfolio-node", live)
	}
	if len(internal) != 1 || internal[0] != "portfolio-node" {
		t.Fatalf("replay internal recipients = %#v, want portfolio-node", internal)
	}
	if len(replayRoutes) != 1 || replayRoutes[0].SubscriberID != "portfolio-node" || !replayRoutes[0].Target.Empty() {
		t.Fatalf("replay routes = %#v, want empty node/portfolio-node route", replayRoutes)
	}

	if err := eb.PublishPersistedRecipients(context.Background(), evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients: %v", err)
	}
	got = requireBusEvent(t, ch, "root routed node replay delivery")
	if got.FlowInstance() != "" {
		t.Fatalf("replayed flow instance = %q, want root event", got.FlowInstance())
	}
}

func TestEventBusPublish_NoTargetRootRoutedNodePersistsSemanticRouteWithoutInternalSubscription(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, routedRootNodeFixtureFiles()))
	eb, err := NewEventBusWithOptions(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := (events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("opco.spinup_requested"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-root")

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	routes := store.routes[evt.ID]
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want one semantic root node route without an internal subscription", routes)
	}
	route := routes[0]
	if route.SubscriberType != "node" || route.SubscriberID != "portfolio-node" {
		t.Fatalf("delivery route = %#v, want node/portfolio-node", route)
	}
	if !route.Target.Empty() {
		t.Fatalf("delivery target = %#v, want empty root target", route.Target)
	}
	if got := store.scopes[evt.ID]; got != replayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, replayclaim.CommittedReplayScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("replay live recipients = %#v, want none without an internal carrier", live)
	}
	if len(internal) != 0 {
		t.Fatalf("replay internal recipients = %#v, want none without an internal carrier", internal)
	}
	if len(replayRoutes) != 1 || replayRoutes[0].SubscriberType != "node" || replayRoutes[0].SubscriberID != "portfolio-node" {
		t.Fatalf("replay routes = %#v, want retained semantic node/portfolio-node evidence", replayRoutes)
	}
	if err := eb.PublishPersistedRecipients(context.Background(), evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients without internal carrier: %v", err)
	}
}

func assertTargetRouteDeliveries(t *testing.T, ch <-chan events.Event, wantEntityIDs ...string) {
	t.Helper()
	seen := map[string]struct{}{}
	for range wantEntityIDs {
		got := requireBusEvent(t, ch, "target route delivery")
		if len(got.TargetRoutes()) != 0 {
			t.Fatalf("delivered event target_set = %#v, want singular delivery target", got.TargetRoutes())
		}
		target := got.TargetRoute()
		if target.Empty() {
			t.Fatalf("delivered target route is empty: %#v", got)
		}
		seen[target.EntityID] = struct{}{}
	}
	for _, want := range wantEntityIDs {
		if _, ok := seen[want]; !ok {
			t.Fatalf("missing target delivery for %q; saw %#v", want, seen)
		}
	}
}

func loadTargetRouteTempBundle(t *testing.T, files map[string]string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("load target route temp bundle: %v", err)
	}
	return bundle
}

func routedRootNodeFixtureFiles() map[string]string {
	return map[string]string{
		"package.yaml": `name: test
version: 1.0.0
`,
		"events.yaml": `opco.spinup_requested:
  entity_id: string
`,
		"nodes.yaml": `portfolio-node:
  id: portfolio-node
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  event_handlers:
    opco.spinup_requested: {}
`,
	}
}

func routedRootInputProjectEventFixtureFiles() map[string]string {
	return map[string]string{
		"package.yaml": `name: test-root-input-project-event
version: 1.0.0
platform_version: ">=1.0.0"
flows:
  - id: validation
    flow: validation
    mode: static
`,
		"schema.yaml": `name: test-root-input-project-event
pins:
  inputs:
    events: [thing.created]
  outputs:
    events: []
`,
		"events.yaml": `thing.created:
  entity_id: string
`,
		"flows/validation/schema.yaml": `name: validation
mode: static
initial_state: ready
terminal_states: [done]
states: [ready, done]
pins:
  inputs:
    events: [thing.created]
  outputs:
    events: []
`,
		"flows/validation/nodes.yaml": `entity-writer:
  id: entity-writer
  execution_type: system_node
  subscribes_to: [thing.created]
  event_handlers:
    thing.created: {}
`,
	}
}

func routedRootInputFlowNodeBundle() *runtimecontracts.WorkflowContractBundle {
	validation := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Path:  "validation",
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "static",
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"thing.created": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"entity-writer": {
				ID:            "entity-writer",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"thing.created"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"thing.created": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{validation}}
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowInputs: map[string][]string{
				"validation": []string{"thing.created"},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"validation": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"validation": validation.Schema,
		},
	}
}

func routedNodeTemplateBundle() *runtimecontracts.WorkflowContractBundle {
	operating := runtimecontracts.FlowContractView{
		Path:  "operating",
		Paths: runtimecontracts.FlowContractPaths{ID: "operating", Flow: "operating"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{
				Event: "opco.product_initialization_requested",
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"opco.product_initialization_requested": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"lifecycle-orchestrator": {
				ID:            "lifecycle-orchestrator",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"opco.product_initialization_requested"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"opco.product_initialization_requested": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{operating}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"operating": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"operating": {
				Mode: "template",
				AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{
					Event: "opco.product_initialization_requested",
				},
			},
		},
	}
}

func routedNodeStaticValidationBundle() *runtimecontracts.WorkflowContractBundle {
	validation := runtimecontracts.FlowContractView{
		Path:  "validation",
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"thing.reviewed": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"entity-writer": {
				ID:            "entity-writer",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"thing.reviewed"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"thing.reviewed": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{validation}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"validation": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"validation": {},
		},
	}
}
