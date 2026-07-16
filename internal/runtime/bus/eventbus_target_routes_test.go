package bus

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type targetRouteMemoryStore struct {
	mu          sync.Mutex
	events      map[string]events.Event
	routes      map[string][]events.DeliveryRoute
	scopes      map[string]replayclaim.CommittedReplayScope
	missing     []events.PersistedReplayEvent
	receipts    map[string]string
	receiptErrs map[string]*runtimefailures.Envelope
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
	_, err := s.AppendEventOutcome(context.Background(), evt)
	return err
}

func (s *targetRouteMemoryStore) AppendEventOutcome(_ context.Context, evt events.Event) (EventAppendOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.events[evt.ID()]; exists {
		return EventAppendExactDuplicate, nil
	}
	s.events[evt.ID()] = evt
	return EventAppendInserted, nil
}

func (s *targetRouteMemoryStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}

func (s *targetRouteMemoryStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	_, err := s.PersistEventWithDeliveryRouteSetAndScopeOutcome(context.Background(), evt, deliveryRoutes, scope)
	return err
}

func (s *targetRouteMemoryStore) PersistEventWithDeliveryRouteSetAndScopeOutcome(_ context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute, scope replayclaim.CommittedReplayScope) (EventAppendOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.events[evt.ID()]; exists {
		return EventAppendExactDuplicate, nil
	}
	s.events[evt.ID()] = evt
	s.routes[evt.ID()] = events.NormalizeDeliveryRoutes(deliveryRoutes)
	s.scopes[evt.ID()] = scope
	return EventAppendInserted, nil
}

func (s *targetRouteMemoryStore) ListEventDeliveryRoutes(_ context.Context, eventID string) ([]events.DeliveryRoute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...), nil
}

func (s *targetRouteMemoryStore) UpsertPipelineReceipt(_ context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.receipts == nil {
		s.receipts = map[string]string{}
	}
	if s.receiptErrs == nil {
		s.receiptErrs = map[string]*runtimefailures.Envelope{}
	}
	s.receipts[eventID] = status
	s.receiptErrs[eventID] = runtimefailures.CloneEnvelope(failure)
	return nil
}

func (s *targetRouteMemoryStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.PersistedReplayEvent(nil), s.missing...), nil
}

func (s *targetRouteMemoryStore) ClaimPipelineReplay(_ context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed == nil {
		s.claimed = map[string]bool{}
	}
	if s.claimed[eventID] {
		return nil, false, nil
	}
	s.claimed[eventID] = true
	return targetRouteMemoryLease{store: s, eventID: eventID}, true, nil
}

func (s *targetRouteMemoryStore) ClaimPipelinePublication(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	return s.ClaimPipelineReplay(ctx, eventID)
}

type targetRouteMemoryLease struct {
	store   *targetRouteMemoryStore
	eventID string
}

func (l targetRouteMemoryLease) Release(context.Context) error {
	if l.store != nil {
		l.store.mu.Lock()
		defer l.store.mu.Unlock()
		if l.store.claimed != nil {
			delete(l.store.claimed, l.eventID)
		}
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
	if evt.ID() != i.eventID {
		i.t.Fatalf("interceptor event_id = %q, want %q", evt.ID(), i.eventID)
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

type targetRouteConsumingInterceptor struct {
	targetCalls int
}

func (i *targetRouteConsumingInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	if evt.TargetRoute().Empty() {
		return true, nil, nil
	}
	i.targetCalls++
	return false, nil, nil
}

func (i *targetRouteConsumingInterceptor) InterceptDeliveryRoute(_ context.Context, evt events.Event, route events.DeliveryRoute) (bool, []events.Event, error) {
	if route.Target.Normalized().Empty() {
		return true, nil, nil
	}
	if evt.TargetRoute().Normalized() != route.Target.Normalized() {
		return true, nil, nil
	}
	i.targetCalls++
	return false, nil, nil
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
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		RecipientPlanMaterializer: func(ctx context.Context, evt events.Event, plan PublishRecipientPlan) ([]events.DeliveryRoute, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if evt.ID() != eventID {
				t.Fatalf("materializer event_id = %q, want %q", evt.ID(), eventID)
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
	if err := eb.Publish(context.Background(), eventtest.RootIngress(eventID,
		events.EventType("review/inst-1/task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !guardSawMaterializedRoute {
		t.Fatal("recipient plan guard did not see materialized route")
	}
}

func TestEventBusPublish_TargetedNodeConsumeSuppressesLiveRecipientDelivery(t *testing.T) {
	const eventType = "worker/work.started"
	eventID := uuid.NewString()
	target := events.RouteIdentity{
		FlowID:       "worker",
		FlowInstance: "worker/inst-1",
		EntityID:     "ent-1",
	}
	targetRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "target-node",
		Target:         target,
	}
	rt := newRouteTable(nil)
	rt.eventPath[eventType] = struct{}{}
	rt.routes[eventType] = []Subscriber{{
		ID:          "target-node",
		Type:        "node",
		Path:        "worker",
		RouteSource: "subscription",
	}}
	interceptor := &targetRouteConsumingInterceptor{}
	eb, err := newScopedTestEventBus(newTargetRouteMemoryStore(), EventBusOptions{
		RouteTable: rt,
		RecipientPlanMaterializer: func(context.Context, events.Event, PublishRecipientPlan) ([]events.DeliveryRoute, error) {
			return []events.DeliveryRoute{targetRoute}, nil
		},
		Interceptors: []EventInterceptor{interceptor},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	live := eb.SubscribeAgent(testAgentSubscriptionAdmissionForFlow(t, "target-node", "worker", events.EventType(eventType)))
	defer eb.Unsubscribe("target-node")
	evt := eventtest.RootIngress(
		eventID,
		events.EventType(eventType),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Now().UTC(),
	)
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.Recipients) != 1 || plan.Recipients[0] != "target-node" {
		t.Fatalf("recipients = %#v, want target-node live recipient", plan.Recipients)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, targetRoute) {
		t.Fatalf("delivery routes = %#v, want target route %#v", plan.DeliveryRoutes, targetRoute)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if interceptor.targetCalls != 1 {
		t.Fatalf("target interceptor calls = %d, want 1", interceptor.targetCalls)
	}
	select {
	case got := <-live:
		t.Fatalf("target-consuming node event leaked to live recipient: %#v", got)
	default:
	}
}

func TestEventBusRecipientPlanMaterializerNormalizesRoutePlanDirectly(t *testing.T) {
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "target-node",
		Target: events.RouteIdentity{
			FlowInstance: "review/inst-1",
		},
	}
	eb, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{
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
	evt := eventtest.RootIngress(uuid.NewString(), events.EventType("review/inst-1/task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	emptyAgentPolicyPlan := routePlanFromManifest(evt, deliveryRecipientManifest{}, routeIntentProducerAgentPolicy)
	if emptyAgentPolicyPlan.AuthorityState != RoutePlanAuthorityNoCanonicalMatch || emptyAgentPolicyPlan.AuthorityOwner != "" {
		t.Fatalf("empty agent-policy route plan authority = %q/%q, want no canonical match", emptyAgentPolicyPlan.AuthorityState, emptyAgentPolicyPlan.AuthorityOwner)
	}

	plan, err := eb.materializePublishRecipientPlan(context.Background(), evt, emptyAgentPolicyPlan)
	if err != nil {
		t.Fatalf("materializePublishRecipientPlan: %v", err)
	}
	if got, wantLen := len(plan.DeliveryIntents), 1; got != wantLen {
		t.Fatalf("route plan delivery intents = %d, want %d", got, wantLen)
	}
	if plan.AuthorityState != RoutePlanAuthorityLowerPrecedence || plan.AuthorityOwner != routePlanSourceRecipientMaterializer {
		t.Fatalf("route plan authority = %q/%q, want lower-precedence materializer", plan.AuthorityState, plan.AuthorityOwner)
	}
	intent := plan.DeliveryIntents[0]
	if intent.SubscriberType != want.SubscriberType || intent.SubscriberID != want.SubscriberID || intent.Target != want.Target {
		t.Fatalf("route plan delivery intent = %#v, want route %#v", intent, want)
	}
	if intent.Producer != routeIntentProducerRecipientMaterializer {
		t.Fatalf("route plan materializer producer = %s, want materializer route", intent.Producer)
	}
	projected := eb.publishRecipientPlan(evt, plan)
	if !deliveryRoutesContain(projected.DeliveryRoutes, want) {
		t.Fatalf("projected delivery routes = %#v, want materialized route %#v", projected.DeliveryRoutes, want)
	}
}

func TestEventBusAgentDispatchIgnoresSameIDNodeRouteTargets(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.SubscribeAgent(testAgentSubscriptionAdmissionForFlow(t, "shared-subscriber", "review/inst-1", events.EventType("review/inst-1/task.started")))
	defer eb.Unsubscribe("shared-subscriber")

	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("review/inst-1/task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

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

func TestEventBusWorkflowRuntimeCarrierPrefersConcreteNodeRouteOverPlaceholder(t *testing.T) {
	eb, err := newScopedTestEventBus(newTargetRouteMemoryStore())
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.SubscribeInternal(workflowRuntimeInternalCarrierID, events.EventType("review/task.started"))
	defer eb.Unsubscribe(workflowRuntimeInternalCarrierID)

	contextRef := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:route-carrier"}}
	evt := eventtest.RootIngress(uuid.NewString(), events.EventType("review/task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	routes := []events.DeliveryRoute{
		{
			SubscriberType: "node",
			SubscriberID:   "review-node",
			Target:         events.RouteIdentity{FlowID: "review", FlowInstance: "review/inst-1", EntityID: "entity-1"},
			Context:        contextRef,
		},
		{
			SubscriberType: "node",
			SubscriberID:   workflowRuntimeInternalCarrierID,
		},
	}
	if err := eb.deliverToRecipientsWithRoutes(context.Background(), evt, []string{workflowRuntimeInternalCarrierID}, routes); err != nil {
		t.Fatalf("deliverToRecipientsWithRoutes: %v", err)
	}
	got := requireBusEvent(t, ch, "workflow runtime concrete route delivery")
	if got.TargetRoute().Normalized() != routes[0].Target.Normalized() {
		t.Fatalf("workflow runtime target = %#v, want %#v", got.TargetRoute(), routes[0].Target)
	}
	if got.DeliveryContext().ReplyContextID() != contextRef.ReplyContextID() {
		t.Fatalf("workflow runtime delivery context = %#v, want %#v", got.DeliveryContext(), contextRef)
	}
	select {
	case extra := <-ch:
		t.Fatalf("workflow runtime received placeholder delivery %#v", extra.TargetRoute())
	case <-time.After(25 * time.Millisecond):
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
		Producer:          routeIntentProducerInternalTargetCarrier,
	})
	plan.AddDeliveryIntents(RoutePlanDeliveryIntent{
		SubscriberType: "node",
		SubscriberID:   nodeID,
		Producer:       routeIntentProducerInternalTargetCarrier,
		Persist:        true,
	})
	return plan.Normalized()
}

func TestEventBusPublish_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = nodeOnlyDeliveryPlanner("workflow-node")
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("custom.node_only"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish node-only route without agent channel: %v", err)
	}
	if got := store.receipts[evt.ID()]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%#v, want processed", got, store.receiptErrs[evt.ID()])
	}
	if routes := store.routes[evt.ID()]; len(routes) != 1 || routes[0].SubscriberType != "node" || routes[0].SubscriberID != "workflow-node" {
		t.Fatalf("delivery routes = %#v, want node/workflow-node", routes)
	}
}

func TestEventBusCommittedPublishDispatch_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("custom.node_only_tx"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	eb.completeCommittedPublishDispatch(context.Background(), evt, nodeOnlyDeliveryPlan(evt, "workflow-node"))

	if got := store.receipts[evt.ID()]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%#v, want processed", got, store.receiptErrs[evt.ID()])
	}
}

func TestEngineDispatcher_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("custom.node_only_outbox"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	store.events[evt.ID()] = evt
	store.routes[evt.ID()] = []events.DeliveryRoute{{SubscriberType: "node", SubscriberID: "workflow-node"}}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{{Event: evt}}); err != nil {
		t.Fatalf("DispatchPostCommit node-only route without agent channel: %v", err)
	}
	if got := store.receipts[evt.ID()]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%#v, want processed", got, store.receiptErrs[evt.ID()])
	}
}

func TestSweepUndispatched_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("custom.node_only_sweep"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	store.events[evt.ID()] = evt
	store.routes[evt.ID()] = []events.DeliveryRoute{{SubscriberType: "node", SubscriberID: "workflow-node"}}
	store.scopes[evt.ID()] = replayclaim.CommittedReplayScopeSubscribed
	store.missing = []events.PersistedReplayEvent{{Event: evt}}
	eb, err := newScopedTestEventBus(store)
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
	if got := store.receipts[evt.ID()]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%#v, want processed", got, store.receiptErrs[evt.ID()])
	}
}

func TestEventBusPublish_MixedNodeAgentRouteStillRequiresAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = mixedNodeAgentDeliveryPlanner("workflow-node", "agent-missing")
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType("custom.mixed_node_agent"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	err = eb.Publish(context.Background(), evt)
	if err == nil {
		t.Fatal("Publish succeeded, want missing agent-channel failure")
	}
	failure, ok := runtimefailures.As(err)
	missing, _ := failure.Failure.Detail.Attributes["missing_recipients"].([]string)
	if !ok || failure.Failure.Detail.Code != "authoritative_delivery_incomplete" || len(missing) != 1 || missing[0] != "agent-missing" {
		t.Fatalf("Publish failure = %#v, want missing agent only", failure)
	}
}

func TestEventBusPublish_TargetSetInternalDeliveryUsesPerTargetRoutes(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
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
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("child/output.done"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{
			{FlowInstance: "child-a/inst-1", EntityID: "ent-a"},
			{FlowInstance: "child-b/inst-1", EntityID: "ent-b"},
		}),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertTargetRouteDeliveries(t, ch, "ent-a", "ent-b")

	persisted := store.events[evt.ID()]
	if got := persisted.EntityID(); got != "" {
		t.Fatalf("persisted EntityID() = %q, want empty target_set projection", got)
	}
	if got := persisted.FlowInstance(); got != "" {
		t.Fatalf("persisted FlowInstance() = %q, want empty target_set projection", got)
	}
	if got := store.routes[evt.ID()]; len(got) != 4 {
		t.Fatalf("persisted delivery routes = %#v, want 4", got)
	}
	wantRoutes := []events.DeliveryRoute{
		{SubscriberType: "node", SubscriberID: "child-a-listener", Target: events.RouteIdentity{FlowInstance: "child-a/inst-1", EntityID: "ent-a"}},
		{SubscriberType: "node", SubscriberID: "child-b-listener", Target: events.RouteIdentity{FlowInstance: "child-b/inst-1", EntityID: "ent-b"}},
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{FlowInstance: "child-a/inst-1", EntityID: "ent-a"}},
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{FlowInstance: "child-b/inst-1", EntityID: "ent-b"}},
	}
	for _, wantRoute := range wantRoutes {
		if !deliveryRoutesContain(store.routes[evt.ID()], wantRoute) {
			t.Fatalf("persisted delivery routes = %#v, missing %#v", store.routes[evt.ID()], wantRoute)
		}
	}

	if err := eb.PublishPersistedRecipients(context.Background(), evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients: %v", err)
	}
	assertTargetRouteDeliveries(t, ch, "ent-a", "ent-b")
}

func TestEventBusPublish_TargetSetSameSemanticNodePersistsPerTargetRoutes(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{
					{ID: "task-handler", Type: "node", Path: "worker/w-001"},
					{ID: "task-handler", Type: "node", Path: "worker/w-002"},
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

	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("worker/work.assign"))
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("worker/work.assign"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{
			{FlowInstance: "worker/w-001", EntityID: "worker/w-001"},
			{FlowInstance: "worker/w-002", EntityID: "worker/w-002"},
		}),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertTargetRouteDeliveries(t, ch, "worker/w-001", "worker/w-002")
	wantRoutes := []events.DeliveryRoute{
		{SubscriberType: "node", SubscriberID: "task-handler", Target: events.RouteIdentity{FlowInstance: "worker/w-001", EntityID: "worker/w-001"}},
		{SubscriberType: "node", SubscriberID: "task-handler", Target: events.RouteIdentity{FlowInstance: "worker/w-002", EntityID: "worker/w-002"}},
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{FlowInstance: "worker/w-001", EntityID: "worker/w-001"}},
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{FlowInstance: "worker/w-002", EntityID: "worker/w-002"}},
	}
	if got := len(store.routes[evt.ID()]); got != len(wantRoutes) {
		t.Fatalf("persisted delivery routes = %#v, want %d same-node target routes", store.routes[evt.ID()], len(wantRoutes))
	}
	for _, wantRoute := range wantRoutes {
		if !deliveryRoutesContain(store.routes[evt.ID()], wantRoute) {
			t.Fatalf("persisted delivery routes = %#v, missing %#v", store.routes[evt.ID()], wantRoute)
		}
	}
}

func TestEventBusPublish_TargetedRouteTableNodePersistsSemanticNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{ID: "task-handler", Type: "node", Path: "worker/w-001"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "task-handler", Type: "node", Path: "worker/w-001"}}
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("worker/work.assign"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowInstance: "worker/w-001", EntityID: "worker/w-001"}),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	wantRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "task-handler",
		Target:         events.RouteIdentity{FlowInstance: "worker/w-001", EntityID: "worker/w-001"},
	}
	if got := store.routes[evt.ID()]; len(got) != 1 || !deliveryRoutesContain(got, wantRoute) {
		t.Fatalf("persisted delivery routes = %#v, want semantic node route %#v", got, wantRoute)
	}
}

func TestEventBusPublish_TargetedTemplateInstanceRouteTableNodePersistsSemanticNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(routedNodeTemplateBundle())
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("operating/opco.product_initialization_requested"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowInstance: "operating/inst-1", EntityID: "ent-operating"}),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "lifecycle-orchestrator" || plan.RoutedRecipients[0].Path != "operating/inst-1" {
		t.Fatalf("routed recipients = %#v, want targeted lifecycle-orchestrator concrete instance route", plan.RoutedRecipients)
	}
	wantRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "lifecycle-orchestrator",
		Target: events.RouteIdentity{
			FlowInstance: "operating/inst-1",
			EntityID:     "ent-operating",
		},
	}
	if len(plan.DeliveryRoutes) != 1 || !deliveryRoutesContain(plan.DeliveryRoutes, wantRoute) {
		t.Fatalf("plan delivery routes = %#v, want semantic node route %#v", plan.DeliveryRoutes, wantRoute)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.routes[evt.ID()]; len(got) != 1 || !deliveryRoutesContain(got, wantRoute) {
		t.Fatalf("persisted delivery routes = %#v, want semantic node route %#v", got, wantRoute)
	}
}

func TestEventBusPublish_TargetedDynamicFlowFixtureRouteTableNodePersistsSemanticNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-dynamic-flow-instance")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load fixture bundle: %v", err)
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("worker", "w-001")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	materialized := eb.RouteTable().MaterializedRoutes(runtimeflowidentity.DeriveRoute("worker", "w-001"))
	hasRoute := func(eventPattern string) bool {
		for _, route := range materialized {
			if route.EventPattern == eventPattern && route.SubscriberID == "task-handler" {
				return true
			}
		}
		return false
	}
	if !hasRoute("worker/w-001/work.assign") {
		t.Fatalf("materialized routes = %#v, want task-handler instance-scoped work.assign route; node entries=%v", materialized, sortedStringKeys(bundle.NodeEntries()))
	}
	if subscriberListContainsRouteSource(eb.RouteTable().Resolve("worker/w-001/work.assign"), "task-handler", "worker/w-001", "receiver_carrier") {
		t.Fatalf("Resolve(worker/w-001/work.assign) = %#v, want no receiver_carrier route for unrelated target-route fixture", eb.RouteTable().Resolve("worker/w-001/work.assign"))
	}
	if resolved := eb.RouteTable().Resolve("worker/w-001/work.assign"); subscriberListContainsRouteSource(resolved, "task-handler", "worker/w-001", "receiver_carrier") {
		t.Fatalf("Resolve(worker/w-001/work.assign) = %#v, want no receiver_carrier route for unrelated target-route fixture", resolved)
	}
	target := events.RouteIdentity{
		FlowInstance: "worker/w-001",
		EntityID:     runtimeflowidentity.EntityID("worker/w-001"),
	}
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("worker/work.assign"),
		"",
		"",
		[]byte(`{"task_label":"route-me"}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, target),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	foundConcreteSubscriber := false
	for _, recipient := range plan.RoutedRecipients {
		if recipient.ID == "task-handler" && recipient.Path == "worker/w-001" {
			foundConcreteSubscriber = true
			break
		}
	}
	if !foundConcreteSubscriber {
		t.Fatalf("routed recipients = %#v, want targeted task-handler concrete worker route", plan.RoutedRecipients)
	}
	wantRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "task-handler",
		Target:         target,
	}
	if len(plan.DeliveryRoutes) != 1 || !deliveryRoutesContain(plan.DeliveryRoutes, wantRoute) {
		t.Fatalf("plan delivery routes = %#v, want semantic node route %#v", plan.DeliveryRoutes, wantRoute)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.routes[evt.ID()]; len(got) != 1 || !deliveryRoutesContain(got, wantRoute) {
		t.Fatalf("persisted delivery routes = %#v, want semantic node route %#v", got, wantRoute)
	}
}

func TestEventBusPublish_NoTargetConcreteRoutedNodePersistsSemanticNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(routedNodeTemplateBundle())
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("operating/inst-1/opco.product_initialization_requested"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-operating"), "operating/inst-1"),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "concrete routed node event delivery")
	if got.FlowInstance() != "operating/inst-1" {
		t.Fatalf("delivered flow instance = %q, want operating/inst-1", got.FlowInstance())
	}

	routes := store.routes[evt.ID()]
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
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("operating/opco.product_initialization_requested"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-operating"), "operating/inst-1"),
		time.Now().UTC(),
	)

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
	routes := store.routes[evt.ID()]
	if len(routes) != 1 || routes[0].Target.FlowInstance != "operating/inst-1" {
		t.Fatalf("persisted delivery routes = %#v, want concrete operating route", routes)
	}
}

func TestEventBusPublish_RuntimeCallbackLocalEventPersistsSameFlowNodeRouteBeforeInternalCarrier(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
	}{
		{name: "success callback", eventType: "repo_scaffold.repo_commit_succeeded"},
		{name: "failure callback", eventType: "repo_scaffold.repo_commit_failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newTargetRouteMemoryStore()
			eventID := uuid.NewString()
			want := events.DeliveryRoute{
				SubscriberType: "node",
				SubscriberID:   "repo-scaffold-node",
				Target: events.RouteIdentity{
					FlowInstance: "repo-scaffold/inst-1",
					EntityID:     "ent-repo",
				},
			}
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: semanticview.Wrap(routedCallbackTemplateBundle()),
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
			if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("repo-scaffold", "inst-1")}); err != nil {
				t.Fatalf("AddFlowInstanceRoute: %v", err)
			}
			concreteEventType := "repo-scaffold/inst-1/" + tc.eventType
			ch := eb.SubscribeInternal("workflow-runtime", events.EventType(concreteEventType))
			defer eb.Unsubscribe("workflow-runtime")
			evt := eventtest.RootIngress(
				eventID,
				events.EventType(tc.eventType),
				"workflow-runtime",
				"",
				[]byte(`{}`),
				0,
				"",
				"",
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-repo"), "repo-scaffold/inst-1"),
				time.Now().UTC(),
			)

			plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if got := plan.Recipients; len(got) != 1 || got[0] != "workflow-runtime" {
				t.Fatalf("recipients = %#v, want workflow-runtime live carrier", got)
			}
			if len(plan.PersistedRecipients) != 0 {
				t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipients)
			}
			if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "repo-scaffold-node" || plan.RoutedRecipients[0].Path != "repo-scaffold/inst-1" {
				t.Fatalf("routed recipients = %#v, want repo-scaffold-node concrete instance route", plan.RoutedRecipients)
			}
			if got := plan.DeliveryRoutes; len(got) != 1 || !deliveryRoutesContain(got, want) {
				t.Fatalf("delivery routes = %#v, want runtime callback node route %#v", got, want)
			}

			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			got := requireBusEvent(t, ch, "runtime callback workflow-runtime carrier delivery")
			if got.Type() != events.EventType(tc.eventType) || got.FlowInstance() != "repo-scaffold/inst-1" || got.EntityID() != "ent-repo" {
				t.Fatalf("delivered event type=%q flow=%q entity=%q, want callback local event in repo-scaffold/inst-1 ent-repo", got.Type(), got.FlowInstance(), got.EntityID())
			}
			routes := store.routes[evt.ID()]
			if len(routes) != 1 || !deliveryRoutesContain(routes, want) {
				t.Fatalf("persisted delivery routes = %#v, want callback route %#v", routes, want)
			}
		})
	}
}

func TestEventBusCheckPublishRecipientPlan_SemanticScopeFlowInstanceMaterializesNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedNodeStaticValidationBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("validation/thing.reviewed"))
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("validation/thing.reviewed"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-validation"), "validation/inst-1"),
		time.Now().UTC(),
	)

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
	if route.SubscriberType != "node" || route.SubscriberID != "entity-writer" {
		t.Fatalf("delivery route = %#v, want node/entity-writer", route)
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
	routes := store.routes[evt.ID()]
	if len(routes) != 1 || routes[0].SubscriberID != "entity-writer" || routes[0].Target.FlowInstance != "validation/inst-1" {
		t.Fatalf("persisted routes = %#v, want entity-writer concrete validation route", routes)
	}
}

func TestEventBusCheckPublishRecipientPlan_SemanticScopeFlowInstanceMaterializesSystemNodeRouteWithoutLiveSubscription(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedNodeStaticValidationBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("validation/thing.reviewed"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-validation"), "validation/inst-1"),
		time.Now().UTC(),
	)

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
	routes := store.routes[evt.ID()]
	if len(routes) != 1 || routes[0].SubscriberID != "entity-writer" || routes[0].Target.FlowInstance != "validation/inst-1" {
		t.Fatalf("persisted routes = %#v, want entity-writer concrete validation route", routes)
	}
}

func TestEventBusPublish_NoTargetScopedRoutedNodePersistsSemanticRouteBeforeInternalCarrier(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedNodeStaticChildBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("child/child.start"))
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("child/child.start"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
		time.Now().UTC(),
	)

	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "child-intake",
		Target: events.RouteIdentity{
			FlowInstance: "child",
			EntityID:     "ent-child",
		},
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got := plan.Recipients; len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want workflow-runtime live carrier", got)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipients)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "child-intake" || plan.RoutedRecipients[0].Path != "child" {
		t.Fatalf("routed recipients = %#v, want child/child-intake", plan.RoutedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 || !deliveryRoutesContain(got, want) {
		t.Fatalf("delivery routes = %#v, want child-intake scoped route %#v", got, want)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "static scoped workflow-runtime carrier delivery")
	if got.FlowInstance() != "child" || got.EntityID() != "ent-child" {
		t.Fatalf("delivered identity flow=%q entity=%q, want child target ent-child", got.FlowInstance(), got.EntityID())
	}
	routes := store.routes[evt.ID()]
	if len(routes) != 1 || !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
}

func TestEventBusPublish_WildcardStaticServiceNodePersistsRouteBeforeInternalCarrier(t *testing.T) {
	const (
		pattern   = "component-scaffold/*/opco.repo_scaffold_requested"
		eventType = "component-scaffold/component-a/opco.repo_scaffold_requested"
	)
	rt := newRouteTable(nil)
	rt.eventPath[eventType] = struct{}{}
	rt.patterns = []routePattern{
		{
			EventPattern: eventType,
			Subscriber: Subscriber{
				ID:          "component-node",
				Type:        "node",
				Path:        "component-scaffold/component-a",
				RouteSource: "subscription",
			},
		},
		{
			EventPattern: pattern,
			Subscriber: Subscriber{
				ID:          "repo-scaffold-node",
				Type:        "node",
				Path:        "repo-scaffold",
				RouteSource: "subscription",
			},
		},
	}
	rt.rebuildLocked()

	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	wildcardServiceRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "repo-scaffold-node",
		Target: events.RouteIdentity{
			FlowInstance: "repo-scaffold",
			EntityID:     "ent-component",
		},
	}
	concreteComponentRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "component-node",
		Target: events.RouteIdentity{
			FlowInstance: "component-scaffold/component-a",
			EntityID:     "ent-component",
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		RouteTable: rt,
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    wildcardServiceRoute,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType(eventType))
	defer eb.Unsubscribe("workflow-runtime")
	evt := eventtest.RootIngress(
		eventID,
		events.EventType(eventType),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-component"), "component-scaffold/component-a"),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got := plan.Recipients; len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want workflow-runtime live carrier", got)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipients)
	}
	if len(plan.RoutedRecipients) != 2 {
		t.Fatalf("routed recipients = %#v, want component node and repo-scaffold wildcard match", plan.RoutedRecipients)
	}
	sawWildcardService := false
	for _, recipient := range plan.RoutedRecipients {
		if recipient.ID == "repo-scaffold-node" && recipient.Path == "repo-scaffold" && recipient.MatchedPattern == pattern {
			sawWildcardService = true
		}
	}
	if !sawWildcardService {
		t.Fatalf("routed recipients = %#v, want repo-scaffold wildcard match", plan.RoutedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 2 || !deliveryRoutesContain(got, wildcardServiceRoute) || !deliveryRoutesContain(got, concreteComponentRoute) {
		t.Fatalf("delivery routes = %#v, want wildcard service route %#v and concrete route %#v", got, wildcardServiceRoute, concreteComponentRoute)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "wildcard static-service workflow-runtime carrier delivery")
	if got.Type() != events.EventType(eventType) || got.FlowInstance() != "component-scaffold/component-a" {
		t.Fatalf("delivered event type=%q flow=%q, want concrete component event", got.Type(), got.FlowInstance())
	}
	routes := store.routes[evt.ID()]
	if len(routes) != 2 || !deliveryRoutesContain(routes, wildcardServiceRoute) || !deliveryRoutesContain(routes, concreteComponentRoute) {
		t.Fatalf("persisted delivery routes = %#v, want wildcard service route %#v and concrete route %#v", routes, wildcardServiceRoute, concreteComponentRoute)
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
	eb, err := newScopedTestEventBus(store, EventBusOptions{
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
	evt := eventtest.RootIngress(
		eventID,
		events.EventType("thing.created"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-root-input"),
		time.Now().UTC(),
	)

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
	routes := store.routes[evt.ID()]
	if !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.scopes[evt.ID()]; got != replayclaim.CommittedReplayScopeSubscribed {
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
	eb, err := newScopedTestEventBus(store, EventBusOptions{
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
	evt := eventtest.RootIngress(
		eventID,
		events.EventType("thing.created"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-root-input"),
		time.Now().UTC(),
	)

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
	if routes := store.routes[evt.ID()]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.scopes[evt.ID()]; got != replayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
}

func TestEventBusPublish_LoadedRootInputProjectEventPersistsRouteBeforeDispatch(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "item-handler",
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t),
		canonicalrouting.ExampleRoot(t, canonicalrouting.RootIngress),
		runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
	if err != nil {
		t.Fatalf("load canonical root ingress: %v", err)
	}
	source := semanticview.Wrap(bundle)
	rt, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	resolved := rt.Resolve("item.received")
	if len(resolved) != 1 || resolved[0].ID != "item-handler" || resolved[0].Path != "" || resolved[0].RouteSource != "subscription" {
		t.Fatalf("resolved subscribers = %#v, want canonical same-flow item-handler subscription", resolved)
	}

	eb, err := newScopedTestEventBus(store, EventBusOptions{
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
	ch := eb.SubscribeInternal("workflow-runtime", events.EventType("item.received"))
	evt := eventtest.RootIngress(
		eventID,
		events.EventType("item.received"),
		"",
		"",
		[]byte(`{"item_id":"item-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-loaded-root-input"),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal root-input node carrier", plan.PersistedRecipients)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "item-handler" || plan.RoutedRecipients[0].Path != "" || plan.RoutedRecipients[0].RouteSource != "subscription" {
		t.Fatalf("routed recipients = %#v, want canonical same-flow item-handler", plan.RoutedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 || !deliveryRoutesContain(got, want) {
		t.Fatalf("delivery routes = %#v, want empty-target node/item-handler route", got)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "loaded root-input flow-node carrier delivery")
	if got.FlowInstance() != "" || got.EntityID() != "ent-loaded-root-input" {
		t.Fatalf("delivered root input identity flow=%q entity=%q, want root ent-loaded-root-input", got.FlowInstance(), got.EntityID())
	}
	if routes := store.routes[evt.ID()]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
}

func TestEventBusPublish_CanonicalParentConnectPersistsSingularStaticRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t),
		canonicalrouting.ExampleRoot(t, canonicalrouting.ParentConnect),
		runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
	if err != nil {
		t.Fatalf("load canonical parent connect: %v", err)
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress(
		uuid.NewString(), events.EventType("producer/work.ready"), "", "",
		[]byte(`{"work_id":"work-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC(),
	)
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 {
		t.Fatalf("parent-connect plan failure/routes = %q/%#v", plan.TargetFailure, plan.DeliveryRoutes)
	}
	want := plan.DeliveryRoutes[0]
	if want.SubscriberType != "node" || want.SubscriberID != "consumer-node" ||
		want.Target.FlowID != "consumer" || want.Target.FlowInstance != "consumer" || want.Target.EntityID == "" {
		t.Fatalf("parent-connect route = %#v, want singular static consumer identity", want)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !deliveryRoutesContain(store.routes[evt.ID()], want) {
		t.Fatalf("persisted parent-connect routes = %#v, want %#v", store.routes[evt.ID()], want)
	}
}

func TestEventBusPublish_NoTargetRootRoutedNodeUsesSemanticNodeDeliveryRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, routedRootNodeFixtureFiles()))
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := eb.SubscribeInternal("portfolio-node", events.EventType("opco.spinup_requested"))
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("opco.spinup_requested"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-root"),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "root routed node event delivery")
	if got.FlowInstance() != "" {
		t.Fatalf("delivered flow instance = %q, want root event", got.FlowInstance())
	}

	routes := store.routes[evt.ID()]
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
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("opco.spinup_requested"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-root"),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	routes := store.routes[evt.ID()]
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
	if got := store.scopes[evt.ID()]; got != replayclaim.CommittedReplayScopeSubscribed {
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

func TestRouteTableTopLevelProjectNodeResolvesProgrammaticRootInputRoute(t *testing.T) {
	rt, err := DeriveRouteTable(semanticview.Wrap(routedTopLevelProjectNodeBundle()))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := rt.Resolve("thing.created")
	if len(got) != 1 {
		t.Fatalf("Resolve(thing.created) = %#v, want one top-level project node route", got)
	}
	if got[0].ID != "reviewer" || got[0].Type != "node" || got[0].Path != "" || got[0].MatchPattern != "thing.created" || got[0].RouteSource != "root_input_project" {
		t.Fatalf("resolved subscriber = %#v, want root input project reviewer route", got[0])
	}
}

func TestEventBusPublish_TopLevelProjectNodePersistsRouteBeforeInterceptor(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	reviewerRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "reviewer",
	}
	workflowRuntimeRoute := events.DeliveryRoute{
		SubscriberType: "agent",
		SubscriberID:   "workflow-runtime",
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedTopLevelProjectNodeBundle()),
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    reviewerRoute,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eb.RegisterRuntimeActiveAgentDescriptor(ActiveAgentDescriptor{AgentID: "workflow-runtime"})
	ch := eb.Subscribe("workflow-runtime", events.EventType("thing.created"))
	evt := eventtest.RootIngress(
		eventID,
		events.EventType("thing.created"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-project"),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got := plan.PersistedRecipients; len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("persisted recipients = %#v, want workflow-runtime carrier", got)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, reviewerRoute) || !deliveryRoutesContain(plan.DeliveryRoutes, workflowRuntimeRoute) {
		t.Fatalf("delivery routes = %#v, want workflow-runtime agent and reviewer node routes", plan.DeliveryRoutes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "workflow-runtime carrier delivery")
	if got.EntityID() != "ent-project" {
		t.Fatalf("delivered entity_id = %q, want ent-project", got.EntityID())
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, reviewerRoute) || !deliveryRoutesContain(routes, workflowRuntimeRoute) {
		t.Fatalf("persisted delivery routes = %#v, want workflow-runtime agent and reviewer node routes", routes)
	}
}

func TestEventBusPublish_NodeRouteFailsClosedWithoutRouteSetPersistence(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedTopLevelProjectNodeBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RootIngress(uuid.NewString(),
		events.EventType("thing.created"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err == nil || !strings.Contains(err.Error(), "typed delivery route persistence") {
		t.Fatalf("Publish error = %v, want typed delivery route persistence failure", err)
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

func routedTopLevelProjectNodeBundle() *runtimecontracts.WorkflowContractBundle {
	handler := runtimecontracts.SystemNodeEventHandler{AdvancesTo: "done"}
	return &runtimecontracts.WorkflowContractBundle{
		Package: runtimecontracts.ProjectPackageDocument{
			Name:    "top-level-project-node",
			Version: "1.0.0",
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "top-level-project-node",
			Version: "1.0.0",
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"reviewer": {"thing.created": handler},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"thing.created": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"reviewer": {
				ID:            "reviewer",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"thing.created"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"thing.created": handler,
				},
			},
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

func routedCallbackTemplateBundle() *runtimecontracts.WorkflowContractBundle {
	repoScaffold := runtimecontracts.FlowContractView{
		Path:  "repo-scaffold",
		Paths: runtimecontracts.FlowContractPaths{ID: "repo-scaffold", Flow: "repo-scaffold"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"repo_scaffold.repo_commit_succeeded": {},
			"repo_scaffold.repo_commit_failed":    {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"repo-scaffold-node": {
				ID:            "repo-scaffold-node",
				ExecutionType: "system_node",
				SubscribesTo: []string{
					"repo_scaffold.repo_commit_succeeded",
					"repo_scaffold.repo_commit_failed",
				},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"repo_scaffold.repo_commit_succeeded": {},
					"repo_scaffold.repo_commit_failed":    {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{repoScaffold}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"repo-scaffold": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"repo-scaffold": {Mode: "template"},
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

func routedNodeStaticChildBundle() *runtimecontracts.WorkflowContractBundle {
	child := runtimecontracts.FlowContractView{
		Path:  "child",
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child.start": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"child-intake": {
				ID:            "child-intake",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"child.start"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"child.start": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"child": {},
		},
	}
}
