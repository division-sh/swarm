package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type routeGenerationPlanBarrier struct {
	started chan struct{}
	release chan struct{}
}

func (b routeGenerationPlanBarrier) Intercept(context.Context, events.Event) (bool, []events.Event, error) {
	select {
	case b.started <- struct{}{}:
	default:
	}
	<-b.release
	return true, nil, nil
}

func TestEventBusSubscribedPublishDoesNotCrossAgentRouteGenerations(t *testing.T) {
	for _, test := range []struct {
		name                string
		successorEventTypes []events.EventType
	}{
		{name: "removed subscription", successorEventTypes: []events.EventType{"test.successor"}},
		{name: "retained subscription", successorEventTypes: []events.EventType{"test.route_generation"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTargetRouteMemoryStore()
			barrier := routeGenerationPlanBarrier{started: make(chan struct{}, 1), release: make(chan struct{})}
			eb, err := newScopedTestEventBus(store, EventBusOptions{Interceptors: []EventInterceptor{barrier}})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			const agentID = "route-generation-agent"
			oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 11, AgentID: agentID, Generation: 1}
			newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 11, AgentID: agentID, Generation: 2}
			oldCh := eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.route_generation")))
			evt := routeGenerationTestEvent(eventtest.UUID("route-generation-publish"), "test.route_generation")

			publishDone := make(chan error, 1)
			go func() { publishDone <- eb.Publish(context.Background(), evt) }()
			requireRouteGenerationSignal(t, barrier.started, "publish interceptor")

			newCh := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, test.successorEventTypes...))
			close(barrier.release)
			if err := requireRouteGenerationResult(t, publishDone, "Publish"); !errors.Is(err, errAuthoritativeDeliveryIncomplete) {
				t.Fatalf("Publish error = %v, want authoritative delivery incomplete", err)
			}
			assertNoRouteGenerationEvent(t, oldCh, "detached predecessor")
			assertNoRouteGenerationEvent(t, newCh, "successor before replay")
			assertNoRouteGenerationReceipt(t, store, evt.ID())

			eb.SetInterceptors()
			if err := eb.RecoverPersistedPipeline(context.Background(), evt, []string{agentID}); err != nil {
				t.Fatalf("RecoverPersistedPipeline: %v", err)
			}
			select {
			case got := <-newCh:
				if got.ID() != evt.ID() {
					t.Fatalf("replayed event id = %q, want %q", got.ID(), evt.ID())
				}
			case <-time.After(time.Second):
				t.Fatal("persisted identity replay did not reach the current route generation")
			}
			assertNoRouteGenerationEvent(t, newCh, "successor after one replay")
		})
	}
}

func TestEventBusPublishInMutationDoesNotCrossAgentRouteGenerations(t *testing.T) {
	store := &connectRoutePlanMutationStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}
	barrier := routeGenerationPlanBarrier{started: make(chan struct{}, 1), release: make(chan struct{})}
	eb, err := newScopedTestEventBus(store, EventBusOptions{Interceptors: []EventInterceptor{barrier}})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	const agentID = "route-generation-mutation-agent"
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 12, AgentID: agentID, Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 12, AgentID: agentID, Generation: 2}
	oldCh := eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.route_generation_mutation")))
	evt := routeGenerationTestEvent(eventtest.UUID("route-generation-mutation"), "test.route_generation_mutation")

	mutationDone := make(chan error, 1)
	go func() {
		mutationDone <- runConnectRoutePlanCommitScope(context.Background(), store, func(commitCtx context.Context) error {
			return eb.PublishInMutation(commitCtx, evt)
		})
	}()
	requireRouteGenerationSignal(t, barrier.started, "post-commit interceptor")
	newCh := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.route_generation_mutation")))
	close(barrier.release)
	if err := requireRouteGenerationResult(t, mutationDone, "RunEventMutation"); err != nil {
		t.Fatalf("RunEventMutation: %v", err)
	}
	assertNoRouteGenerationEvent(t, oldCh, "detached predecessor")
	assertNoRouteGenerationEvent(t, newCh, "transactional successor")
	assertNoRouteGenerationReceipt(t, store.targetRouteMemoryStore, evt.ID())
}

func TestEventBusPublishAcknowledgedDoesNotCrossAgentRouteGenerations(t *testing.T) {
	store := &connectRoutePlanMutationStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}
	barrier := routeGenerationPlanBarrier{started: make(chan struct{}, 1), release: make(chan struct{})}
	eb, err := newScopedTestEventBus(store, EventBusOptions{Interceptors: []EventInterceptor{barrier}})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	const agentID = "route-generation-ack-agent"
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 13, AgentID: agentID, Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 13, AgentID: agentID, Generation: 2}
	oldCh := eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.route_generation_ack")))
	evt := routeGenerationTestEvent(eventtest.UUID("route-generation-ack"), "test.route_generation_ack")

	if err := eb.PublishAcknowledged(context.Background(), evt); err != nil {
		t.Fatalf("PublishAcknowledged: %v", err)
	}
	requireRouteGenerationSignal(t, barrier.started, "acknowledged interceptor")
	newCh := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.route_generation_ack")))
	close(barrier.release)
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}
	assertNoRouteGenerationEvent(t, oldCh, "detached predecessor")
	assertNoRouteGenerationEvent(t, newCh, "acknowledged successor")
	assertNoRouteGenerationReceipt(t, store.targetRouteMemoryStore, evt.ID())
}

func TestEventBusIdentityRoutePlanResolvesCurrentAgentGenerationAtDispatch(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	const agentID = "identity-route-generation-agent"
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 14, AgentID: agentID, Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 14, AgentID: agentID, Generation: 2}
	oldCh := eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.identity_route")))
	evt := routeGenerationTestEvent("identity-route-generation", "test.identity_route")
	plan, err := eb.planDirectRoutePlan(context.Background(), evt, []string{agentID})
	if err != nil {
		t.Fatalf("planDirectRoutePlan: %v", err)
	}
	if len(plan.LiveRecipients) != 1 || plan.LiveRecipients[0].liveAuthority != liveRecipientAuthorityIdentity {
		t.Fatalf("direct live recipients = %#v, want one identity-authoritative recipient", plan.LiveRecipients)
	}
	newCh := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.identity_route")))
	if err := eb.deliverRoutePlanWithRoutes(context.Background(), evt, plan); err != nil {
		t.Fatalf("deliverRoutePlanWithRoutes: %v", err)
	}
	assertNoRouteGenerationEvent(t, oldCh, "detached identity predecessor")
	select {
	case got := <-newCh:
		if got.ID() != evt.ID() {
			t.Fatalf("identity-dispatched event id = %q, want %q", got.ID(), evt.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("identity-authoritative plan did not resolve the current route generation")
	}
}

func TestEventBusIdentityRouteAuthorityDominatesDuplicateExactSubscription(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	const agentID = "duplicate-route-authority-agent"
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: 15, AgentID: agentID, Generation: 1}
	eb.ReplaceAgentRoute(token, testAgentSubscriptionAdmission(t, token.AgentID, events.EventType("test.duplicate_route")))
	eb.mu.RLock()
	route := eb.agentRouteHandles[agentID]
	eb.mu.RUnlock()
	candidates := normalizeDeliveryRecipientCandidates([]deliveryRecipientCandidate{
		{ID: agentID, PersistAsDelivery: true, LiveAuthority: liveRecipientAuthorityIdentity},
		{ID: agentID, PersistAsDelivery: true, LiveAuthority: liveRecipientAuthorityAgentRoute, AgentRoute: route},
	})
	if len(candidates) != 1 {
		t.Fatalf("normalized candidates = %#v, want one", candidates)
	}
	if candidates[0].LiveAuthority != liveRecipientAuthorityIdentity || candidates[0].AgentRoute != nil {
		t.Fatalf("duplicate authority = %#v, want identity without an exact route handle", candidates[0])
	}
	planRecipients := normalizeRoutePlanLiveRecipients([]RoutePlanLiveRecipient{
		{RecipientID: agentID, SubscriberType: routePlanSubscriberAgent, PersistAsDelivery: true, liveAuthority: liveRecipientAuthorityIdentity},
		{RecipientID: agentID, SubscriberType: routePlanSubscriberAgent, PersistAsDelivery: true, liveAuthority: liveRecipientAuthorityAgentRoute, agentRoute: route},
	})
	if len(planRecipients) != 1 || planRecipients[0].liveAuthority != liveRecipientAuthorityIdentity || planRecipients[0].agentRoute != nil {
		t.Fatalf("normalized route-plan recipients = %#v, want identity authority to dominate", planRecipients)
	}
}

func routeGenerationTestEvent(id, eventType string) events.Event {
	return eventtest.RunCreatingRootIngress(
		eventtest.UUID(id),
		events.EventType(eventType),
		"route-generation-test",
		"",
		[]byte(`{}`),
		0,
		eventtest.UUID("route-generation-run"),
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
}

func requireRouteGenerationSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func requireRouteGenerationResult(t *testing.T, ch <-chan error, label string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", label)
		return nil
	}
}

func assertNoRouteGenerationEvent(t *testing.T, ch <-chan *LocalDelivery, label string) {
	t.Helper()
	select {
	case delivery := <-ch:
		_ = delivery.Complete()
		t.Fatalf("%s received event %q", label, delivery.ID())
	default:
	}
}

func assertNoRouteGenerationReceipt(t *testing.T, store *targetRouteMemoryStore, eventID string) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	if status := store.receipts[eventID]; status != "" {
		t.Fatalf("pipeline receipt = %q, want pending without a terminal receipt", status)
	}
}
