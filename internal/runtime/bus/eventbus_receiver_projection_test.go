package bus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

type publisherOnlyContextKey struct{}

type receiverProjectionStore struct{ InMemoryEventStore }

func (receiverProjectionStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}

type receiverProjectionEffectStore struct{}

func (receiverProjectionEffectStore) IsExternalEffectAuthorityCurrent(context.Context, runtimeeffects.Authority) (bool, error) {
	return true, nil
}
func (receiverProjectionEffectStore) AuthorizeExternalAttempt(context.Context, runtimeeffects.Authority, runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	return runtimeeffects.Attempt{}, nil
}
func (receiverProjectionEffectStore) MarkExternalAttemptLaunched(context.Context, runtimeeffects.Attempt, time.Time) error {
	return nil
}
func (receiverProjectionEffectStore) MarkExternalAttemptResponseObserved(context.Context, runtimeeffects.Attempt, map[string]any, time.Time) error {
	return nil
}
func (receiverProjectionEffectStore) SettleExternalAttempt(context.Context, runtimeeffects.Settlement) error {
	return nil
}

type receiverProjectionInterceptor struct {
	eventErr error
	routeErr error
}

func (i *receiverProjectionInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	i.eventErr = validateClosedReceiverContext(ctx, evt)
	return true, nil, runtimepipelineobligation.Continue(), i.eventErr
}

func (i *receiverProjectionInterceptor) InterceptDeliveryRoute(ctx context.Context, evt events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	i.routeErr = validateClosedReceiverContext(ctx, evt.Event())
	if i.routeErr == nil {
		got, ok := runtimedeliveryRouteFromContext(ctx)
		if !ok || !sameReceiverRoute(got, route) {
			i.routeErr = fmt.Errorf("receiver route = %#v, %v; want %#v", got, ok, route.Normalized())
		}
	}
	return false, nil, runtimepipelineobligation.Continue(), i.routeErr
}

func TestEventBusEventInterceptorReceiverProjectionRejectsPublisherState(t *testing.T) {
	interceptor := &receiverProjectionInterceptor{}
	eventBus, err := newScopedTestEventBus(receiverProjectionStore{}, EventBusOptions{Interceptors: []EventInterceptor{interceptor}})
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	evt := receiverProjectionEvent("event-wide")
	if err := eventBus.Publish(hostilePublisherContext(t), evt); err != nil {
		t.Fatalf("publish through event-wide receiver: %v", err)
	}
	if interceptor.eventErr != nil {
		t.Fatal(interceptor.eventErr)
	}
}

func TestPersistedReplayUsesClosedReceiverProjection(t *testing.T) {
	interceptor := &receiverProjectionInterceptor{}
	eventBus, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{Interceptors: []EventInterceptor{interceptor}})
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	evt := receiverProjectionEvent("persisted-replay")
	if _, err := eventBus.publishPersistedRecipientsWithScope(
		hostilePublisherContext(t), evt, runtimepipelineobligation.ScopeSubscribed, nil, true, false,
	); err != nil {
		t.Fatalf("replay persisted receiver: %v", err)
	}
	if interceptor.eventErr != nil {
		t.Fatal(interceptor.eventErr)
	}
}

func TestEngineOutboxUsesClosedReceiverProjection(t *testing.T) {
	interceptor := &receiverProjectionInterceptor{}
	eventBus, err := newScopedTestEventBus(receiverProjectionStore{}, EventBusOptions{Interceptors: []EventInterceptor{interceptor}})
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	evt := receiverProjectionEvent("engine-outbox")
	if err := eventBus.EngineDispatcher().DispatchPostCommit(hostilePublisherContext(t), []runtimeengine.EmitIntent{{Event: evt}}); err != nil {
		t.Fatalf("dispatch engine outbox receiver: %v", err)
	}
	if interceptor.eventErr != nil {
		t.Fatal(interceptor.eventErr)
	}
}

func TestDeliveryContinuationUsesClosedReceiverProjection(t *testing.T) {
	interceptor := &receiverProjectionInterceptor{}
	eventBus, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{Interceptors: []EventInterceptor{interceptor}})
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	evt := eventtest.RuntimeControl(
		uuid.NewString(), events.EventType("custom.continuation_receiver_projection"), "receiver-projection-test", "", []byte(`{}`), 0,
		"", "", events.EventEnvelope{}, time.Now().UTC(),
	)
	route := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "workflow-node",
		Context:        events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-continuation"}},
	}
	if err := eventBus.DispatchDeliveryContinuation(hostilePublisherContext(t), evt, route); err != nil {
		t.Fatalf("dispatch continuation receiver: %v", err)
	}
	if interceptor.routeErr != nil {
		t.Fatal(interceptor.routeErr)
	}
}

func TestEventBusNodeRouteReceiverProjectionRejectsPublisherState(t *testing.T) {
	interceptor := &receiverProjectionInterceptor{}
	eventBus, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{Interceptors: []EventInterceptor{interceptor}})
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	eventBus.deliveryPlanner = nodeOnlyDeliveryPlanner("workflow-node")
	evt := receiverProjectionEvent("node-route")
	if err := eventBus.Publish(hostilePublisherContext(t), evt); err != nil {
		t.Fatalf("publish through node-route receiver: %v", err)
	}
	if interceptor.eventErr != nil {
		t.Fatalf("event-wide receiver: %v", interceptor.eventErr)
	}
	if interceptor.routeErr != nil {
		t.Fatalf("node-route receiver: %v", interceptor.routeErr)
	}
}

func TestAgentRouteUsesClosedReceiverProjection(t *testing.T) {
	eventBus, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	eventType := events.EventType("custom.agent_receiver_projection")
	deliveries := subscribeTestAgent(t, eventBus, "agent-a", eventType)
	evt := receiverProjectionEventForType("agent-route", eventType)
	if err := eventBus.Publish(hostilePublisherContext(t), evt); err != nil {
		t.Fatalf("publish through agent receiver: %v", err)
	}
	delivery := <-deliveries
	if err := validateClosedReceiverContext(delivery.Context(), evt); err != nil {
		t.Fatal(err)
	}
	if route, ok := runtimedeliveryRouteFromContext(delivery.Context()); !ok || route.SubscriberType != "agent" || route.SubscriberID != "agent-a" {
		t.Fatalf("agent receiver route = %#v, %v", route, ok)
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete agent delivery: %v", err)
	}
}

func TestInternalSubscriptionUsesClosedReceiverProjection(t *testing.T) {
	eventBus, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	eventType := events.EventType("custom.internal_receiver_projection")
	deliveries := subscribeInternalDeliveriesForTest(t, eventBus, "internal-owner", eventType)
	evt := receiverProjectionEventForType("internal-route", eventType)
	route := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "internal-owner",
		Context:        evt.DeliveryContext(),
	}
	if err := eventBus.deliverToRecipientsWithRoutes(hostilePublisherContext(t), evt, []string{"internal-owner"}, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("publish through internal receiver: %v", err)
	}
	delivery := <-deliveries
	if err := validateClosedReceiverContext(delivery.Context(), evt); err != nil {
		t.Fatal(err)
	}
	if route, ok := runtimedeliveryRouteFromContext(delivery.Context()); !ok || route.SubscriberType != "node" || route.SubscriberID != "internal-owner" {
		t.Fatalf("internal receiver route = %#v, %v", route, ok)
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete internal delivery: %v", err)
	}
}

func hostilePublisherContext(t testing.TB) context.Context {
	t.Helper()
	token := testAgentLifecycleToken(t, "publisher-agent", "", 11, 3)
	authority := runtimeeffects.NormalAgentAuthority(token, "publisher-turn", time.Now().Add(time.Minute))
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"publisher-runtime",
		1,
		"",
		"publisher-census",
		authorActivityTestBundleHash,
		nil,
	)
	if err != nil {
		t.Fatalf("create publisher admission: %v", err)
	}
	ctx := context.WithValue(context.Background(), publisherOnlyContextKey{}, "publisher-secret")
	ctx = runtimeeffects.WithAuthority(ctx, authority)
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(receiverProjectionEffectStore{}))
	ctx = managedexecution.WithAdmission(ctx, admission)
	ctx = runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{
		Owner: "publisher-lineage", RunID: uuid.NewString(), Classification: runtimecorrelation.RuntimeLineageClassificationForkLocal,
	})
	return runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeMock)
}

func receiverProjectionEvent(suffix string) events.Event {
	return receiverProjectionEventForType(suffix, events.EventType("custom.receiver_projection"))
}

func receiverProjectionEventForType(suffix string, eventType events.EventType) events.Event {
	evt := eventtest.RuntimeControl(
		uuid.NewString(), eventType, "receiver-projection-test", "", []byte(`{}`), 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	return eventtest.ForDelivery(evt, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-" + suffix}})
}

func validateClosedReceiverContext(ctx context.Context, evt events.Event) error {
	if got := ctx.Value(publisherOnlyContextKey{}); got != nil {
		return fmt.Errorf("receiver inherited arbitrary publisher value %#v", got)
	}
	if _, ok := runtimeeffects.AuthorityFromContext(ctx); ok {
		return fmt.Errorf("normal receiver inherited publisher authority")
	}
	if _, ok := managedexecution.FromContext(ctx); ok {
		return fmt.Errorf("normal receiver inherited publisher admission")
	}
	if _, ok := runtimeeffects.ControllerFromContext(ctx); ok {
		return fmt.Errorf("normal receiver inherited publisher controller")
	}
	if lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx); ok && lineage.Owner == "publisher-lineage" {
		return fmt.Errorf("normal receiver inherited publisher runtime lineage")
	}
	if mode, ok := runtimeeffects.ExecutionModeFromContext(ctx); !ok || mode != evt.ExecutionMode() {
		return fmt.Errorf("receiver mode = %q, %v; want event mode %q", mode, ok, evt.ExecutionMode())
	}
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok || inbound.ID() != evt.ID() {
		return fmt.Errorf("receiver inbound event = %q, %v; want %q", inbound.ID(), ok, evt.ID())
	}
	if runID := runtimecorrelation.RunIDFromContext(ctx); runID != evt.RunID() {
		return fmt.Errorf("receiver run id = %q; want %q", runID, evt.RunID())
	}
	if delivery := events.DeliveryContextFromContext(ctx); delivery.ReplyContextID() != evt.DeliveryContext().ReplyContextID() {
		return fmt.Errorf("receiver reply context = %q; want %q", delivery.ReplyContextID(), evt.DeliveryContext().ReplyContextID())
	}
	if _, ok := worklifetime.OccurrenceFromContext(ctx); !ok {
		return fmt.Errorf("receiver occurrence is missing")
	}
	if fact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); !ok || !fact.Matches(authorActivityTestBundleSourceFact) {
		return fmt.Errorf("receiver bundle source fact is missing or wrong")
	}
	return nil
}

func runtimedeliveryRouteFromContext(ctx context.Context) (events.DeliveryRoute, bool) {
	return runtimedelivery.RouteFromContext(ctx)
}

func sameReceiverRoute(left, right events.DeliveryRoute) bool {
	leftID, leftErr := left.Identity()
	rightID, rightErr := right.Identity()
	return leftErr == nil && rightErr == nil && leftID == rightID
}
