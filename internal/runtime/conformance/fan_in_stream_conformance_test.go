package conformance

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
)

func TestFanInStreamConformance_RoutesToSingletonAndKernelEnforcesWindowedDedup(t *testing.T) {
	ctx := context.Background()
	source := templatefanin.LoadSource(t, templatefanin.Options{})
	report := runtimebootverify.Run(ctx, source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("fan-in stream hard invalidities = %#v, want none", got)
	}

	store := &fanInStreamMemoryStore{}
	eb, err := bus.NewEventBusWithOptions(store, bus.EventBusOptions{
		ContractBundle: source,
		TemplateInstanceActivator: func(context.Context, runtimepipeline.FlowInstanceActivationRequest) error {
			t.Fatal("fan-in stream routes to an explicit singleton; template activation is not authoritative")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
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
	handler, ok := source.NodeEventHandler(templatefanin.ReceiverNodeID, templatefanin.ReceiverEvent)
	if !ok {
		t.Fatalf("receiver handler %s/%s missing", templatefanin.ReceiverNodeID, templatefanin.ReceiverEvent)
	}

	state := runtimeengine.StateSnapshot{
		EntityID:     "portfolio-entity",
		CurrentState: "active",
		StateCarrier: runtimeengine.NewStateCarrier(map[string]any{}, nil, nil),
	}
	target := events.RouteIdentity{
		FlowID:       templatefanin.ReceiverFlowID,
		FlowInstance: templatefanin.ReceiverFlowInstance,
	}.Normalized()
	first := fanInStreamEvent(source.ResolveFlowEventReference(templatefanin.ProducerFlowID, templatefanin.ProducerEvent), "evt-fanin-a", "operating/a", "report-1", "2026-Q1", 100)
	state = fanInStreamPublishAndExecute(t, ctx, eb, store, exec, handler, state, first, target)
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q1"); got != 1 {
		t.Fatalf("Q1 accumulator items after first = %d, want 1", got)
	}

	duplicate := fanInStreamEvent(source.ResolveFlowEventReference(templatefanin.ProducerFlowID, templatefanin.ProducerEvent), "evt-fanin-b", "operating/b", "report-1", "2026-Q1", 200)
	state = fanInStreamPublishAndExecute(t, ctx, eb, store, exec, handler, state, duplicate, target)
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q1"); got != 1 {
		t.Fatalf("Q1 accumulator items after duplicate = %d, want 1", got)
	}

	nextWindow := fanInStreamEvent(source.ResolveFlowEventReference(templatefanin.ProducerFlowID, templatefanin.ProducerEvent), "evt-fanin-c", "operating/c", "report-1", "2026-Q2", 300)
	state = fanInStreamPublishAndExecute(t, ctx, eb, store, exec, handler, state, nextWindow, target)
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q1"); got != 1 {
		t.Fatalf("Q1 accumulator items after Q2 = %d, want 1", got)
	}
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q2"); got != 1 {
		t.Fatalf("Q2 accumulator items = %d, want 1", got)
	}
}

type fanInStreamMemoryStore struct {
	bus.InMemoryEventStore
	events         map[string]events.Event
	deliveryRoutes map[string][]events.DeliveryRoute
	scopes         map[string]runtimereplayclaim.CommittedReplayScope
}

func (s *fanInStreamMemoryStore) SupportsPersistedReplay() bool { return true }

func (s *fanInStreamMemoryStore) PersistEventWithDeliveryRouteSetAndScope(_ context.Context, evt events.Event, routes []events.DeliveryRoute, scope runtimereplayclaim.CommittedReplayScope) error {
	if s.events == nil {
		s.events = map[string]events.Event{}
	}
	if s.deliveryRoutes == nil {
		s.deliveryRoutes = map[string][]events.DeliveryRoute{}
	}
	if s.scopes == nil {
		s.scopes = map[string]runtimereplayclaim.CommittedReplayScope{}
	}
	s.events[evt.ID()] = evt
	s.deliveryRoutes[evt.ID()] = events.NormalizeDeliveryRoutes(routes)
	s.scopes[evt.ID()] = scope
	return nil
}

func (s *fanInStreamMemoryStore) InsertEventDeliveryRoutes(_ context.Context, eventID string, routes []events.DeliveryRoute) error {
	if s.deliveryRoutes == nil {
		s.deliveryRoutes = map[string][]events.DeliveryRoute{}
	}
	s.deliveryRoutes[eventID] = events.NormalizeDeliveryRoutes(routes)
	return nil
}

func (s *fanInStreamMemoryStore) ListEventDeliveryRoutes(_ context.Context, eventID string) ([]events.DeliveryRoute, error) {
	return append([]events.DeliveryRoute(nil), s.deliveryRoutes[eventID]...), nil
}

func (s *fanInStreamMemoryStore) UpsertCommittedReplayScope(_ context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	if s.scopes == nil {
		s.scopes = map[string]runtimereplayclaim.CommittedReplayScope{}
	}
	s.scopes[eventID] = scope
	return nil
}

func (s *fanInStreamMemoryStore) LoadCommittedReplayScope(_ context.Context, eventID string) (runtimereplayclaim.CommittedReplayScope, error) {
	scope := s.scopes[eventID]
	if scope == "" {
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	}
	return scope, nil
}

func (s *fanInStreamMemoryStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func fanInStreamPublishAndExecute(
	t *testing.T,
	ctx context.Context,
	eb *bus.EventBus,
	store *fanInStreamMemoryStore,
	exec *runtimeengine.Executor,
	handler runtimecontracts.SystemNodeEventHandler,
	state runtimeengine.StateSnapshot,
	evt events.Event,
	target events.RouteIdentity,
) runtimeengine.StateSnapshot {
	t.Helper()
	preflight, err := eb.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan(%s): %v", evt.ID(), err)
	}
	if preflight.TargetFailure != "" || !fanInStreamRoutesContain(preflight.DeliveryRoutes, target) {
		t.Fatalf("preflight for %s = failure:%q routes:%#v, want singleton target %#v", evt.ID(), preflight.TargetFailure, preflight.DeliveryRoutes, target)
	}
	if err := eb.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish(%s): %v", evt.ID(), err)
	}
	if !fanInStreamRoutesContain(store.deliveryRoutes[evt.ID()], target) {
		t.Fatalf("persisted routes for %s = %#v, want singleton target %#v", evt.ID(), store.deliveryRoutes[evt.ID()], target)
	}
	if got := store.scopes[evt.ID()]; got != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope for %s = %q, want subscribed", evt.ID(), got)
	}
	if err := eb.PublishPersistedRecipients(ctx, evt, nil); err != nil {
		t.Fatalf("PublishPersistedRecipients(%s): %v", evt.ID(), err)
	}
	result, err := exec.Execute(ctx, runtimeengine.ExecutionRequest{
		EntityID:        state.EntityID,
		NodeID:          runtimeidentity.NodeID(templatefanin.ReceiverNodeID),
		FlowID:          runtimeidentity.FlowID(templatefanin.ReceiverFlowID),
		Event:           evt,
		HandlerEventKey: templatefanin.ReceiverEvent,
		Handler:         handler,
		ProducerRoute:   target,
		State:           state,
		MaxDepth:        10,
		ChainDepth:      0,
		ExecutionID:     evt.ID(),
	})
	if err != nil {
		t.Fatalf("Execute(%s): %v", evt.ID(), err)
	}
	return runtimeengine.StateSnapshot{
		EntityID:     state.EntityID,
		CurrentState: state.CurrentState,
		StateCarrier: result.StateMutation.StateCarrier,
	}
}

func fanInStreamEvent(eventType, id, flowInstance, reportID, periodID string, revenue int) events.Event {
	payload, _ := json.Marshal(map[string]any{
		"portfolio_id": "portfolio-default",
		"report_id":    reportID,
		"period_id":    periodID,
		"operating_id": flowInstance,
		"revenue":      revenue,
	})
	return eventtest.RootIngress(
		id,
		events.EventType(eventType),
		"",
		"",
		payload,
		0,
		"run-fanin-stream",
		"",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID:       templatefanin.ProducerFlowID,
			FlowInstance: flowInstance,
			EntityID:     flowInstance + "-entity",
		}),
		time.Now().UTC(),
	)
}

func fanInStreamRoutesContain(routes []events.DeliveryRoute, target events.RouteIdentity) bool {
	target = target.Normalized()
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.SubscriberType == "node" && route.SubscriberID == templatefanin.ReceiverNodeID &&
			route.Target.FlowID == target.FlowID && route.Target.FlowInstance == target.FlowInstance {
			return true
		}
	}
	return false
}

func fanInStreamAccumulatorItemCount(t *testing.T, buckets map[string]map[string]any, window string) int {
	t.Helper()
	key := timeridentity.NewAccumulatorWindowBucketRef(templatefanin.ReceiverNodeID, templatefanin.ReceiverEvent, window).Key()
	nodeBucket, ok := buckets[templatefanin.ReceiverNodeID]
	if !ok {
		t.Fatalf("receiver node bucket missing from %#v", buckets)
	}
	accumulators, ok := nodeBucket["handler_accumulators"].(map[string]any)
	if !ok {
		t.Fatalf("handler accumulator map missing from %#v", nodeBucket)
	}
	raw, ok := accumulators[key].(map[string]any)
	if !ok {
		t.Fatalf("window accumulator %q missing from %#v", key, accumulators)
	}
	switch items := raw["items"].(type) {
	case []map[string]any:
		return len(items)
	case []any:
		return len(items)
	default:
		t.Fatalf("accumulator items have unexpected shape: %#v", raw["items"])
		return 0
	}
}
