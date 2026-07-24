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
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestFanInStreamConformance_RoutesToSingletonAndKernelEnforcesWindowedDedup(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.FanInStream)
	ctx := testAuthorActivityContext(context.Background())
	source := templatefanin.LoadSource(t, templatefanin.Options{})
	report := runtimebootverify.Run(ctx, source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("fan-in stream hard invalidities = %#v, want none", got)
	}
	proveFanInStreamProducerPath(t, source)

	store := &fanInStreamMemoryStore{}
	eb, err := newScopedTestEventBus(t, store, bus.EventBusOptions{
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
		CurrentState: "active",
		StateCarrier: runtimeengine.NewStateCarrier(map[string]any{}, nil, nil),
	}
	target := events.RouteIdentity{
		FlowID:       templatefanin.ReceiverFlowID,
		FlowInstance: templatefanin.ReceiverFlowInstance,
		EntityID:     runtimeflowidentity.EntityID(templatefanin.ReceiverFlowInstance),
	}.Normalized()
	first := fanInStreamEvent(source.ResolveFlowEventReference(templatefanin.ProducerFlowID, templatefanin.ProducerEvent), "evt-fanin-a", "operating/a", "report-1", "2026-Q1", 100)
	state = fanInStreamPublishAndExecute(t, ctx, eb, store, exec, handler, state, first, target)
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q1"); got != 1 {
		t.Fatalf("Q1 accumulator items after first = %d, want 1", got)
	}
	if got := state.StateCarrier.Metadata["last_revenue"]; got != float64(100) {
		t.Fatalf("last revenue after first = %#v, want 100", got)
	}

	duplicate := fanInStreamEvent(source.ResolveFlowEventReference(templatefanin.ProducerFlowID, templatefanin.ProducerEvent), "evt-fanin-b", "operating/a", "report-2", "2026-Q1", 200)
	state = fanInStreamPublishAndExecute(t, ctx, eb, store, exec, handler, state, duplicate, target)
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q1"); got != 1 {
		t.Fatalf("Q1 accumulator items after duplicate = %d, want 1", got)
	}
	if got := state.StateCarrier.Metadata["last_revenue"]; got != float64(100) {
		t.Fatalf("last revenue after duplicate = %#v, want unchanged first arrival value", got)
	}

	nextWindow := fanInStreamEvent(source.ResolveFlowEventReference(templatefanin.ProducerFlowID, templatefanin.ProducerEvent), "evt-fanin-c", "operating/a", "report-3", "2026-Q2", 300)
	state = fanInStreamPublishAndExecute(t, ctx, eb, store, exec, handler, state, nextWindow, target)
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q1"); got != 1 {
		t.Fatalf("Q1 accumulator items after Q2 = %d, want 1", got)
	}
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q2"); got != 1 {
		t.Fatalf("Q2 accumulator items = %d, want 1", got)
	}
	if got := state.StateCarrier.Metadata["last_revenue"]; got != float64(300) {
		t.Fatalf("last revenue after next window = %#v, want 300", got)
	}
}

func TestCreateEventIDCarryProjectionReachesHandler(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.FanInStream)
	proveFanInStreamProducerPath(t, templatefanin.LoadSource(t, templatefanin.Options{}))
}

func proveFanInStreamProducerPath(t *testing.T, source semanticview.Source) {
	t.Helper()
	backend := storetest.StartSQLiteRuntimeStore(t)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	seedFanInBarrierRun(t, ctx, backend, backend.DB, runID)
	runtime := newFanInBarrierRuntime(t, backend, backend.DB, source)
	if err := runtime.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      templatefanin.ReceiverFlowInstance,
		StorageRef:      templatefanin.ReceiverFlowInstance,
		WorkflowName:    templatefanin.ReceiverFlowID,
		WorkflowVersion: "1.0.0",
		CurrentState:    "active",
		Metadata: map[string]any{
			"entity_id":     runtimeflowidentity.EntityID(templatefanin.ReceiverFlowInstance),
			"portfolio_id":  "portfolio-default",
			"flow_path":     templatefanin.ReceiverFlowInstance,
			"instance_id":   templatefanin.ReceiverFlowInstance,
			"instance_kind": "singleton",
		},
	}); err != nil {
		t.Fatalf("seed fan-in stream singleton: %v", err)
	}

	requestEventID := uuid.NewString()
	publishFanInBarrierEvent(t, ctx, runtime.bus, source, requestEventID, "ingress", "operating.report.requested", map[string]any{
		"period_id": "2026-Q1",
		"revenue":   100,
	})

	var requestPayloadRaw, reportPayloadRaw string
	if err := backend.DB.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_id = ?`, requestEventID).Scan(&requestPayloadRaw); err != nil {
		t.Fatalf("load producer request payload: %v", err)
	}
	if err := backend.DB.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_name LIKE 'operating/%/operating.reported'`).Scan(&reportPayloadRaw); err != nil {
		t.Fatalf("load producer-driven report payload: %v", err)
	}
	var requestPayload, reportPayload map[string]any
	if err := json.Unmarshal([]byte(requestPayloadRaw), &requestPayload); err != nil {
		t.Fatalf("decode producer request payload: %v", err)
	}
	if err := json.Unmarshal([]byte(reportPayloadRaw), &reportPayload); err != nil {
		t.Fatalf("decode producer-driven report payload: %v", err)
	}
	if _, exists := requestPayload["operating_id"]; exists {
		t.Fatalf("producer request payload was mutated with receiver carry: %#v", requestPayload)
	}
	if reportPayload["operating_id"] != requestEventID || reportPayload["period_id"] != "2026-Q1" || reportPayload["revenue"] != float64(100) {
		t.Fatalf("producer-driven report payload = %#v, want minted carry %s", reportPayload, requestEventID)
	}
	routes, err := backend.ListEventDeliveryRoutes(ctx, requestEventID)
	if err != nil {
		t.Fatalf("load producer request delivery routes: %v", err)
	}
	if len(routes) != 1 || routes[0].PayloadProjection.Fields()["operating_id"] != requestEventID {
		t.Fatalf("producer request delivery routes = %#v, want stamped operating_id %s", routes, requestEventID)
	}
	portfolio := loadFanInBarrierPortfolio(t, ctx, runtime.workflowStore)
	carrier, err := runtimeengine.StateCarrierFromPersisted(portfolio.Metadata, portfolio.StateBuckets)
	if err != nil {
		t.Fatalf("load producer-driven stream state: %v", err)
	}
	if got := fanInStreamAccumulatorItemCount(t, carrier.StateBuckets, "2026-Q1"); got != 1 {
		t.Fatalf("producer-driven stream accumulator items = %d, want 1", got)
	}
	if got := carrier.Metadata["last_revenue"]; got != float64(100) {
		t.Fatalf("producer-driven stream last revenue = %#v, want 100", got)
	}
}

func TestFanInStreamConformance_EventIDDedupUsesEventIdentity(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	source := templatefanin.LoadSource(t, templatefanin.Options{EventIDDedup: true})
	report := runtimebootverify.Run(ctx, source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("fan-in stream event.id hard invalidities = %#v, want none", got)
	}

	store := &fanInStreamMemoryStore{}
	eb, err := newScopedTestEventBus(t, store, bus.EventBusOptions{
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
		CurrentState: "active",
		StateCarrier: runtimeengine.NewStateCarrier(map[string]any{}, nil, nil),
	}
	target := events.RouteIdentity{
		FlowID:       templatefanin.ReceiverFlowID,
		FlowInstance: templatefanin.ReceiverFlowInstance,
		EntityID:     runtimeflowidentity.EntityID(templatefanin.ReceiverFlowInstance),
	}.Normalized()

	first := fanInStreamEvent(source.ResolveFlowEventReference(templatefanin.ProducerFlowID, templatefanin.ProducerEvent), "evt-fanin-event-id", "operating/a", "report-1", "2026-Q1", 100)
	state = fanInStreamPublishAndExecute(t, ctx, eb, store, exec, handler, state, first, target)
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q1"); got != 1 {
		t.Fatalf("Q1 accumulator items after first = %d, want 1", got)
	}

	sameEventDifferentPayloadKey := fanInStreamEvent(source.ResolveFlowEventReference(templatefanin.ProducerFlowID, templatefanin.ProducerEvent), "evt-fanin-event-id", "operating/b", "report-2", "2026-Q1", 200)
	state = fanInStreamPublishAndExecute(t, ctx, eb, store, exec, handler, state, sameEventDifferentPayloadKey, target)
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q1"); got != 1 {
		t.Fatalf("Q1 accumulator items after same event id = %d, want 1", got)
	}

	nextEvent := fanInStreamEvent(source.ResolveFlowEventReference(templatefanin.ProducerFlowID, templatefanin.ProducerEvent), "evt-fanin-event-id-2", "operating/c", "report-2", "2026-Q1", 300)
	state = fanInStreamPublishAndExecute(t, ctx, eb, store, exec, handler, state, nextEvent, target)
	if got := fanInStreamAccumulatorItemCount(t, state.StateCarrier.StateBuckets, "2026-Q1"); got != 2 {
		t.Fatalf("Q1 accumulator items after distinct event id = %d, want 2", got)
	}
}

type fanInStreamMemoryStore struct {
	bus.InMemoryEventStore
	events         map[string]events.Event
	deliveryRoutes map[string][]events.DeliveryRoute
	scopes         map[string]runtimepipelineobligation.CommittedScope
}

func (s *fanInStreamMemoryStore) CommitPublish(ctx context.Context, plan bus.CommitPublishPlan) (bus.PreparedPublish, error) {
	return runtimebustest.CommitPublish(ctx, plan, nil, func(_ context.Context, req bus.CommitPublishRequest) error {
		event := req.Event.Event()
		if s.events == nil {
			s.events = map[string]events.Event{}
		}
		if s.deliveryRoutes == nil {
			s.deliveryRoutes = map[string][]events.DeliveryRoute{}
		}
		if s.scopes == nil {
			s.scopes = map[string]runtimepipelineobligation.CommittedScope{}
		}
		s.events[event.ID()] = event
		s.deliveryRoutes[event.ID()] = events.NormalizeDeliveryRoutes(req.DeliveryRoutes)
		s.scopes[event.ID()] = req.ReplayScope
		return nil
	})
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
	deliveryTarget, ok := fanInStreamTargetRoute(store.deliveryRoutes[evt.ID()], target)
	if !ok {
		t.Fatalf("persisted routes for %s = %#v, want singleton target %#v", evt.ID(), store.deliveryRoutes[evt.ID()], target)
	}
	if deliveryTarget.EntityID == "" {
		t.Fatalf("persisted route for %s has empty entity_id: %#v", evt.ID(), deliveryTarget)
	}
	if got := store.scopes[evt.ID()]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope for %s = %q, want subscribed", evt.ID(), got)
	}
	if _, err := eb.RecoverPersistedPipeline(ctx, runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline(%s): %v", evt.ID(), err)
	}
	delivered := eventtest.TargetRouted(evt, deliveryTarget)
	if got := delivered.EntityID(); got != deliveryTarget.EntityID {
		t.Fatalf("delivered event %s entity_id = %q, want delivery target entity %q", evt.ID(), got, deliveryTarget.EntityID)
	}
	executionState := state
	executionState.EntityID = ""
	result, err := exec.Execute(ctx, runtimeengine.ExecutionRequest{
		NodeID:          runtimeidentity.NodeID(templatefanin.ReceiverNodeID),
		FlowID:          runtimeidentity.FlowID(templatefanin.ReceiverFlowID),
		Event:           delivered,
		HandlerEventKey: templatefanin.ReceiverEvent,
		Handler:         handler,
		ProducerRoute:   target,
		State:           executionState,
		MaxDepth:        10,
		ChainDepth:      0,
		ExecutionID:     evt.ID(),
	})
	if err != nil {
		t.Fatalf("Execute(%s): %v", evt.ID(), err)
	}
	metadata := state.StateCarrier.Metadata
	if result.StateMutation.StateCarrier.Metadata != nil {
		metadata = result.StateMutation.StateCarrier.Metadata
	}
	gates := state.StateCarrier.Gates
	if result.StateMutation.StateCarrier.Gates != nil {
		gates = result.StateMutation.StateCarrier.Gates
	}
	buckets := state.StateCarrier.StateBuckets
	if result.StateMutation.StateCarrier.StateBuckets != nil {
		buckets = result.StateMutation.StateCarrier.StateBuckets
	}
	return runtimeengine.StateSnapshot{
		EntityID:     runtimeidentity.EntityID(deliveryTarget.EntityID),
		CurrentState: state.CurrentState,
		StateCarrier: runtimeengine.NewStateCarrier(metadata, gates, buckets),
	}
}

func fanInStreamEvent(eventType, id, flowInstance, reportID, periodID string, revenue int) events.Event {
	if prefix := templatefanin.ProducerFlowID + "/"; strings.HasPrefix(eventType, prefix) {
		eventType = strings.Trim(strings.TrimSpace(flowInstance), "/") + "/" + strings.TrimPrefix(eventType, prefix)
	}
	payload, _ := json.Marshal(map[string]any{
		"portfolio_id": "portfolio-default",
		"report_id":    reportID,
		"period_id":    periodID,
		"operating_id": flowInstance,
		"revenue":      revenue,
	})
	return eventtest.RunCreatingRootIngress(
		eventtest.UUID(id),
		events.EventType(eventType),
		"",
		"",
		payload,
		0,
		eventtest.UUID("run-fanin-stream"),
		"",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID:       templatefanin.ProducerFlowID,
			FlowInstance: flowInstance,
			EntityID:     runtimeflowidentity.EntityID(flowInstance + "-entity"),
		}),
		time.Now().UTC(),
	)
}

func fanInStreamRoutesContain(routes []events.DeliveryRoute, target events.RouteIdentity) bool {
	_, ok := fanInStreamTargetRoute(routes, target)
	return ok
}

func fanInStreamTargetRoute(routes []events.DeliveryRoute, target events.RouteIdentity) (events.RouteIdentity, bool) {
	target = target.Normalized()
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.SubscriberType == "node" && route.SubscriberID == templatefanin.ReceiverNodeID &&
			route.Target.FlowID == target.FlowID && route.Target.FlowInstance == target.FlowInstance &&
			route.Target.EntityID == target.EntityID {
			return route.Target, true
		}
	}
	return events.RouteIdentity{}, false
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
