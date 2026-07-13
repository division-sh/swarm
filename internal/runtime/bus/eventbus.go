package bus

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// EventInterceptor runs deterministic coordination in the publish path.
// It may consume the inbound event and/or emit deferred events.
type EventInterceptor interface {
	Intercept(ctx context.Context, evt events.Event) (passthrough bool, deferred []events.Event, err error)
}

// DeliveryRouteInterceptor runs deterministic coordination for one
// authoritative delivery route. EventBus uses this for workflow-node delivery
// routes so Pipeline receives "execute this node for this route" semantics
// instead of inferring route authority from an event-wide context.
type DeliveryRouteInterceptor interface {
	InterceptDeliveryRoute(ctx context.Context, evt events.Event, route events.DeliveryRoute) (passthrough bool, deferred []events.Event, err error)
}

// PayloadValidator validates canonical event-store admission before an event is
// persisted or direct-recipient eligibility is reported. It does not own
// producer-surface shaping or routing/delivery/source-target semantics.
type PayloadValidator func(eventType string, payload []byte) error

type EventBus struct {
	mu                          sync.RWMutex
	channels                    map[events.EventType]map[string]chan events.Event
	agentChans                  map[string]chan events.Event
	subscriptions               map[string][]events.EventType
	subscriptionKinds           map[string]inMemorySubscriberKind
	pendingInternalByID         map[string][]events.DeliveryRoute
	routeTable                  *RouteTable
	runtimeAgentDescriptors     map[string]ActiveAgentDescriptor
	connectRoutePlanner         connectRoutePlanResolver
	deliveryPlanner             deliveryPlanner
	interceptors                []EventInterceptor
	interceptorProvider         func() []EventInterceptor
	store                       EventStore
	logger                      LoggerHook
	semanticSource              semanticview.Source
	templateInstanceActivator   runtimepipeline.FlowInstanceActivator
	payloadValidator            PayloadValidator
	recipientPlanAdmissionGuard PublishRecipientPlanAdmissionGuard
	recipientPlanMaterializer   PublishRecipientPlanMaterializer
	recipientPlanGuard          PublishRecipientPlanGuard
	runtimeIngressDispatchGate  RuntimeIngressDispatchGate
	runDispatchGate             RunDispatchGate
	bundleFingerprint           string
	bundleSourceFact            runtimecorrelation.BundleSourceFact
	testLifecycleProbe          runtimelifecycleprobe.Observer
	providerOutputVerifier      ProviderOutputAuthorizationVerifier
	outboxSweeperActive         bool
	inFlightPublishes           atomic.Int64
}

type PublishRecipientPlan struct {
	Recipients             []string
	PersistedRecipients    []string
	RoutedRecipients       []PublishDiagnosticRecipient
	SubscriptionRecipients []string
	DeliveryRoutes         []events.DeliveryRoute
	TargetFailure          string
	canonicalAuthority     bool
}

type DirectRecipientStatus struct {
	Requested  []string
	Recipients []string
	Filtered   []string
	Missing    []string
}

type PublishRecipientPlanAdmissionGuard func(context.Context, events.Event) error
type PublishRecipientPlanMaterializer func(context.Context, events.Event, PublishRecipientPlan) ([]events.DeliveryRoute, error)
type PublishRecipientPlanGuard func(context.Context, events.Event, PublishRecipientPlan) error

type RuntimeIngressDispatchGate interface {
	QueueableIngressPaused(context.Context) (bool, error)
}

type RunDispatchGate interface {
	QueueableRunDispatchBlocked(context.Context, string) (bool, error)
}

type EventBusOptions struct {
	Logger                      LoggerHook
	Interceptors                []EventInterceptor
	InterceptorProvider         func() []EventInterceptor
	ContractBundle              semanticview.Source
	RouteTable                  *RouteTable
	TemplateInstanceActivator   runtimepipeline.FlowInstanceActivator
	PayloadValidator            PayloadValidator
	RecipientPlanAdmissionGuard PublishRecipientPlanAdmissionGuard
	RecipientPlanMaterializer   PublishRecipientPlanMaterializer
	RecipientPlanGuard          PublishRecipientPlanGuard
	RuntimeIngressDispatchGate  RuntimeIngressDispatchGate
	RunDispatchGate             RunDispatchGate
	BundleFingerprint           string
	BundleSourceFact            runtimecorrelation.BundleSourceFact
	TestLifecycleProbe          runtimelifecycleprobe.Observer
	ProviderOutputVerifier      ProviderOutputAuthorizationVerifier
}

const deliverySendTimeout = 250 * time.Millisecond

var ErrStaleRuntimeEpoch = errors.New("stale runtime epoch")

type inMemorySubscriberKind string

const (
	inMemorySubscriberAgent    inMemorySubscriberKind = "agent"
	inMemorySubscriberInternal inMemorySubscriberKind = "internal"
)

func NewEventBus(store EventStore) (*EventBus, error) {
	return NewEventBusWithOptions(store, EventBusOptions{})
}

func NewEventBusWithOptions(store EventStore, opts EventBusOptions) (*EventBus, error) {
	if store == nil {
		store = InMemoryEventStore{}
	}
	semanticSource := opts.ContractBundle
	filtered := make([]EventInterceptor, 0, len(opts.Interceptors))
	for _, it := range opts.Interceptors {
		if it != nil {
			filtered = append(filtered, it)
		}
	}
	routeTable := opts.RouteTable
	if routeTable != nil {
		if err := validateTypedPubSubAuthorizations(semanticSource); err != nil {
			return nil, err
		}
	}
	if routeTable == nil {
		derived, err := DeriveRouteTable(semanticSource)
		if err != nil {
			return nil, err
		}
		routeTable = derived
	}
	eb := &EventBus{
		channels:                    make(map[events.EventType]map[string]chan events.Event),
		agentChans:                  make(map[string]chan events.Event),
		subscriptions:               make(map[string][]events.EventType),
		subscriptionKinds:           make(map[string]inMemorySubscriberKind),
		runtimeAgentDescriptors:     make(map[string]ActiveAgentDescriptor),
		pendingInternalByID:         make(map[string][]events.DeliveryRoute),
		routeTable:                  routeTable,
		store:                       store,
		logger:                      opts.Logger,
		interceptors:                filtered,
		interceptorProvider:         opts.InterceptorProvider,
		semanticSource:              semanticSource,
		templateInstanceActivator:   opts.TemplateInstanceActivator,
		payloadValidator:            opts.PayloadValidator,
		recipientPlanAdmissionGuard: opts.RecipientPlanAdmissionGuard,
		recipientPlanMaterializer:   opts.RecipientPlanMaterializer,
		recipientPlanGuard:          opts.RecipientPlanGuard,
		runtimeIngressDispatchGate:  opts.RuntimeIngressDispatchGate,
		runDispatchGate:             opts.RunDispatchGate,
		bundleFingerprint:           strings.TrimSpace(opts.BundleFingerprint),
		bundleSourceFact:            opts.BundleSourceFact.Normalized(),
		testLifecycleProbe:          opts.TestLifecycleProbe,
		providerOutputVerifier:      opts.ProviderOutputVerifier,
	}
	eb.rebuildRoutePlanners()
	return eb, nil
}

func (eb *EventBus) SetProviderOutputAuthorizationVerifier(verifier ProviderOutputAuthorizationVerifier) {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	eb.providerOutputVerifier = verifier
	eb.mu.Unlock()
}

func (eb *EventBus) providerOutputAuthorizationVerifier() ProviderOutputAuthorizationVerifier {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.providerOutputVerifier
}

func (eb *EventBus) rebuildRoutePlanners() {
	if eb == nil {
		return
	}
	eb.connectRoutePlanner = newConnectRoutePlanResolver(eb.semanticSource, eb.routeTable, eb.PinRoutingDescriptors, eb.templateInstanceActivator, eb.store)
	eb.deliveryPlanner = eb.newEventBusDeliveryPlanner()
}

func (eb *EventBus) SetRunDispatchGate(gate RunDispatchGate) {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	eb.runDispatchGate = gate
	eb.mu.Unlock()
}

func (eb *EventBus) SetRuntimeIngressDispatchGate(gate RuntimeIngressDispatchGate) {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	eb.runtimeIngressDispatchGate = gate
	eb.mu.Unlock()
}

func (eb *EventBus) Store() EventStore {
	if eb == nil {
		return nil
	}
	return eb.store
}

func (eb *EventBus) MarkDeliveryInProgress(ctx context.Context, agentID, sessionID string) (bool, error) {
	if eb == nil || eb.store == nil {
		return false, nil
	}
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok || strings.TrimSpace(inbound.ID()) == "" || strings.TrimSpace(agentID) == "" {
		return false, nil
	}
	type deliveryProgressWriter interface {
		MarkEventDeliveryInProgress(ctx context.Context, eventID, agentID, sessionID string) error
	}
	writer, ok := eb.store.(deliveryProgressWriter)
	if !ok || writer == nil {
		return false, nil
	}
	if err := writer.MarkEventDeliveryInProgress(ctx, inbound.ID(), agentID, sessionID); err != nil {
		return false, err
	}
	return true, nil
}

func (eb *EventBus) RouteTable() *RouteTable {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.routeTable
}

func (eb *EventBus) HasFlowInstanceRoute(identity runtimeflowidentity.Route) bool {
	table := eb.RouteTable()
	return table != nil && table.HasFlowInstanceRoute(identity)
}

func (eb *EventBus) AddFlowInstanceRoute(req FlowInstanceRouteMaterializationRequest) error {
	return eb.AddFlowInstanceRouteContext(context.Background(), req)
}

func (eb *EventBus) AddFlowInstanceRouteContext(ctx context.Context, req FlowInstanceRouteMaterializationRequest) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	req = req.Normalized()
	hadRoute := table.HasFlowInstanceRoute(req.Identity)
	if err := table.AddFlowInstanceRoute(req); err != nil {
		return err
	}
	addedRoute := !hadRoute && table.HasFlowInstanceRoute(req.Identity)
	if addedRoute {
		if _, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok {
			if !runtimepipeline.QueuePipelineRollbackAction(ctx, func() {
				_ = table.RemoveFlowInstanceRoute(req.Identity)
			}) {
				_ = table.RemoveFlowInstanceRoute(req.Identity)
				return errors.New("flow-instance route rollback action is required in pipeline transaction")
			}
		}
	}
	persister, ok := eb.store.(FlowInstanceRoutePersistence)
	if !ok {
		return nil
	}
	routes := table.MaterializedRoutes(req.Identity)
	if len(routes) == 0 {
		return nil
	}
	for _, route := range routes {
		if err := persister.UpsertFlowInstanceRoute(ctx, route); err != nil {
			if addedRoute {
				if rollback, ok := eb.store.(FlowInstanceRouteRollbackPersistence); ok && rollback != nil {
					_ = rollback.RollbackFlowInstanceRoute(ctx, route.Identity)
				}
				_ = table.RemoveFlowInstanceRoute(route.Identity)
			}
			return err
		}
	}
	return nil
}

func (eb *EventBus) RemoveFlowInstanceRoute(identity runtimeflowidentity.Route) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	owner, exists, err := table.flowInstanceRouteRemovalOwner(identity)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if persister, ok := eb.store.(FlowInstanceRoutePersistence); ok {
		if err := persister.DeleteFlowInstanceRoute(context.Background(), owner); err != nil {
			return err
		}
	}
	return table.RemoveFlowInstanceRoute(owner)
}

func (eb *EventBus) SetLoggerHook(logger LoggerHook) {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	eb.logger = logger
	eb.mu.Unlock()
}

func (eb *EventBus) SetInterceptors(interceptors ...EventInterceptor) {
	if eb == nil {
		return
	}
	filtered := make([]EventInterceptor, 0, len(interceptors))
	for _, it := range interceptors {
		if it != nil {
			filtered = append(filtered, it)
		}
	}
	eb.mu.Lock()
	eb.interceptors = filtered
	eb.interceptorProvider = nil
	eb.mu.Unlock()
}

func (eb *EventBus) ResetInMemoryState() error {
	if eb == nil {
		return nil
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	for _, ch := range eb.agentChans {
		close(ch)
	}
	eb.channels = make(map[events.EventType]map[string]chan events.Event)
	eb.agentChans = make(map[string]chan events.Event)
	eb.subscriptions = make(map[string][]events.EventType)
	eb.subscriptionKinds = make(map[string]inMemorySubscriberKind)
	eb.pendingInternalByID = make(map[string][]events.DeliveryRoute)
	routeTable, err := eb.deriveBootRouteTableLocked()
	if err != nil {
		return err
	}
	eb.routeTable = routeTable
	eb.rebuildRoutePlanners()
	eb.inFlightPublishes.Store(0)
	return nil
}

func (eb *EventBus) WaitForQuiescence(ctx context.Context) error {
	if eb == nil {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if eb.inFlightPublishes.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (eb *EventBus) PendingAgentDeliveries() int {
	if eb == nil {
		return 0
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	pending := 0
	for _, ch := range eb.agentChans {
		pending += len(ch)
	}
	return pending
}

func (eb *EventBus) Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event {
	return eb.subscribe(agentID, inMemorySubscriberAgent, eventTypes...)
}

func (eb *EventBus) SubscribeInternal(subscriberID string, eventTypes ...events.EventType) <-chan events.Event {
	return eb.subscribe(subscriberID, inMemorySubscriberInternal, eventTypes...)
}

func (eb *EventBus) subscribe(subscriberID string, kind inMemorySubscriberKind, eventTypes ...events.EventType) <-chan events.Event {
	ch := make(chan events.Event, 128)
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if existing, ok := eb.agentChans[subscriberID]; ok {
		ch = existing
	} else {
		eb.agentChans[subscriberID] = ch
	}
	eb.subscriptionKinds[subscriberID] = kind

	for _, et := range eventTypes {
		eb.subscriptions[subscriberID] = AppendUniqueEventType(eb.subscriptions[subscriberID], et)
		if eb.channels[et] == nil {
			eb.channels[et] = make(map[string]chan events.Event)
		}
		eb.channels[et][subscriberID] = ch
	}
	return ch
}

func (eb *EventBus) Unsubscribe(agentID string) {
	agentID = strings.TrimSpace(agentID)
	if eb == nil || agentID == "" {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if ch, ok := eb.agentChans[agentID]; ok {
		delete(eb.agentChans, agentID)
		close(ch)
	}
	delete(eb.subscriptions, agentID)
	delete(eb.subscriptionKinds, agentID)
	for et := range eb.channels {
		delete(eb.channels[et], agentID)
		if len(eb.channels[et]) == 0 {
			delete(eb.channels, et)
		}
	}
}

func (eb *EventBus) deriveBootRouteTableLocked() (*RouteTable, error) {
	derived, err := DeriveRouteTable(eb.semanticSource)
	if err != nil {
		return nil, err
	}
	return derived, nil
}
