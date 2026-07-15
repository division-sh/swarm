package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
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
type PayloadValidator func(ctx context.Context, eventType string, payload []byte) error

type EventBus struct {
	mu                          sync.RWMutex
	channels                    map[events.EventType]map[string]chan events.Event
	agentChans                  map[string]chan events.Event
	agentRouteHandles           map[string]*agentRouteHandle
	subscriptions               map[string][]events.EventType
	subscriptionKinds           map[string]inMemorySubscriberKind
	pendingInternalByID         map[string][]events.DeliveryRoute
	pendingOutboxByID           map[string][]pendingOutboxOperation
	pendingOutboxSequence       uint64
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
	inFlightEventIDs            map[string]int
}

type transactionRouteOverlayKey struct{}

type transactionRouteOverlay struct {
	table *RouteTable
}

func (eb *EventBus) withTransactionRouteOverlay(ctx context.Context) (context.Context, error) {
	if _, active := runtimepipeline.PipelineSQLTxFromContext(ctx); !active {
		return ctx, nil
	}
	if overlay, _ := ctx.Value(transactionRouteOverlayKey{}).(*transactionRouteOverlay); overlay != nil && overlay.table != nil {
		return ctx, nil
	}
	table, err := DeriveRouteTable(eb.semanticSource)
	if err != nil {
		return nil, fmt.Errorf("derive transaction-local route table: %w", err)
	}
	return context.WithValue(ctx, transactionRouteOverlayKey{}, &transactionRouteOverlay{table: table}), nil
}

func transactionRouteTableFromContext(ctx context.Context) *RouteTable {
	if ctx == nil {
		return nil
	}
	overlay, _ := ctx.Value(transactionRouteOverlayKey{}).(*transactionRouteOverlay)
	if overlay == nil {
		return nil
	}
	return overlay.table
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
		agentRouteHandles:           make(map[string]*agentRouteHandle),
		subscriptions:               make(map[string][]events.EventType),
		subscriptionKinds:           make(map[string]inMemorySubscriberKind),
		runtimeAgentDescriptors:     make(map[string]ActiveAgentDescriptor),
		pendingInternalByID:         make(map[string][]events.DeliveryRoute),
		pendingOutboxByID:           make(map[string][]pendingOutboxOperation),
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
		inFlightEventIDs:            make(map[string]int),
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

// RestorePersistedFlowInstanceRoute rebuilds the in-memory route table from
// already-persisted route truth without rewriting that truth during startup.
func (eb *EventBus) RestorePersistedFlowInstanceRoute(req FlowInstanceRouteMaterializationRequest) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	return table.AddFlowInstanceRoute(req.Normalized())
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
	if _, txActive := runtimepipeline.PipelineSQLTxFromContext(ctx); txActive && !hadRoute {
		staged := transactionRouteTableFromContext(ctx)
		if staged == nil {
			var err error
			staged, err = DeriveRouteTable(table.source)
			if err != nil {
				return fmt.Errorf("derive transaction-local flow-instance route table: %w", err)
			}
		}
		hadStagedRoute := staged.HasFlowInstanceRoute(req.Identity)
		if err := staged.AddFlowInstanceRoute(req); err != nil {
			return err
		}
		routes := staged.MaterializedRoutes(req.Identity)
		persister, ok := eb.store.(FlowInstanceRoutePersistence)
		if !ok {
			return errors.New("transactional flow-instance route persistence is required")
		}
		for _, route := range routes {
			if err := persister.UpsertFlowInstanceRoute(ctx, route); err != nil {
				return err
			}
		}
		if !hadStagedRoute {
			postCommitCtx := runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(ctx))
			if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func() {
				if err := table.AddFlowInstanceRoute(req); err != nil {
					_ = eb.LogRuntime(postCommitCtx, runtimepipeline.RuntimeLogEntry{
						Level: "error", Message: "Post-commit flow-instance route publication failed",
						Component: "eventbus", Action: "flow_instance_route_post_commit_publish_failed",
						Detail: map[string]any{"instance_path": req.Identity.InstancePath, "error": err.Error()},
					})
				}
			}) {
				return errors.New("transactional flow-instance route requires post-commit publication owner")
			}
		}
		return nil
	}
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
	return eb.RemoveFlowInstanceRouteContext(context.Background(), identity)
}

func (eb *EventBus) RemoveFlowInstanceRouteContext(ctx context.Context, identity runtimeflowidentity.Route) error {
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
		if err := persister.DeleteFlowInstanceRoute(ctx, owner); err != nil {
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
	for _, route := range eb.agentRouteHandles {
		route.deactivate()
	}
	eb.channels = make(map[events.EventType]map[string]chan events.Event)
	eb.agentChans = make(map[string]chan events.Event)
	eb.agentRouteHandles = make(map[string]*agentRouteHandle)
	eb.subscriptions = make(map[string][]events.EventType)
	eb.subscriptionKinds = make(map[string]inMemorySubscriberKind)
	eb.pendingInternalByID = make(map[string][]events.DeliveryRoute)
	eb.pendingOutboxByID = make(map[string][]pendingOutboxOperation)
	eb.inFlightEventIDs = make(map[string]int)
	routeTable, err := eb.deriveBootRouteTableLocked()
	if err != nil {
		return err
	}
	eb.routeTable = routeTable
	eb.rebuildRoutePlanners()
	eb.inFlightPublishes.Store(0)
	return nil
}

func (eb *EventBus) beginEventPublish(eventID string) {
	if eb == nil {
		return
	}
	eb.inFlightPublishes.Add(1)
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return
	}
	eb.mu.Lock()
	if eb.inFlightEventIDs == nil {
		eb.inFlightEventIDs = make(map[string]int)
	}
	eb.inFlightEventIDs[eventID]++
	eb.mu.Unlock()
}

func (eb *EventBus) endEventPublish(eventID string) {
	if eb == nil {
		return
	}
	eventID = strings.TrimSpace(eventID)
	if eventID != "" {
		eb.mu.Lock()
		if count := eb.inFlightEventIDs[eventID]; count <= 1 {
			delete(eb.inFlightEventIDs, eventID)
		} else {
			eb.inFlightEventIDs[eventID] = count - 1
		}
		eb.mu.Unlock()
	}
	eb.inFlightPublishes.Add(-1)
}

func (eb *EventBus) eventPublishInFlight(eventID string) bool {
	if eb == nil {
		return false
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.inFlightEventIDs[eventID] > 0
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

// ReplaceAgentRoute installs one exact lifecycle-generation route. The old
// channel is detached, not closed: publishers may retain a lock-free snapshot
// of it and must be allowed to finish without a send-on-closed panic.
func (eb *EventBus) ReplaceAgentRoute(token runtimeeffects.LifecycleToken, eventTypes ...events.EventType) <-chan events.Event {
	if eb == nil || !token.Valid() {
		return nil
	}
	agentID := strings.TrimSpace(token.AgentID)
	ch := make(chan events.Event, 128)
	route := newAgentRouteHandle(token, ch)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.detachSubscriberLocked(agentID)
	eb.agentChans[agentID] = ch
	eb.agentRouteHandles[agentID] = route
	eb.subscriptionKinds[agentID] = inMemorySubscriberAgent
	for _, eventType := range eventTypes {
		eventType = events.EventType(strings.TrimSpace(string(eventType)))
		if eventType == "" {
			continue
		}
		eb.subscriptions[agentID] = AppendUniqueEventType(eb.subscriptions[agentID], eventType)
		if eb.channels[eventType] == nil {
			eb.channels[eventType] = make(map[string]chan events.Event)
		}
		eb.channels[eventType][agentID] = ch
	}
	return ch
}

// RemoveAgentRoute removes only the exact generation that owns the route.
// Delayed predecessor cleanup is therefore harmless after replacement.
func (eb *EventBus) RemoveAgentRoute(token runtimeeffects.LifecycleToken) {
	if eb == nil || !token.Valid() {
		return
	}
	agentID := strings.TrimSpace(token.AgentID)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if current := eb.agentRouteHandles[agentID]; current == nil || current.token != token {
		return
	}
	eb.detachSubscriberLocked(agentID)
}

func (eb *EventBus) SubscribeInternal(subscriberID string, eventTypes ...events.EventType) <-chan events.Event {
	return eb.subscribe(subscriberID, inMemorySubscriberInternal, eventTypes...)
}

func (eb *EventBus) subscribe(subscriberID string, kind inMemorySubscriberKind, eventTypes ...events.EventType) <-chan events.Event {
	ch := make(chan events.Event, 128)
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if existing, ok := eb.agentChans[subscriberID]; ok {
		if _, exactRoute := eb.agentRouteHandles[subscriberID]; exactRoute {
			return existing
		}
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
	if _, exactRoute := eb.agentRouteHandles[agentID]; exactRoute {
		return
	}

	eb.detachSubscriberLocked(agentID)
}

func (eb *EventBus) detachSubscriberLocked(agentID string) {
	if route := eb.agentRouteHandles[agentID]; route != nil {
		route.deactivate()
	}
	delete(eb.agentChans, agentID)
	delete(eb.agentRouteHandles, agentID)
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
