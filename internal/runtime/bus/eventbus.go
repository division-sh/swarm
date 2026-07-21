package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
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
	InterceptDeliveryRoute(ctx context.Context, evt events.DeliveryEvent, route events.DeliveryRoute) (passthrough bool, deferred []events.Event, err error)
}

// PayloadValidator validates canonical event-store admission before an event is
// persisted or direct-recipient eligibility is reported. It does not own
// producer-surface shaping or routing/delivery/source-target semantics.
type PayloadValidator func(ctx context.Context, eventType string, payload []byte) error

type EventBus struct {
	mu                          sync.RWMutex
	channels                    map[events.EventType]map[string]chan *LocalDelivery
	agentChans                  map[string]chan *LocalDelivery
	agentRouteHandles           map[string]*agentRouteHandle
	internalHandles             map[string]*internalSubscriptionHandle
	retiringAgentRoutes         []*agentRouteHandle
	retiringInternalHandles     []*internalSubscriptionHandle
	resetInProgress             bool
	resetDone                   chan struct{}
	internalChanged             chan struct{}
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
	runtimeInstanceID           string
	testLifecycleProbe          runtimelifecycleprobe.Observer
	providerOutputVerifier      ProviderOutputAuthorizationVerifier
	outboxSweeperActive         bool
	outboxSweeperDone           chan struct{}
	workOwner                   worklifetime.Occurrence
}

type LocalDelivery = worklifetime.EventDelivery

type transactionRouteOverlayKey struct{}

func (eb *EventBus) RuntimeMutationRunner() runtimepipeline.RuntimeMutationRunner {
	if eb == nil {
		return nil
	}
	runner, _ := eb.store.(runtimepipeline.RuntimeMutationRunner)
	return runner
}

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
	RuntimeInstanceID           string
	TestLifecycleProbe          runtimelifecycleprobe.Observer
	ProviderOutputVerifier      ProviderOutputAuthorizationVerifier
	WorkOwner                   worklifetime.Occurrence
}

const deliverySendTimeout = 250 * time.Millisecond

var ErrStaleRuntimeEpoch = errors.New("stale runtime epoch")

type inMemorySubscriberKind string

const (
	inMemorySubscriberAgent    inMemorySubscriberKind = "agent"
	inMemorySubscriberInternal inMemorySubscriberKind = "internal"
)

func closedSignal() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

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
		channels:                    make(map[events.EventType]map[string]chan *LocalDelivery),
		agentChans:                  make(map[string]chan *LocalDelivery),
		agentRouteHandles:           make(map[string]*agentRouteHandle),
		internalHandles:             make(map[string]*internalSubscriptionHandle),
		resetDone:                   closedSignal(),
		internalChanged:             make(chan struct{}),
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
		runtimeInstanceID:           strings.TrimSpace(opts.RuntimeInstanceID),
		testLifecycleProbe:          opts.TestLifecycleProbe,
		providerOutputVerifier:      opts.ProviderOutputVerifier,
		workOwner:                   opts.WorkOwner,
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
			if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func(actionCtx context.Context) {
				actionCtx = runtimepipeline.WithoutPipelineSQLConnContext(runtimepipeline.WithoutPipelineSQLTxContext(actionCtx))
				if err := table.AddFlowInstanceRoute(req); err != nil {
					_ = eb.LogRuntime(actionCtx, runtimepipeline.RuntimeLogEntry{
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
			if !runtimepipeline.QueuePipelineRollbackAction(ctx, func(context.Context) {
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

func (eb *EventBus) ResetInMemoryState() (resetErr error) {
	if eb == nil {
		return nil
	}
	eb.mu.Lock()
	if eb.resetInProgress {
		eb.mu.Unlock()
		return errors.New("event bus reset is already in progress")
	}
	routeTable, err := eb.deriveBootRouteTableLocked()
	if err != nil {
		eb.mu.Unlock()
		return err
	}
	eb.resetInProgress = true
	eb.resetDone = make(chan struct{})
	pendingClaims := make([]*pipelinePublicationClaim, 0, len(eb.pendingOutboxByID))
	for _, operations := range eb.pendingOutboxByID {
		for _, operation := range operations {
			pendingClaims = append(pendingClaims, operation.publicationClaim)
		}
	}
	routes := append([]*agentRouteHandle(nil), eb.retiringAgentRoutes...)
	for _, route := range eb.agentRouteHandles {
		route.deactivate()
		routes = append(routes, route)
	}
	internalHandles := append([]*internalSubscriptionHandle(nil), eb.retiringInternalHandles...)
	for _, handle := range eb.internalHandles {
		handle.deactivate()
		internalHandles = append(internalHandles, handle)
	}
	eb.channels = make(map[events.EventType]map[string]chan *LocalDelivery)
	eb.agentChans = make(map[string]chan *LocalDelivery)
	eb.agentRouteHandles = make(map[string]*agentRouteHandle)
	eb.internalHandles = make(map[string]*internalSubscriptionHandle)
	eb.subscriptions = make(map[string][]events.EventType)
	eb.subscriptionKinds = make(map[string]inMemorySubscriberKind)
	eb.pendingInternalByID = make(map[string][]events.DeliveryRoute)
	eb.pendingOutboxByID = make(map[string][]pendingOutboxOperation)
	eb.retiringAgentRoutes = nil
	eb.retiringInternalHandles = nil
	eb.routeTable = routeTable
	eb.rebuildRoutePlanners()
	eb.notifyInternalSubscriptionChangedLocked()
	eb.mu.Unlock()

	resetOpened := false
	defer func() {
		if resetOpened {
			return
		}
		eb.mu.Lock()
		if resetErr != nil {
			for _, route := range routes {
				eb.retainRetiringAgentRouteLocked(route)
			}
			for _, handle := range internalHandles {
				eb.retainRetiringInternalHandleLocked(handle)
			}
		}
		eb.resetInProgress = false
		close(eb.resetDone)
		resetOpened = true
		eb.notifyInternalSubscriptionChangedLocked()
		eb.mu.Unlock()
	}()

	// Retained queues and claims are lifecycle evidence. Prove their durable
	// handoff and settle their leases before erasing any in-memory owner map.
	for _, route := range routes {
		if retireErr := route.retireAndWait(context.Background(), eb.store); retireErr != nil {
			return retireErr
		}
	}
	for _, handle := range internalHandles {
		if retireErr := handle.retireAndWait(context.Background(), eb.store); retireErr != nil {
			return retireErr
		}
	}
	for _, claim := range pendingClaims {
		claim.Release(context.Background())
	}

	// Reset's deferred epilogue opens admission. Runners that acknowledged the
	// retire signal then resubscribe and report readiness through the same
	// lifecycle handle; no raw channel is silently reused.
	restartHandles := make([]*internalSubscriptionHandle, 0, len(internalHandles))
	for _, handle := range internalHandles {
		if handle.wantsRestart() {
			restartHandles = append(restartHandles, handle)
		}
	}
	eb.mu.Lock()
	eb.resetInProgress = false
	close(eb.resetDone)
	resetOpened = true
	eb.notifyInternalSubscriptionChangedLocked()
	eb.mu.Unlock()
	for _, handle := range restartHandles {
		restartCtx := handle.restartContext()
		if restartCtx == nil {
			return fmt.Errorf("internal subscriber %s restart lifecycle context is required", handle.subscriberID)
		}
		if restartCtx.Err() != nil {
			continue
		}
		if err := eb.waitForInternalSubscriptionReady(restartCtx, handle.subscriberID); err != nil {
			if restartCtx.Err() != nil {
				continue
			}
			return err
		}
	}
	return nil
}

func (eb *EventBus) WaitForQuiescence(ctx context.Context) error {
	if eb == nil {
		return nil
	}
	eb.mu.Lock()
	routes := append([]*agentRouteHandle(nil), eb.retiringAgentRoutes...)
	handles := append([]*internalSubscriptionHandle(nil), eb.retiringInternalHandles...)
	eb.mu.Unlock()
	for _, route := range routes {
		if err := route.retireAndWait(ctx, eb.store); err != nil {
			return err
		}
		eb.removeRetiringAgentRoute(route)
	}
	for _, handle := range handles {
		if err := handle.retireAndWait(ctx, eb.store); err != nil {
			return err
		}
		eb.removeRetiringInternalHandle(handle)
	}
	if eb.workOwner == nil {
		return nil
	}
	return eb.workOwner.WaitForQuiescence(ctx)
}

// AgentRoutePreparation owns an exact route generation before it becomes
// reachable. Agent lifecycle persistence can therefore fail without exposing a
// route, while post-commit publication failure still has one exact cleanup
// authority.
type AgentRoutePreparation interface {
	Deliveries() <-chan *LocalDelivery
	Publish() error
	Discard() error
}

type preparedAgentRoute struct {
	mu         sync.Mutex
	bus        *EventBus
	token      runtimeeffects.LifecycleToken
	eventTypes []events.EventType
	route      *agentRouteHandle
	ch         chan *LocalDelivery
	published  bool
	discarded  bool
}

func (p *preparedAgentRoute) Deliveries() <-chan *LocalDelivery {
	if p == nil {
		return nil
	}
	return p.ch
}

func (p *preparedAgentRoute) Publish() error {
	if p == nil || p.bus == nil || p.route == nil {
		return errors.New("prepared agent route is required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.published {
		return nil
	}
	if p.discarded {
		return errors.New("prepared agent route is no longer active")
	}
	eb := p.bus
	agentID := strings.TrimSpace(p.token.AgentID)
	eb.mu.Lock()
	old, oldInternal := eb.detachSubscriberLocked(agentID)
	eb.mu.Unlock()
	if old != nil {
		if err := old.retireAndWait(context.Background(), eb.store); err != nil {
			eb.retainRetiringAgentRoute(old)
			p.discarded = true
			_ = p.route.retireAndWait(context.Background(), eb.store)
			return fmt.Errorf("retire predecessor agent route: %w", err)
		}
	}
	if oldInternal != nil {
		if err := oldInternal.retireAndWait(context.Background(), eb.store); err != nil {
			eb.retainRetiringInternalHandle(oldInternal)
			p.discarded = true
			_ = p.route.retireAndWait(context.Background(), eb.store)
			return fmt.Errorf("retire predecessor internal route: %w", err)
		}
	}
	eb.mu.Lock()
	eb.agentChans[agentID] = p.ch
	eb.agentRouteHandles[agentID] = p.route
	eb.subscriptionKinds[agentID] = inMemorySubscriberAgent
	for _, eventType := range p.eventTypes {
		eventType = events.EventType(strings.TrimSpace(string(eventType)))
		if eventType == "" {
			continue
		}
		eb.subscriptions[agentID] = AppendUniqueEventType(eb.subscriptions[agentID], eventType)
		if eb.channels[eventType] == nil {
			eb.channels[eventType] = make(map[string]chan *LocalDelivery)
		}
		eb.channels[eventType][agentID] = p.ch
	}
	eb.mu.Unlock()
	p.published = true
	return nil
}

func (p *preparedAgentRoute) Discard() error {
	if p == nil || p.route == nil {
		return nil
	}
	p.mu.Lock()
	if p.discarded {
		p.mu.Unlock()
		return nil
	}
	p.discarded = true
	published := p.published
	eb := p.bus
	token := p.token
	route := p.route
	p.mu.Unlock()
	if published && eb != nil {
		eb.RemoveAgentRoute(token)
		return nil
	}
	if eb == nil {
		return nil
	}
	return route.retireAndWait(context.Background(), eb.store)
}

func (eb *EventBus) PrepareAgentRoute(token runtimeeffects.LifecycleToken, admission semanticview.FlowOwnedAgentSubscriptionAdmission) AgentRoutePreparation {
	if eb == nil || eb.workOwner == nil || !token.Valid() || !admission.ValidForAgent(token.AgentID) {
		return nil
	}
	eventTypes := admittedAgentEventTypes(admission)
	agentID := strings.TrimSpace(token.AgentID)
	owner, err := eb.workOwner.NewRoute(context.Background(), worklifetime.RouteIdentity{
		RuntimeEpoch: uint64(token.RuntimeEpoch), AgentID: agentID, Generation: token.Generation,
	})
	if err != nil {
		return nil
	}
	ch := make(chan *LocalDelivery, 128)
	route := newAgentRouteHandle(token, ch, owner)
	return &preparedAgentRoute{bus: eb, token: token, eventTypes: eventTypes, route: route, ch: ch}
}

// ReplaceAgentRoute remains the direct exact-generation operation for callers
// that have no separate durable lifecycle transition.
func (eb *EventBus) ReplaceAgentRoute(token runtimeeffects.LifecycleToken, admission semanticview.FlowOwnedAgentSubscriptionAdmission) <-chan *LocalDelivery {
	prepared := eb.PrepareAgentRoute(token, admission)
	if prepared == nil || prepared.Publish() != nil {
		return nil
	}
	return prepared.Deliveries()
}

func admittedAgentEventTypes(admission semanticview.FlowOwnedAgentSubscriptionAdmission) []events.EventType {
	patterns := admission.RoutePatterns()
	out := make([]events.EventType, 0, len(patterns))
	for _, pattern := range patterns {
		out = append(out, events.EventType(pattern))
	}
	return out
}

// RemoveAgentRoute removes only the exact generation that owns the route.
// Delayed predecessor cleanup is therefore harmless after replacement.
func (eb *EventBus) RemoveAgentRoute(token runtimeeffects.LifecycleToken) {
	if eb == nil || !token.Valid() {
		return
	}
	agentID := strings.TrimSpace(token.AgentID)
	eb.mu.Lock()
	if current := eb.agentRouteHandles[agentID]; current == nil || current.token != token {
		eb.mu.Unlock()
		return
	}
	route, _ := eb.detachSubscriberLocked(agentID)
	eb.mu.Unlock()
	if route != nil {
		if err := route.retireAndWait(context.Background(), eb.store); err != nil {
			eb.retainRetiringAgentRoute(route)
		}
	}
}

func (eb *EventBus) SubscribeInternal(ctx context.Context, subscriberID string, eventTypes ...events.EventType) (worklifetime.InternalSubscription, error) {
	if eb == nil || eb.workOwner == nil {
		return nil, errors.New("event bus runtime work owner is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	subscriberID = strings.TrimSpace(subscriberID)
	if subscriberID == "" {
		return nil, errors.New("internal subscriber id is required")
	}
	for {
		eb.mu.Lock()
		if eb.resetInProgress {
			resetDone := eb.resetDone
			eb.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-resetDone:
				continue
			}
		}
		if existing := eb.internalHandles[subscriberID]; existing != nil {
			eb.mu.Unlock()
			return nil, fmt.Errorf("internal subscriber %s already has an active generation", subscriberID)
		}
		handle := newInternalSubscriptionHandle(ctx, eb, subscriberID, eventTypes)
		eb.internalHandles[subscriberID] = handle
		eb.agentChans[subscriberID] = handle.ch
		eb.subscriptionKinds[subscriberID] = inMemorySubscriberInternal
		for _, eventType := range eventTypes {
			eventType = events.EventType(strings.TrimSpace(string(eventType)))
			if eventType == "" {
				continue
			}
			eb.subscriptions[subscriberID] = AppendUniqueEventType(eb.subscriptions[subscriberID], eventType)
			if eb.channels[eventType] == nil {
				eb.channels[eventType] = make(map[string]chan *LocalDelivery)
			}
			eb.channels[eventType][subscriberID] = handle.ch
		}
		eb.notifyInternalSubscriptionChangedLocked()
		eb.mu.Unlock()
		return handle, nil
	}
}

func (eb *EventBus) detachSubscriberLocked(agentID string) (*agentRouteHandle, *internalSubscriptionHandle) {
	var detached *agentRouteHandle
	var internal *internalSubscriptionHandle
	if route := eb.agentRouteHandles[agentID]; route != nil {
		route.deactivate()
		detached = route
	}
	if handle := eb.internalHandles[agentID]; handle != nil {
		handle.deactivate()
		internal = handle
	}
	delete(eb.agentChans, agentID)
	delete(eb.agentRouteHandles, agentID)
	delete(eb.internalHandles, agentID)
	delete(eb.subscriptions, agentID)
	delete(eb.subscriptionKinds, agentID)
	for et := range eb.channels {
		delete(eb.channels[et], agentID)
		if len(eb.channels[et]) == 0 {
			delete(eb.channels, et)
		}
	}
	eb.notifyInternalSubscriptionChangedLocked()
	return detached, internal
}

func (eb *EventBus) completeInternalSubscription(handle *internalSubscriptionHandle) error {
	if eb == nil || handle == nil {
		return nil
	}
	eb.mu.Lock()
	natural := eb.internalHandles[handle.subscriberID] == handle
	if natural {
		_, _ = eb.detachSubscriberLocked(handle.subscriberID)
	}
	eb.mu.Unlock()
	if !natural {
		return nil
	}
	if err := handle.retireAndWait(context.Background(), eb.store); err != nil {
		eb.retainRetiringInternalHandle(handle)
		return err
	}
	return nil
}

func (eb *EventBus) notifyInternalSubscriptionChanged() {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	eb.notifyInternalSubscriptionChangedLocked()
	eb.mu.Unlock()
}

func (eb *EventBus) notifyInternalSubscriptionChangedLocked() {
	if eb.internalChanged != nil {
		close(eb.internalChanged)
	}
	eb.internalChanged = make(chan struct{})
}

func (eb *EventBus) waitForInternalSubscriptionReady(ctx context.Context, subscriberID string) error {
	for {
		eb.mu.Lock()
		handle := eb.internalHandles[subscriberID]
		changed := eb.internalChanged
		eb.mu.Unlock()
		if handle != nil {
			select {
			case <-handle.ready:
				return nil
			default:
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for internal subscriber %s readiness: %w", subscriberID, ctx.Err())
		case <-changed:
		}
	}
}

func (eb *EventBus) retainRetiringAgentRoute(route *agentRouteHandle) {
	if eb == nil || route == nil {
		return
	}
	eb.mu.Lock()
	eb.retainRetiringAgentRouteLocked(route)
	eb.mu.Unlock()
}

func (eb *EventBus) retainRetiringAgentRouteLocked(route *agentRouteHandle) {
	for _, existing := range eb.retiringAgentRoutes {
		if existing == route {
			return
		}
	}
	eb.retiringAgentRoutes = append(eb.retiringAgentRoutes, route)
}

func (eb *EventBus) removeRetiringAgentRoute(route *agentRouteHandle) {
	if eb == nil || route == nil {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	for i, existing := range eb.retiringAgentRoutes {
		if existing == route {
			eb.retiringAgentRoutes = append(eb.retiringAgentRoutes[:i], eb.retiringAgentRoutes[i+1:]...)
			return
		}
	}
}

func (eb *EventBus) retainRetiringInternalHandle(handle *internalSubscriptionHandle) {
	if eb == nil || handle == nil {
		return
	}
	eb.mu.Lock()
	eb.retainRetiringInternalHandleLocked(handle)
	eb.mu.Unlock()
}

func (eb *EventBus) retainRetiringInternalHandleLocked(handle *internalSubscriptionHandle) {
	for _, existing := range eb.retiringInternalHandles {
		if existing == handle {
			return
		}
	}
	eb.retiringInternalHandles = append(eb.retiringInternalHandles, handle)
}

func (eb *EventBus) removeRetiringInternalHandle(handle *internalSubscriptionHandle) {
	if eb == nil || handle == nil {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	for i, existing := range eb.retiringInternalHandles {
		if existing == handle {
			eb.retiringInternalHandles = append(eb.retiringInternalHandles[:i], eb.retiringInternalHandles[i+1:]...)
			return
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
