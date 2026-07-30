package bus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// EventInterceptor runs deterministic coordination in the publish path.
// It may consume the inbound event and/or emit deferred events.
type EventInterceptor interface {
	Intercept(ctx context.Context, evt events.Event) (passthrough bool, deferred []events.Event, outcome runtimepipelineobligation.ExecutionOutcome, err error)
}

// DeliveryRouteInterceptor runs deterministic coordination for one
// authoritative delivery route. EventBus uses this for workflow-node delivery
// routes so Pipeline receives "execute this node for this route" semantics
// instead of inferring route authority from an event-wide context.
type DeliveryRouteInterceptor interface {
	InterceptDeliveryRoute(ctx context.Context, evt events.DeliveryEvent, route events.DeliveryRoute) (passthrough bool, deferred []events.Event, outcome runtimepipelineobligation.ExecutionOutcome, err error)
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
	pipelineObligations         runtimepipelineobligation.Store
	ephemeral                   bool
	logger                      LoggerHook
	semanticSource              semanticview.Source
	templateInstanceActivator   runtimepipeline.FlowInstanceActivator
	payloadValidator            PayloadValidator
	recipientPlanAdmissionGuard PublishRecipientPlanAdmissionGuard
	recipientPlanMaterializer   PublishRecipientPlanMaterializer
	recipientPlanGuard          PublishRecipientPlanGuard
	runtimeIngressDispatchGate  RuntimeIngressDispatchGate
	runDispatchGate             RunDispatchGate
	standingRunWorkOwner        StandingRunWorkOwner
	bundleSourceFact            runtimecorrelation.BundleSourceFact
	runtimeInstanceID           string
	testLifecycleProbe          runtimelifecycleprobe.Observer
	providerOutputVerifier      ProviderOutputAuthorizationVerifier
	outboxSweeperActive         bool
	outboxSweeperDone           chan struct{}
	pipelineSweepMu             pipelineSweepLock
	pipelineScans               map[runtimepipelineobligation.ScanRequest]*pipelineSweepScan
	workOwner                   worklifetime.Occurrence
}

// PipelineParentTransition excludes selected-store recovery scans while a
// parent lifecycle operation fences, drains, and terminalizes its run.
type PipelineParentTransition struct {
	once sync.Once
	bus  *EventBus
}

type pipelineSweepLock struct {
	once  sync.Once
	token chan struct{}
}

func (l *pipelineSweepLock) acquire(ctx context.Context) error {
	if ctx == nil {
		return errors.New("pipeline sweep lock context is required")
	}
	l.once.Do(func() {
		l.token = make(chan struct{}, 1)
		l.token <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.token:
		if err := ctx.Err(); err != nil {
			l.token <- struct{}{}
			return err
		}
		return nil
	}
}

func (l *pipelineSweepLock) release() {
	l.token <- struct{}{}
}

func (eb *EventBus) BeginPipelineParentTransition(ctx context.Context) (*PipelineParentTransition, error) {
	if eb == nil {
		return nil, errors.New("event bus is required")
	}
	if err := eb.pipelineSweepMu.acquire(ctx); err != nil {
		return nil, fmt.Errorf("acquire pipeline parent transition: %w", err)
	}
	return &PipelineParentTransition{bus: eb}, nil
}

func (t *PipelineParentTransition) Done() {
	if t == nil {
		return
	}
	t.once.Do(func() {
		t.bus.pipelineSweepMu.release()
	})
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

func (eb *EventBus) PipelineObligationOwner() runtimepipelineobligation.Store {
	if eb == nil {
		return nil
	}
	return eb.pipelineObligations
}

func (eb *EventBus) RunLifecycleCandidateOwner() runtimerunlifecycle.OperationOwner {
	if eb == nil {
		return nil
	}
	owner, _ := eb.store.(runtimerunlifecycle.OperationOwner)
	return owner
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

// RunOriginReader exposes only the typed construction authority needed to
// select a process-local owner for persisted pipeline recovery.
type RunOriginReader interface {
	LoadRunOrigin(context.Context, string) (runtimerunlifecycle.RunOrigin, error)
}

// StandingRunWorkOwner admits recovery work to the exact standing generation
// named by a persisted run origin. It must reject stale or fenced generations.
type StandingRunWorkOwner interface {
	BeginStandingRunRecovery(context.Context, string, runtimerunlifecycle.RunOrigin) (*worklifetime.Lease, error)
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
	BundleSourceFact            runtimecorrelation.BundleSourceFact
	RuntimeInstanceID           string
	TestLifecycleProbe          runtimelifecycleprobe.Observer
	ProviderOutputVerifier      ProviderOutputAuthorizationVerifier
	WorkOwner                   worklifetime.Occurrence
	PipelineObligations         runtimepipelineobligation.Store
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

type pipelineObligationStoreProvider interface {
	PipelineObligations() runtimepipelineobligation.Store
}

func NewEventBusWithOptions(store EventStore, opts EventBusOptions) (*EventBus, error) {
	if opts.PipelineObligations == nil {
		return nil, errors.New("durable event bus requires the pipeline obligation owner")
	}
	if err := opts.BundleSourceFact.Validate(); err != nil {
		return nil, fmt.Errorf("durable event bus requires an immutable bundle source fact: %w", err)
	}
	return newEventBusWithOptions(store, opts)
}

// NewEphemeralEventBus is the explicit non-durable constructor for isolated
// previews and tests. A selected store cannot cross this boundary.
func NewEphemeralEventBus(store EventStore) (*EventBus, error) {
	return NewEphemeralEventBusWithOptions(store, EventBusOptions{})
}

func NewEphemeralEventBusWithOptions(store EventStore, opts EventBusOptions) (*EventBus, error) {
	if opts.PipelineObligations != nil {
		return nil, errors.New("ephemeral event bus cannot accept a durable pipeline obligation owner")
	}
	if _, selected := store.(pipelineObligationStoreProvider); selected {
		return nil, errors.New("selected event store requires the durable event bus constructor")
	}
	eb, err := newEventBusWithOptions(store, opts)
	if err != nil {
		return nil, err
	}
	eb.ephemeral = true
	return eb, nil
}

func newEventBusWithOptions(store EventStore, opts EventBusOptions) (*EventBus, error) {
	if opts.PipelineObligations != nil {
		if err := opts.BundleSourceFact.Validate(); err != nil {
			return nil, fmt.Errorf("durable event bus requires an immutable bundle source fact: %w", err)
		}
	}
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
		pipelineObligations:         opts.PipelineObligations,
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
		bundleSourceFact:            opts.BundleSourceFact,
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

func (eb *EventBus) SetStandingRunWorkOwner(owner StandingRunWorkOwner) {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	eb.standingRunWorkOwner = owner
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

func (eb *EventBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	if eb == nil || eb.pipelineObligations == nil {
		if eb != nil && eb.ephemeral {
			return runtimepipelineobligation.GlobalWorkPresence{}, nil
		}
		return runtimepipelineobligation.GlobalWorkPresence{}, errors.New("pipeline obligation owner is required")
	}
	return eb.pipelineObligations.GlobalWorkPresence(ctx)
}

func (eb *EventBus) MarkDeliveryInProgress(ctx context.Context, agentID, sessionID string) (bool, error) {
	if eb == nil || eb.store == nil {
		return false, nil
	}
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return false, err
	}
	claim, ok := runtimedelivery.ClaimFromContext(ctx)
	if !ok || claim.SubscriberClass() != runtimedelivery.SubscriberAgent || claim.SubscriberID() != strings.TrimSpace(agentID) {
		return false, fmt.Errorf("agent session binding requires the exact current delivery claim")
	}
	owner, ok := eb.store.(runtimedelivery.Store)
	if !ok || owner == nil {
		return false, fmt.Errorf("selected event store does not expose delivery lifecycle ownership")
	}
	if _, err := owner.BindAgentSession(ctx, claim, sessionID); err != nil {
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

func (eb *EventBus) ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.Route, error) {
	if eb == nil || eb.store == nil {
		return nil, errors.New("event bus store is required")
	}
	store, ok := eb.store.(FlowInstanceRoutePersistence)
	if !ok || store == nil {
		return nil, errors.New("event bus store does not support flow-instance route persistence")
	}
	return store.ListFlowInstanceRoutes(ctx)
}

func (eb *EventBus) VerifyFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	if eb == nil || eb.store == nil {
		return errors.New("event bus store is required")
	}
	table := eb.RouteTable()
	if table == nil || !table.HasFlowInstanceRoute(identity) {
		return fmt.Errorf("flow-instance route %s is not process-ready", identity.InstancePath)
	}
	expected := table.MaterializedRoutes(identity)
	reader, ok := eb.store.(FlowInstanceRouteRecordReader)
	if !ok || reader == nil {
		return errors.New("event bus store does not expose exact flow-instance route records")
	}
	actual, err := reader.ListFlowInstanceRouteRecords(ctx, identity)
	if err != nil {
		return err
	}
	if !slices.Equal(flowInstanceRouteRecordKeys(actual), flowInstanceRouteRecordKeys(expected)) {
		return fmt.Errorf("flow-instance route %s persisted topology does not match process topology", identity.InstancePath)
	}
	return nil
}

func flowInstanceRouteRecordKeys(records []FlowInstanceRouteRecord) []string {
	keys := make([]string, 0, len(records))
	for _, record := range records {
		keys = append(keys, strings.Join([]string{
			strings.Trim(record.Identity.InstancePath, "/"),
			strings.TrimSpace(record.EventPattern),
			strings.TrimSpace(record.SubscriberType),
			strings.TrimSpace(record.SubscriberID),
			strings.TrimSpace(record.SourceFlow),
		}, "\x00"))
	}
	sort.Strings(keys)
	return keys
}

func (eb *EventBus) activeFlowInstanceDescriptorsForSemanticSource(
	ctx context.Context,
	lister ActiveFlowInstanceDescriptorLister,
) ([]ActiveFlowInstanceDescriptor, error) {
	if eb == nil || lister == nil {
		return nil, errors.New("event bus and active flow-instance descriptor owner are required")
	}
	descriptors, err := lister.ListActiveFlowInstanceDescriptors(ctx)
	if err != nil {
		return nil, err
	}
	if len(descriptors) == 0 {
		return nil, nil
	}
	eb.mu.RLock()
	source := eb.semanticSource
	sourceFact := eb.bundleSourceFact
	eb.mu.RUnlock()
	bundleHash, bundleSource := sourceFact.StorageValues()
	if bundleHash == "" || bundleSource == "" || source == nil || strings.TrimSpace(source.WorkflowVersion()) == "" {
		return nil, errors.New("flow-instance route topology requires exact EventBus semantic source")
	}
	workflowVersion := strings.TrimSpace(source.WorkflowVersion())
	out := make([]ActiveFlowInstanceDescriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		descriptor = descriptor.Normalized()
		if !descriptor.HasSemanticSource() {
			return nil, fmt.Errorf("active flow-instance descriptor %s is missing exact semantic source", descriptor.FlowInstance)
		}
		if descriptor.BundleHash != bundleHash ||
			descriptor.BundleSource != bundleSource ||
			descriptor.WorkflowVersion != workflowVersion {
			return nil, fmt.Errorf(
				"active flow-instance descriptor %s semantic source does not match the current EventBus source",
				descriptor.FlowInstance,
			)
		}
		out = append(out, descriptor)
	}
	return out, nil
}

func (eb *EventBus) deriveFlowInstanceRouteTopology(
	ctx context.Context,
	table *RouteTable,
	lister ActiveFlowInstanceDescriptorLister,
	include *FlowInstanceRouteMaterializationRequest,
	exclude runtimeflowidentity.Route,
) (*RouteTable, []runtimeflowidentity.Route, error) {
	staged := transactionRouteTableFromContext(ctx)
	if staged == nil {
		var err error
		staged, err = DeriveRouteTable(table.source)
		if err != nil {
			return nil, nil, fmt.Errorf("derive persisted flow-instance route table: %w", err)
		}
	}
	descriptors, err := eb.activeFlowInstanceDescriptorsForSemanticSource(ctx, lister)
	if err != nil {
		return nil, nil, fmt.Errorf("list active flow-instance route topology: %w", err)
	}
	identities := make(map[string]runtimeflowidentity.Route, len(descriptors)+1)
	exclude = runtimeflowidentity.StoredRoute(exclude.ScopeKey, exclude.InstanceID, exclude.InstancePath)
	if exclude.Valid() {
		if err := staged.RemoveFlowInstanceRoute(exclude); err != nil {
			return nil, nil, fmt.Errorf("exclude terminal flow-instance route %s: %w", exclude.InstancePath, err)
		}
	}
	for _, descriptor := range descriptors {
		identity := runtimeflowidentity.StoredRoute("", descriptor.InstanceID, descriptor.FlowInstance)
		if identity == exclude || (include != nil && identity == include.Identity) {
			continue
		}
		templateID, found := staged.flowInstanceTemplateID(identity)
		if !found {
			continue
		}
		if descriptor.FlowTemplate != templateID {
			return nil, nil, fmt.Errorf(
				"active flow-instance descriptor %s template %s does not match route template %s for scope %s",
				identity.InstancePath,
				descriptor.FlowTemplate,
				templateID,
				identity.ScopeKey,
			)
		}
		if err := staged.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{
			Identity:            identity,
			ActivationVariables: descriptor.AddressFields,
		}); err != nil {
			return nil, nil, fmt.Errorf("derive active flow-instance route %s: %w", identity.InstancePath, err)
		}
		identities[identity.InstancePath] = identity
	}
	if include != nil {
		req := include.Normalized()
		if err := staged.AddFlowInstanceRoute(req); err != nil {
			return nil, nil, err
		}
		identities[req.Identity.InstancePath] = req.Identity
	}
	out := make([]runtimeflowidentity.Route, 0, len(identities))
	for _, identity := range identities {
		out = append(out, identity)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstancePath < out[j].InstancePath })
	return staged, out, nil
}

func (eb *EventBus) AddFlowInstanceRoute(req FlowInstanceRouteMaterializationRequest) error {
	return eb.AddFlowInstanceRouteContext(context.Background(), req)
}

// PublishPersistedFlowInstanceRoute makes already-persisted route truth
// process-visible without rewriting storage.
func (eb *EventBus) PublishPersistedFlowInstanceRoute(req FlowInstanceRouteMaterializationRequest) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	if _, err := eb.admitBundleSourceFact(context.Background()); err != nil {
		return err
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	return table.AddFlowInstanceRoute(req.Normalized())
}

// RetirePublishedFlowInstanceRoute removes process-visible route truth without
// changing its durable lifecycle.
func (eb *EventBus) RetirePublishedFlowInstanceRoute(identity runtimeflowidentity.Route) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	return table.RemoveFlowInstanceRoute(identity)
}

// StageFlowInstanceRouteContext persists the exact derived route set but keeps
// it process-invisible until its topology owner publishes it.
func (eb *EventBus) StageFlowInstanceRouteContext(ctx context.Context, req FlowInstanceRouteMaterializationRequest) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return err
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	persister, ok := eb.store.(FlowInstanceRouteSetPersistence)
	if !ok || persister == nil {
		return errors.New("exact flow-instance route-set persistence is required")
	}
	descriptorLister, ok := eb.store.(ActiveFlowInstanceDescriptorLister)
	if !ok || descriptorLister == nil {
		return errors.New("flow-instance route staging requires active flow-instance descriptors")
	}
	req = req.Normalized()
	stage := func(txctx context.Context) error {
		staged, identities, err := eb.deriveFlowInstanceRouteTopology(
			txctx,
			table,
			descriptorLister,
			&req,
			runtimeflowidentity.Route{},
		)
		if err != nil {
			return err
		}
		for _, identity := range identities {
			routes := staged.MaterializedRoutes(identity)
			if err := persister.ReplaceFlowInstanceRouteRecords(txctx, identity, routes); err != nil {
				return err
			}
		}
		return nil
	}
	if _, active := runtimepipeline.PipelineSQLTxFromContext(ctx); active {
		return stage(ctx)
	}
	runner := eb.RuntimeMutationRunner()
	if runner == nil {
		return errors.New("flow-instance route staging requires selected runtime mutation")
	}
	return runner.RunRuntimeMutationContext(ctx, stage)
}

func (eb *EventBus) AddFlowInstanceRouteContext(ctx context.Context, req FlowInstanceRouteMaterializationRequest) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return err
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
			postCommitCtx := runtimepipeline.WithoutPipelineSQLConnContext(
				runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(ctx)),
			)
			if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func(context.Context) {
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
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return err
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
		owner = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
		if !owner.Valid() {
			return fmt.Errorf("flow-instance route removal requires exact identity")
		}
	}
	persister, ok := eb.store.(FlowInstanceRouteSetPersistence)
	if !ok || persister == nil {
		if _, selected := eb.store.(pipelineObligationStoreProvider); selected {
			return errors.New("selected store requires exact flow-instance route-set persistence")
		}
		return table.RemoveFlowInstanceRoute(owner)
	}
	descriptorLister, ok := eb.store.(ActiveFlowInstanceDescriptorLister)
	if !ok || descriptorLister == nil {
		return errors.New("flow-instance route removal requires active flow-instance descriptors")
	}
	stage := func(txctx context.Context) error {
		staged, identities, err := eb.deriveFlowInstanceRouteTopology(txctx, table, descriptorLister, nil, owner)
		if err != nil {
			return err
		}
		identities = append(identities, owner)
		sort.Slice(identities, func(i, j int) bool { return identities[i].InstancePath < identities[j].InstancePath })
		for _, affected := range identities {
			if err := persister.ReplaceFlowInstanceRouteRecords(txctx, affected, staged.MaterializedRoutes(affected)); err != nil {
				return err
			}
		}
		return nil
	}
	if _, active := runtimepipeline.PipelineSQLTxFromContext(ctx); active {
		if err := stage(ctx); err != nil {
			return err
		}
		if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func(context.Context) {
			_ = table.RemoveFlowInstanceRoute(owner)
		}) {
			return errors.New("flow-instance route removal requires post-commit process retirement owner")
		}
		return nil
	}
	runner := eb.RuntimeMutationRunner()
	if runner == nil {
		return errors.New("flow-instance route removal requires selected runtime mutation")
	}
	if err := runner.RunRuntimeMutationContext(ctx, stage); err != nil {
		return err
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
	cleanupCtx, err := eb.admitBundleSourceFact(context.Background())
	if err != nil {
		return err
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
	pendingOperations := make([]pendingOutboxOperation, 0, len(eb.pendingOutboxByID))
	for _, operations := range eb.pendingOutboxByID {
		for _, operation := range operations {
			pendingOperations = append(pendingOperations, operation)
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
	eb.retiringAgentRoutes = nil
	eb.retiringInternalHandles = nil
	eb.routeTable = routeTable
	eb.rebuildRoutePlanners()
	eb.notifyInternalSubscriptionChangedLocked()
	eb.mu.Unlock()

	resetOpened := false
	retirementSucceeded := false
	defer func() {
		if resetOpened {
			return
		}
		eb.mu.Lock()
		if resetErr != nil && !retirementSucceeded {
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
	var retirementErr error
	for _, route := range routes {
		retirementErr = errors.Join(retirementErr, route.retireAndWait(cleanupCtx, eb.store))
	}
	for _, handle := range internalHandles {
		retirementErr = errors.Join(retirementErr, handle.retireAndWait(cleanupCtx, eb.store))
	}
	if retirementErr != nil {
		return retirementErr
	}
	retirementSucceeded = true
	var releaseErr error
	for _, operation := range pendingOperations {
		releaseErr = errors.Join(releaseErr, operation.publicationClaim.Release(cleanupCtx))
	}
	for _, operation := range pendingOperations {
		eb.removePendingOutboxOperation(operation.intent.Event.ID(), operation.sequence)
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
			return errors.Join(releaseErr, fmt.Errorf("internal subscriber %s restart lifecycle context is required", handle.subscriberID))
		}
		if restartCtx.Err() != nil {
			continue
		}
		if err := eb.waitForInternalSubscriptionReady(restartCtx, handle.subscriberID); err != nil {
			if restartCtx.Err() != nil {
				continue
			}
			return errors.Join(releaseErr, err)
		}
	}
	return releaseErr
}

func (eb *EventBus) WaitForQuiescence(ctx context.Context) error {
	if eb == nil {
		return nil
	}
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return err
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
	mu           sync.Mutex
	bus          *EventBus
	lifecycleCtx context.Context
	token        runtimeeffects.LifecycleToken
	eventTypes   []events.EventType
	route        *agentRouteHandle
	ch           chan *LocalDelivery
	published    bool
	discarded    bool
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
	cleanupCtx, err := p.bus.admitBundleSourceFact(p.lifecycleCtx)
	if err != nil {
		return err
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
		if err := old.retireAndWait(cleanupCtx, eb.store); err != nil {
			eb.retainRetiringAgentRoute(old)
			p.discarded = true
			_ = p.route.retireAndWait(cleanupCtx, eb.store)
			return fmt.Errorf("retire predecessor agent route: %w", err)
		}
	}
	if oldInternal != nil {
		if err := oldInternal.retireAndWait(cleanupCtx, eb.store); err != nil {
			eb.retainRetiringInternalHandle(oldInternal)
			p.discarded = true
			_ = p.route.retireAndWait(cleanupCtx, eb.store)
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
	eb := p.bus
	if eb == nil {
		return nil
	}
	cleanupCtx, err := eb.admitBundleSourceFact(p.lifecycleCtx)
	if err != nil {
		return err
	}
	p.mu.Lock()
	if p.discarded {
		p.mu.Unlock()
		return nil
	}
	p.discarded = true
	published := p.published
	token := p.token
	route := p.route
	p.mu.Unlock()
	if published {
		eb.RemoveAgentRoute(token)
		return nil
	}
	return route.retireAndWait(cleanupCtx, eb.store)
}

func (eb *EventBus) PrepareAgentRoute(token runtimeeffects.LifecycleToken, admission semanticview.FlowOwnedAgentSubscriptionAdmission) AgentRoutePreparation {
	if eb == nil || eb.workOwner == nil || !token.Valid() || !admission.ValidForAgent(token.AgentID) {
		return nil
	}
	lifecycleCtx, err := eb.admitBundleSourceFact(context.Background())
	if err != nil {
		return nil
	}
	eventTypes := admittedAgentEventTypes(admission)
	agentID := strings.TrimSpace(token.AgentID)
	owner, err := eb.workOwner.NewRoute(lifecycleCtx, worklifetime.RouteIdentity{
		RuntimeEpoch: uint64(token.RuntimeEpoch), AgentID: agentID, Generation: token.Generation,
	})
	if err != nil {
		return nil
	}
	ch := make(chan *LocalDelivery, 128)
	route := newAgentRouteHandle(token, ch, owner)
	return &preparedAgentRoute{
		bus: eb, lifecycleCtx: lifecycleCtx, token: token,
		eventTypes: eventTypes, route: route, ch: ch,
	}
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
	cleanupCtx, err := eb.admitBundleSourceFact(context.Background())
	if err != nil {
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
		if err := route.retireAndWait(cleanupCtx, eb.store); err != nil {
			eb.retainRetiringAgentRoute(route)
		}
	}
}

func (eb *EventBus) SubscribeInternal(ctx context.Context, subscriberID string, eventTypes ...events.EventType) (worklifetime.InternalSubscription, error) {
	if eb == nil || eb.workOwner == nil {
		return nil, errors.New("event bus runtime work owner is required")
	}
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return nil, err
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
	cleanupCtx, err := eb.admitBundleSourceFact(context.WithoutCancel(handle.lifecycleCtx))
	if err != nil {
		return err
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
	if err := handle.retireAndWait(cleanupCtx, eb.store); err != nil {
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
