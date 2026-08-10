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
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
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
	channels                    map[events.EventType]map[subscriberKey]chan *LocalDelivery
	agentChans                  map[agentidentity.Identity]chan *LocalDelivery
	agentRouteHandles           map[agentidentity.Identity]*agentRouteHandle
	internalHandles             map[string]*internalSubscriptionHandle
	retiringAgentRoutes         []*agentRouteHandle
	retiringInternalHandles     []*internalSubscriptionHandle
	resetInProgress             bool
	resetDone                   chan struct{}
	internalChanged             chan struct{}
	subscriptions               map[subscriberKey][]events.EventType
	pendingInternalByID         map[string][]events.DeliveryRoute
	pendingOutboxByID           map[string][]pendingOutboxOperation
	pendingOutboxSequence       uint64
	routeTable                  *RouteTable
	runtimeAgentDescriptors     map[agentidentity.Identity]ActiveAgentDescriptor
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
	templateInstancePlanner     runtimepipeline.FlowInstanceActivationPlanner
	flowActivationFinalizer     runtimepipeline.CommittedFlowInstanceActivationFinalizer
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
	receiverExecution           eventreceiver.ExecutionVariant
	deliveryAuthority           runtimedelivery.ExecutionAuthority
	deliveryContinuations       DeliveryContinuationOwner
	durable                     DurableDependencies
	executionPosture            executionposture.Posture
}

// DurableDependencies is the exact selected-store contract consumed by a
// durable EventBus. EventStore is never inspected to discover these roles.
type DurableDependencies struct {
	ReplyContext          runtimereplycontext.Store
	RunLifecycle          runtimerunlifecycle.OperationOwner
	DeliveryLifecycle     runtimedelivery.Store
	FlowRoutes            FlowInstanceRoutePersistence
	FlowRouteRecords      FlowInstanceRouteRecordReader
	FlowRouteSets         FlowInstanceRouteSetPersistence
	FlowRouteTopology     FlowInstanceRouteTopologyPersistence
	FlowRouteRollback     FlowInstanceRouteRollbackPersistence
	ActiveAgents          ActiveAgentDescriptorLister
	ActiveFlows           ActiveFlowInstanceDescriptorLister
	DeliveryTargets       EventDeliveryTargetReader
	DeliveryRouteSets     EventDeliveryRouteSetReader
	TargetFailureRecorder TargetFailureDeadLetterRecorder
	RunOrigins            RunOriginReader
}

func (d DurableDependencies) validate() error {
	required := []struct {
		name  string
		value any
	}{
		{"run lifecycle owner", d.RunLifecycle},
		{"delivery lifecycle owner", d.DeliveryLifecycle},
		{"flow route owner", d.FlowRoutes},
		{"flow route record reader", d.FlowRouteRecords},
		{"flow route set owner", d.FlowRouteSets},
		{"flow route topology owner", d.FlowRouteTopology},
		{"flow route rollback owner", d.FlowRouteRollback},
		{"active agent descriptor reader", d.ActiveAgents},
		{"active flow descriptor reader", d.ActiveFlows},
		{"delivery target reader", d.DeliveryTargets},
		{"delivery route reader", d.DeliveryRouteSets},
		{"target failure recorder", d.TargetFailureRecorder},
		{"run origin reader", d.RunOrigins},
	}
	for _, role := range required {
		if role.value == nil {
			return fmt.Errorf("durable event bus requires explicit %s", role.name)
		}
	}
	return nil
}

// DeliveryContinuationOwner is the exact transfer boundary between committed
// publication, the normal generation coordinator, carriers, and attempts.
type DeliveryContinuationOwner interface {
	AcceptCommitted([]runtimedelivery.DurableHandoffProof) error
	Acquire(string) (worklifetime.DeliveryContinuation, error)
	Retain(runtimedelivery.Snapshot) error
	Release(string) error
	OwnsPersistedRecovery() bool
	Signal()
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
	return eb.durable.RunLifecycle
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

type ExactDirectRouteStatus struct {
	Requested   []events.DeliveryRoute
	Deliverable []events.DeliveryRoute
	Missing     []events.DeliveryRoute
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
	ExecutionPosture            executionposture.Posture
	Logger                      LoggerHook
	Interceptors                []EventInterceptor
	InterceptorProvider         func() []EventInterceptor
	ContractBundle              semanticview.Source
	RouteTable                  *RouteTable
	TemplateInstanceActivator   runtimepipeline.FlowInstanceActivator
	TemplateInstancePlanner     runtimepipeline.FlowInstanceActivationPlanner
	FlowActivationFinalizer     runtimepipeline.CommittedFlowInstanceActivationFinalizer
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
	ReceiverExecution           eventreceiver.ExecutionVariant
	PipelineObligations         runtimepipelineobligation.Store
	DeliveryAuthority           runtimedelivery.ExecutionAuthority
	Durable                     DurableDependencies
}

const deliverySendTimeout = 250 * time.Millisecond

var ErrStaleRuntimeEpoch = errors.New("stale runtime epoch")

type inMemorySubscriberKind string

const (
	inMemorySubscriberAgent    inMemorySubscriberKind = "agent"
	inMemorySubscriberInternal inMemorySubscriberKind = "internal"
)

type subscriberKey struct {
	kind       inMemorySubscriberKind
	agent      agentidentity.Identity
	internalID string
}

func agentSubscriptionKey(identity agentidentity.Identity) (subscriberKey, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return subscriberKey{}, err
	}
	return subscriberKey{kind: inMemorySubscriberAgent, agent: identity}, nil
}

func internalSubscriptionKey(subscriberID string) (subscriberKey, error) {
	subscriberID = strings.TrimSpace(subscriberID)
	if subscriberID == "" {
		return subscriberKey{}, errors.New("internal subscriber id is required")
	}
	return subscriberKey{kind: inMemorySubscriberInternal, internalID: subscriberID}, nil
}

func (k subscriberKey) subscriberID() string {
	if k.kind == inMemorySubscriberAgent {
		return k.agent.AgentID()
	}
	return strings.TrimSpace(k.internalID)
}

func closedSignal() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func NewEventBusWithOptions(store EventStore, opts EventBusOptions) (*EventBus, error) {
	if !opts.ExecutionPosture.Valid() {
		return nil, errors.New("durable event bus requires a valid execution posture")
	}
	if opts.PipelineObligations == nil {
		return nil, errors.New("durable event bus requires the pipeline obligation owner")
	}
	if err := opts.BundleSourceFact.Validate(); err != nil {
		return nil, fmt.Errorf("durable event bus requires an immutable bundle source fact: %w", err)
	}
	if err := opts.Durable.validate(); err != nil {
		return nil, err
	}
	if opts.DeliveryAuthority.Kind() != "" {
		if err := opts.DeliveryAuthority.Validate(); err != nil {
			return nil, fmt.Errorf("durable event bus delivery execution authority: %w", err)
		}
	}
	return newEventBusWithOptions(store, opts)
}

func (eb *EventBus) SetDeliveryAuthority(authority runtimedelivery.ExecutionAuthority) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	if err := authority.Validate(); err != nil {
		return err
	}
	eb.mu.Lock()
	eb.deliveryAuthority = authority
	eb.mu.Unlock()
	return nil
}

// FinalizeSelectedReceiverAdmission installs provider-preflight evidence before
// the selected EventBus begins dispatching receiver work.
func (eb *EventBus) FinalizeSelectedReceiverAdmission(admission managedexecution.Admission) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	variant, err := eb.receiverExecution.WithSelectedAdmission(admission)
	if err != nil {
		return fmt.Errorf("finalize selected event bus receiver admission: %w", err)
	}
	eb.receiverExecution = variant
	return nil
}

func (eb *EventBus) SetDeliveryContinuationOwner(owner DeliveryContinuationOwner) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	if owner == nil {
		return errors.New("delivery continuation owner is required")
	}
	eb.mu.Lock()
	eb.deliveryContinuations = owner
	eb.mu.Unlock()
	return nil
}

func (eb *EventBus) DeliveryContinuationOwner() DeliveryContinuationOwner {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.deliveryContinuations
}

func (eb *EventBus) AcquireDeliveryContinuation(deliveryID string) (worklifetime.DeliveryContinuation, error) {
	owner := eb.DeliveryContinuationOwner()
	if owner == nil {
		return nil, errors.New("delivery continuation owner is required")
	}
	return owner.Acquire(deliveryID)
}

func (eb *EventBus) RetainDeliveryContinuation(snapshot runtimedelivery.Snapshot) error {
	owner := eb.DeliveryContinuationOwner()
	if owner == nil {
		return errors.New("delivery continuation owner is required")
	}
	return owner.Retain(snapshot)
}

func (eb *EventBus) ReleaseDeliveryContinuation(deliveryID string) error {
	owner := eb.DeliveryContinuationOwner()
	if owner == nil {
		return errors.New("delivery continuation owner is required")
	}
	return owner.Release(deliveryID)
}

func (eb *EventBus) SignalDeliveryContinuations() {
	if owner := eb.DeliveryContinuationOwner(); owner != nil {
		owner.Signal()
	}
}

func (eb *EventBus) DeliveryAuthority() (runtimedelivery.ExecutionAuthority, error) {
	if eb == nil {
		return runtimedelivery.ExecutionAuthority{}, errors.New("event bus is required")
	}
	eb.mu.RLock()
	authority := eb.deliveryAuthority
	eb.mu.RUnlock()
	if err := authority.Validate(); err != nil {
		return runtimedelivery.ExecutionAuthority{}, err
	}
	return authority, nil
}

// NewEphemeralEventBus is the explicit non-durable constructor for isolated
// previews and tests. A selected store cannot cross this boundary.
func NewEphemeralEventBus(store EventStore) (*EventBus, error) {
	return NewEphemeralEventBusWithOptions(store, EventBusOptions{
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
}

func NewEphemeralEventBusWithOptions(store EventStore, opts EventBusOptions) (*EventBus, error) {
	if opts.PipelineObligations != nil {
		return nil, errors.New("ephemeral event bus cannot accept a durable pipeline obligation owner")
	}
	eb, err := newEventBusWithOptions(store, opts)
	if err != nil {
		return nil, err
	}
	eb.ephemeral = true
	return eb, nil
}

func newEventBusWithOptions(store EventStore, opts EventBusOptions) (*EventBus, error) {
	if err := opts.ReceiverExecution.Validate(); err != nil {
		return nil, fmt.Errorf("event bus receiver execution: %w", err)
	}
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
		channels:                    make(map[events.EventType]map[subscriberKey]chan *LocalDelivery),
		agentChans:                  make(map[agentidentity.Identity]chan *LocalDelivery),
		agentRouteHandles:           make(map[agentidentity.Identity]*agentRouteHandle),
		internalHandles:             make(map[string]*internalSubscriptionHandle),
		resetDone:                   closedSignal(),
		internalChanged:             make(chan struct{}),
		subscriptions:               make(map[subscriberKey][]events.EventType),
		runtimeAgentDescriptors:     make(map[agentidentity.Identity]ActiveAgentDescriptor),
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
		templateInstancePlanner:     opts.TemplateInstancePlanner,
		flowActivationFinalizer:     opts.FlowActivationFinalizer,
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
		receiverExecution:           opts.ReceiverExecution,
		deliveryAuthority:           opts.DeliveryAuthority,
		durable:                     opts.Durable,
		executionPosture:            opts.ExecutionPosture,
	}
	if opts.DeliveryAuthority.Kind() == runtimedelivery.ExecutionAuthoritySelectedContractFork {
		transfers, err := newSelectedDeliveryTransfers(opts.DeliveryAuthority)
		if err != nil {
			return nil, err
		}
		eb.deliveryContinuations = transfers
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
	eb.connectRoutePlanner = newConnectRoutePlanResolver(eb.semanticSource, eb.routeTable, eb.PinRoutingDescriptors, eb.templateInstanceActivator, eb.templateInstancePlanner, eb.durable.ReplyContext)
	eb.connectRoutePlanner.loadAgents = eb.activeAgentDescriptors
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
	owner := eb.durable.DeliveryLifecycle
	if owner == nil {
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
	store := eb.durable.FlowRoutes
	if store == nil {
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
	reader := eb.durable.FlowRouteRecords
	if reader == nil {
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

type flowInstanceRouteRecordIdentity struct {
	instancePath   string
	eventPattern   string
	subscriberType string
	subscriberID   string
	sourceFlow     string
}

func flowInstanceRouteRecordKeys(records []FlowInstanceRouteRecord) []flowInstanceRouteRecordIdentity {
	keys := make([]flowInstanceRouteRecordIdentity, 0, len(records))
	for _, record := range records {
		keys = append(keys, flowInstanceRouteRecordIdentity{
			instancePath:   strings.Trim(record.Identity.InstancePath, "/"),
			eventPattern:   strings.TrimSpace(record.EventPattern),
			subscriberType: strings.TrimSpace(record.SubscriberType),
			subscriberID:   strings.TrimSpace(record.SubscriberID),
			sourceFlow:     strings.TrimSpace(record.SourceFlow),
		})
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].instancePath != keys[j].instancePath {
			return keys[i].instancePath < keys[j].instancePath
		}
		if keys[i].eventPattern != keys[j].eventPattern {
			return keys[i].eventPattern < keys[j].eventPattern
		}
		if keys[i].subscriberType != keys[j].subscriberType {
			return keys[i].subscriberType < keys[j].subscriberType
		}
		if keys[i].subscriberID != keys[j].subscriberID {
			return keys[i].subscriberID < keys[j].subscriberID
		}
		return keys[i].sourceFlow < keys[j].sourceFlow
	})
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
	staged, err := DeriveRouteTable(table.source)
	if err != nil {
		return nil, nil, fmt.Errorf("derive persisted flow-instance route table: %w", err)
	}
	descriptors, err := eb.activeFlowInstanceDescriptorsForSemanticSource(ctx, lister)
	if err != nil {
		return nil, nil, fmt.Errorf("list active flow-instance route topology: %w", err)
	}
	identities := make(map[string]runtimeflowidentity.Route, len(descriptors)+1)
	exclude = runtimeflowidentity.StoredRoute(exclude.ScopeKey, exclude.InstanceID, exclude.InstancePath)
	if exclude.Valid() {
		if err := staged.removeFlowInstanceRouteForContext(ctx, exclude); err != nil {
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
		if err := staged.addFlowInstanceRouteForContext(ctx, FlowInstanceRouteMaterializationRequest{
			Identity:            identity,
			ActivationVariables: descriptor.AddressFields,
		}); err != nil {
			return nil, nil, fmt.Errorf("derive active flow-instance route %s: %w", identity.InstancePath, err)
		}
		identities[identity.InstancePath] = identity
	}
	if include != nil {
		req := include.Normalized()
		if err := staged.addFlowInstanceRouteForContext(ctx, req); err != nil {
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

func flowInstanceRouteTopologyRecordSets(table *RouteTable, identities []runtimeflowidentity.Route) []FlowInstanceRouteRecordSet {
	sets := make([]FlowInstanceRouteRecordSet, 0, len(identities))
	for _, identity := range identities {
		sets = append(sets, FlowInstanceRouteRecordSet{
			Identity: identity,
			Routes:   table.MaterializedRoutes(identity),
		})
	}
	return sets
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
	persister := eb.durable.FlowRouteTopology
	if persister == nil {
		return errors.New("exact flow-instance route-topology persistence is required")
	}
	descriptorLister := eb.durable.ActiveFlows
	if descriptorLister == nil {
		return errors.New("flow-instance route staging requires active flow-instance descriptors")
	}
	req = req.Normalized()
	staged, identities, err := eb.deriveFlowInstanceRouteTopology(
		ctx,
		table,
		descriptorLister,
		&req,
		runtimeflowidentity.Route{},
	)
	if err != nil {
		return err
	}
	return persister.ReplaceFlowInstanceRouteTopology(ctx, flowInstanceRouteTopologyRecordSets(staged, identities))
}

func (eb *EventBus) AddFlowInstanceRouteContext(ctx context.Context, req FlowInstanceRouteMaterializationRequest) error {
	if err := eb.StageFlowInstanceRouteContext(ctx, req); err != nil {
		return err
	}
	return eb.PublishPersistedFlowInstanceRoute(req)
}

func (eb *EventBus) RemoveFlowInstanceRoute(identity runtimeflowidentity.Route) error {
	return eb.RemoveFlowInstanceRouteContext(context.Background(), identity)
}

// RetireCommittedFlowInstanceRoute applies selected-store commit evidence to
// process-local routing. Durable route retirement has already committed.
func (eb *EventBus) RetireCommittedFlowInstanceRoute(identity runtimeflowidentity.Route) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	return table.removeFlowInstanceRouteForContext(context.Background(), identity)
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
	persister := eb.durable.FlowRouteTopology
	if persister == nil {
		if eb.ephemeral {
			return table.removeFlowInstanceRouteForContext(ctx, owner)
		}
		return errors.New("selected store requires exact flow-instance route-set persistence")
	}
	descriptorLister := eb.durable.ActiveFlows
	if descriptorLister == nil {
		return errors.New("flow-instance route removal requires active flow-instance descriptors")
	}
	staged, identities, err := eb.deriveFlowInstanceRouteTopology(ctx, table, descriptorLister, nil, owner)
	if err != nil {
		return err
	}
	identities = append(identities, owner)
	sort.Slice(identities, func(i, j int) bool { return identities[i].InstancePath < identities[j].InstancePath })
	sets := flowInstanceRouteTopologyRecordSets(staged, identities)
	if err := persister.ReplaceFlowInstanceRouteTopology(ctx, sets); err != nil {
		return err
	}
	return table.removeFlowInstanceRouteForContext(ctx, owner)
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
	eb.channels = make(map[events.EventType]map[subscriberKey]chan *LocalDelivery)
	eb.agentChans = make(map[agentidentity.Identity]chan *LocalDelivery)
	eb.agentRouteHandles = make(map[agentidentity.Identity]*agentRouteHandle)
	eb.internalHandles = make(map[string]*internalSubscriptionHandle)
	eb.subscriptions = make(map[subscriberKey][]events.EventType)
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
	identity := p.token.Identity.Normalize()
	key, err := agentSubscriptionKey(identity)
	if err != nil {
		return fmt.Errorf("prepared agent route identity: %w", err)
	}
	eb.mu.Lock()
	old := eb.detachAgentSubscriberLocked(identity)
	eb.mu.Unlock()
	if old != nil {
		if err := old.retireAndWait(cleanupCtx, eb.store); err != nil {
			eb.retainRetiringAgentRoute(old)
			p.discarded = true
			_ = p.route.retireAndWait(cleanupCtx, eb.store)
			return fmt.Errorf("retire predecessor agent route: %w", err)
		}
	}
	eb.mu.Lock()
	eb.agentChans[identity] = p.ch
	eb.agentRouteHandles[identity] = p.route
	for _, eventType := range p.eventTypes {
		eventType = events.EventType(strings.TrimSpace(string(eventType)))
		if eventType == "" {
			continue
		}
		eb.subscriptions[key] = AppendUniqueEventType(eb.subscriptions[key], eventType)
		if eb.channels[eventType] == nil {
			eb.channels[eventType] = make(map[subscriberKey]chan *LocalDelivery)
		}
		eb.channels[eventType][key] = p.ch
	}
	eb.mu.Unlock()
	p.published = true
	eb.SignalDeliveryContinuations()
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
	owner, err := eb.workOwner.NewRoute(lifecycleCtx, worklifetime.RouteIdentity{
		RuntimeEpoch: uint64(token.RuntimeEpoch), Agent: token.Identity, Generation: token.Generation,
	})
	if err != nil {
		return nil
	}
	ch := make(chan *LocalDelivery, 128)
	route := newAgentRouteHandle(eb, token, ch, owner)
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

// FenceAgentRoute closes admission for the exact route generation without
// waiting for work already accepted by that route to settle.
func (eb *EventBus) FenceAgentRoute(token runtimeeffects.LifecycleToken) {
	if eb == nil || !token.Valid() {
		return
	}
	identity := token.Identity.Normalize()
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if current := eb.agentRouteHandles[identity]; current != nil && current.token == token {
		current.deactivate()
	}
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
	identity := token.Identity.Normalize()
	eb.mu.Lock()
	if current := eb.agentRouteHandles[identity]; current == nil || current.token != token {
		eb.mu.Unlock()
		return
	}
	route := eb.detachAgentSubscriberLocked(identity)
	eb.mu.Unlock()
	if route != nil {
		if err := route.retireAndWait(cleanupCtx, eb.store); err != nil {
			eb.retainRetiringAgentRoute(route)
		}
	}
	eb.SignalDeliveryContinuations()
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
		key, keyErr := internalSubscriptionKey(subscriberID)
		if keyErr != nil {
			eb.mu.Unlock()
			return nil, keyErr
		}
		eb.internalHandles[subscriberID] = handle
		for _, eventType := range eventTypes {
			eventType = events.EventType(strings.TrimSpace(string(eventType)))
			if eventType == "" {
				continue
			}
			eb.subscriptions[key] = AppendUniqueEventType(eb.subscriptions[key], eventType)
			if eb.channels[eventType] == nil {
				eb.channels[eventType] = make(map[subscriberKey]chan *LocalDelivery)
			}
			eb.channels[eventType][key] = handle.ch
		}
		eb.notifyInternalSubscriptionChangedLocked()
		eb.mu.Unlock()
		eb.SignalDeliveryContinuations()
		return handle, nil
	}
}

func (eb *EventBus) detachAgentSubscriberLocked(identity agentidentity.Identity) *agentRouteHandle {
	identity = identity.Normalize()
	var detached *agentRouteHandle
	if route := eb.agentRouteHandles[identity]; route != nil {
		route.deactivate()
		detached = route
	}
	key, _ := agentSubscriptionKey(identity)
	delete(eb.agentChans, identity)
	delete(eb.agentRouteHandles, identity)
	delete(eb.subscriptions, key)
	for et := range eb.channels {
		delete(eb.channels[et], key)
		if len(eb.channels[et]) == 0 {
			delete(eb.channels, et)
		}
	}
	return detached
}

func (eb *EventBus) detachInternalSubscriberLocked(subscriberID string) *internalSubscriptionHandle {
	subscriberID = strings.TrimSpace(subscriberID)
	var internal *internalSubscriptionHandle
	if handle := eb.internalHandles[subscriberID]; handle != nil {
		handle.deactivate()
		internal = handle
	}
	key, _ := internalSubscriptionKey(subscriberID)
	delete(eb.internalHandles, subscriberID)
	delete(eb.subscriptions, key)
	for et := range eb.channels {
		delete(eb.channels[et], key)
		if len(eb.channels[et]) == 0 {
			delete(eb.channels, et)
		}
	}
	eb.notifyInternalSubscriptionChangedLocked()
	return internal
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
		_ = eb.detachInternalSubscriberLocked(handle.subscriberID)
	}
	eb.mu.Unlock()
	if !natural {
		return nil
	}
	if err := handle.retireAndWait(cleanupCtx, eb.store); err != nil {
		eb.retainRetiringInternalHandle(handle)
		return err
	}
	eb.SignalDeliveryContinuations()
	return nil
}

func (eb *EventBus) notifyInternalSubscriptionChanged() {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	eb.notifyInternalSubscriptionChangedLocked()
	eb.mu.Unlock()
	eb.SignalDeliveryContinuations()
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
