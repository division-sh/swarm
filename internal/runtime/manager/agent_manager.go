package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type AgentManager struct {
	mu                              sync.RWMutex
	workspaces                      workspace.Lifecycle
	bus                             Bus
	factory                         AgentFactory
	store                           ManagerPersistence
	deliveryStore                   runtimedelivery.Store
	testLifecycleProbe              runtimelifecycleprobe.Observer
	sessions                        sessions.Registry
	sessionResetter                 sessions.Resetter
	semanticSource                  semanticview.Source
	semanticReadinessSource         dynamicFlowRuntimeReadinessSource
	budget                          BudgetGuard
	resetRuntimeOwnedState          func()
	runtimeShutdownAdmissionClosed  func() bool
	runtimeIngressSafetyPause       func(context.Context, string, *runtimefailures.Envelope) error
	nativeToolAdmissionValidator    func(context.Context, models.AgentConfig) error
	runtimeMode                     string
	llmBackend                      string
	modelAliases                    llmselection.ModelAliases
	requireModelResolution          bool
	executionPosture                executionposture.Posture
	throttleSuppressPrefixes        []string
	workflowInstances               flowInstancePersistence
	workOwner                       worklifetime.Occurrence
	receiverExecution               eventreceiver.ExecutionVariant
	selectedContractRouteRecoveries map[string]SelectedContractRouteRecoveryTruth
	directiveHeartbeat              directiveHeartbeatConfig
	lifecycle                       *agentLifecycleCoordinator
	roles                           PersistenceRoles
	baseContext                     context.Context
	staticTopology                  runtimeagenttopology.Admission
	staticSourceSet                 runtimeagenttopology.SourceSetPlan
	startupAgentsHydrated           bool
	startupEffectsMu                sync.Mutex
	startupEffectsReconciled        bool

	runMu              sync.Mutex
	authBreakerTripped bool

	poisonMu            sync.Mutex
	poisonPanicCounts   map[poisonPanicKey]int
	poisonEventEntities map[string]map[string]struct{}
	poisonEventEmitted  map[string]bool

	deadLetterMu         sync.Mutex
	deadLetterWindows    map[string][]deadLetterEscalationSample
	deadLetterLastRaised map[string]time.Time

	deliveryLaneMu sync.Mutex
	deliveryLanes  map[runtimeagentidentity.Identity]*claimedAttemptLane

	dynamicFlowReadinessMu       sync.Mutex
	dynamicFlowReadinessAttempts map[dynamicFlowRuntimeReadinessKey]*dynamicFlowRuntimeReadinessAttempt
	dynamicFlowReadinessSignal   chan struct{}

	testAfterDynamicFlowReadinessAdmission func()
}

// InstallStartupTopology binds the selected-store generation writer and the
// exact declaration plan before startup recovery can read or mutate agents.
func (am *AgentManager) InstallStartupTopology(store AgentLifecyclePersistence, admission runtimeagenttopology.Admission, plan runtimeagenttopology.SourceSetPlan) error {
	if am == nil || store == nil {
		return errors.New("startup agent lifecycle persistence is required")
	}
	if err := admission.Validate(); err != nil {
		return fmt.Errorf("startup static topology admission: %w", err)
	}
	if admission.Authority.Kind != runtimeagenttopology.AuthorityStaticDeclarationPlan || admission.Lifetime != runtimeagenttopology.LifetimeDurableManaged {
		return errors.New("startup agent topology requires durable static declaration authority")
	}
	if err := plan.Validate(); err != nil {
		return fmt.Errorf("startup source-set plan: %w", err)
	}
	if admission.Authority.Static.SourceSetRevision != plan.Revision {
		return errors.New("startup static topology admission differs from complete source-set plan")
	}
	if err := am.lifecycle.installPersistence(store); err != nil {
		return err
	}
	am.startupEffectsMu.Lock()
	am.startupEffectsReconciled = false
	am.startupEffectsMu.Unlock()
	am.mu.Lock()
	am.staticTopology = admission
	am.staticSourceSet = plan
	am.mu.Unlock()
	return nil
}

func (am *AgentManager) completeStaticSourceSet() (runtimeagenttopology.SourceSetPlan, error) {
	am.mu.RLock()
	plan := am.staticSourceSet
	am.mu.RUnlock()
	if err := plan.Validate(); err != nil {
		return runtimeagenttopology.SourceSetPlan{}, fmt.Errorf("complete static source set is not installed: %w", err)
	}
	return plan, nil
}

func (am *AgentManager) staticTopologyAdmission() (runtimeagenttopology.Admission, error) {
	if am == nil {
		return runtimeagenttopology.Admission{}, errors.New("agent manager is required")
	}
	am.mu.RLock()
	admission := am.staticTopology
	am.mu.RUnlock()
	if err := admission.Validate(); err != nil {
		return runtimeagenttopology.Admission{}, fmt.Errorf("static declaration topology is not installed: %w", err)
	}
	return admission, nil
}

var (
	ErrAgentAlreadyExists = errors.New("agent already exists")
	ErrAgentNotFound      = errors.New("agent not found")
)

type lifecycleOnlyAgent struct {
	id        string
	agentType string
}

func (a lifecycleOnlyAgent) ID() string                      { return a.id }
func (a lifecycleOnlyAgent) Type() string                    { return a.agentType }
func (lifecycleOnlyAgent) Subscriptions() []events.EventType { return nil }
func (lifecycleOnlyAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, errors.New("lifecycle-only agent cannot execute events")
}

const (
	poisonPanicQuarantineAt       = 3
	poisonEventEntityThreshold    = 3
	deadLetterEscalationThreshold = 3
	deadLetterEscalationWindow    = 10 * time.Minute
	runtimeSpecVersion            = "v2.2.1"
)

type deadLetterEscalationSample struct {
	at         time.Time
	eventID    string
	agentID    string
	entityID   string
	retryCount int
	failure    *runtimefailures.Envelope
}

func normalizeManagerLLMBackend(raw string) string {
	profile, err := llmselection.ResolvePersistedBackend(raw)
	if err != nil {
		return strings.TrimSpace(raw)
	}
	return profile.ID
}

func NewAgentManager(bus Bus, factory AgentFactory, stores ...ManagerPersistence) *AgentManager {
	return NewAgentManagerWithOptions(bus, factory, AgentManagerOptions{
		ReceiverExecution: eventreceiver.NormalExecution(),
	}, stores...)
}

func NewAgentManagerWithOptions(bus Bus, factory AgentFactory, opts AgentManagerOptions, stores ...ManagerPersistence) *AgentManager {
	if err := opts.ReceiverExecution.Validate(); err != nil {
		panic(fmt.Sprintf("agent manager receiver execution: %v", err))
	}
	var store ManagerPersistence
	if len(stores) > 0 {
		store = stores[0]
	}
	throttleSuppressPrefixes := make([]string, 0, len(opts.ThrottleSuppressPrefixes))
	for _, prefix := range opts.ThrottleSuppressPrefixes {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" {
			throttleSuppressPrefixes = append(throttleSuppressPrefixes, prefix)
		}
	}
	lifecycle := newAgentLifecycleCoordinator(
		opts.LifecycleStore,
		opts.SessionLifecycle,
		opts.PersistenceRoles.AgentRoutes,
		opts.PersistenceRoles.LifecycleState,
		opts.PersistenceRoles.LifecycleEffects,
	)
	lifecycle.baseContext = opts.BaseContext
	lifecycle.executionPosture = opts.ExecutionPosture
	if opts.WorkOwner != nil {
		_ = lifecycle.prepareRunOwner(opts.BaseContext, opts.WorkOwner)
	}
	return &AgentManager{
		bus:                bus,
		factory:            factory,
		store:              store,
		deliveryStore:      opts.DeliveryStore,
		testLifecycleProbe: opts.TestLifecycleProbe,
		workspaces:         opts.Workspaces,
		sessions:           opts.Sessions,
		sessionResetter:    opts.SessionResetter,
		semanticSource:     opts.SemanticSource,
		semanticReadinessSource: dynamicFlowRuntimeReadinessSource{
			fact: opts.BundleSourceFact, source: opts.SemanticSource,
		},
		workflowInstances:               opts.WorkflowInstances,
		workOwner:                       opts.WorkOwner,
		receiverExecution:               opts.ReceiverExecution,
		selectedContractRouteRecoveries: map[string]SelectedContractRouteRecoveryTruth{},
		directiveHeartbeat:              defaultDirectiveHeartbeatConfig(),
		runtimeMode:                     strings.TrimSpace(opts.RuntimeMode),
		budget:                          opts.Budget,
		resetRuntimeOwnedState:          opts.ResetRuntimeOwnedState,
		runtimeShutdownAdmissionClosed:  opts.RuntimeShutdownAdmissionClosed,
		runtimeIngressSafetyPause:       opts.RuntimeIngressSafetyPause,
		nativeToolAdmissionValidator:    opts.NativeToolAdmissionValidator,
		throttleSuppressPrefixes:        throttleSuppressPrefixes,
		llmBackend:                      normalizeManagerLLMBackend(opts.LLMBackend),
		modelAliases:                    llmselection.EffectiveModelAliases(opts.ModelAliases),
		requireModelResolution:          opts.RequireModelResolution,
		lifecycle:                       lifecycle,
		roles:                           opts.PersistenceRoles,
		baseContext:                     opts.BaseContext,
		executionPosture:                opts.ExecutionPosture,
		poisonPanicCounts:               make(map[poisonPanicKey]int),
		poisonEventEntities:             make(map[string]map[string]struct{}),
		poisonEventEmitted:              make(map[string]bool),
		deadLetterWindows:               make(map[string][]deadLetterEscalationSample),
		deadLetterLastRaised:            make(map[string]time.Time),
		deliveryLanes:                   make(map[runtimeagentidentity.Identity]*claimedAttemptLane),
		dynamicFlowReadinessAttempts:    make(map[dynamicFlowRuntimeReadinessKey]*dynamicFlowRuntimeReadinessAttempt),
		dynamicFlowReadinessSignal:      make(chan struct{}, 1),
	}
}

// SetReceiverExecution replaces the selected-fork receiver owner before the
// manager starts. Selected provider preflight finalizes admission surfaces
// after manager materialization but before any delivery loop is live.
func (am *AgentManager) SetReceiverExecution(variant eventreceiver.ExecutionVariant) error {
	if am == nil {
		return errors.New("agent manager is required")
	}
	if err := variant.Validate(); err != nil {
		return fmt.Errorf("agent manager receiver execution: %w", err)
	}
	if _, _, running := am.lifecycle.runSnapshot(); running {
		return errors.New("agent manager receiver execution cannot change after start")
	}
	am.mu.Lock()
	am.receiverExecution = variant
	am.mu.Unlock()
	return nil
}

func (am *AgentManager) runtimeContext() context.Context {
	if am == nil {
		return context.Background()
	}
	ctx, _, _ := am.lifecycle.runSnapshot()
	if ctx != nil && ctx.Err() == nil {
		return ctx
	}
	if am.baseContext != nil {
		return am.baseContext
	}
	return context.Background()
}

func (am *AgentManager) bindRuntimeOperationContext(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	base := am.runtimeContext()
	ownerScope, ownerOK := runtimeauthoractivity.ScopeFromContext(base)
	currentScope, currentOK := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ownerOK {
		return ctx, nil
	}
	if currentOK {
		switch currentScope.Kind {
		case runtimeauthoractivity.ScopeBundle:
			if currentScope != ownerScope {
				return nil, fmt.Errorf("manager runtime scope conflicts with selected operation scope")
			}
		case runtimeauthoractivity.ScopeRuntime:
			if currentScope.RuntimeInstanceID != ownerScope.RuntimeInstanceID {
				return nil, fmt.Errorf("manager runtime instance conflicts with selected operation scope")
			}
		default:
			return nil, fmt.Errorf("manager operation cannot bind bundle semantics over %q scope", currentScope.Kind)
		}
	}
	ctx = runtimeauthoractivity.WithScope(ctx, ownerScope)
	if runtimeID, ok := runtimecorrelation.RuntimeInstanceIDFromContext(base); ok {
		ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeID)
	} else if ownerScope.RuntimeInstanceID != "" {
		ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, ownerScope.RuntimeInstanceID)
	}
	if fact, ok := runtimecorrelation.BundleSourceFactFromContext(base); ok {
		if current, currentOK := runtimecorrelation.BundleSourceFactFromContext(ctx); currentOK && !current.Matches(fact) {
			return nil, fmt.Errorf("manager bundle source fact conflicts with selected operation scope")
		}
		ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	}
	return ctx, nil
}

func (am *AgentManager) runtimePlatformControlEventContext(ctx context.Context) context.Context {
	if ctx == nil {
		return am.runtimeContext()
	}
	if ctx.Err() != nil {
		return context.WithoutCancel(ctx)
	}
	return ctx
}

func (am *AgentManager) WaitForQuiescence(ctx context.Context) error {
	if am == nil || am.lifecycle == nil {
		return nil
	}
	return am.lifecycle.waitForWork(ctx)
}

func (am *AgentManager) PublishEvent(ctx context.Context, evt events.Event) error {
	if am == nil || am.bus == nil {
		return errors.New("event bus is not configured")
	}
	return am.bus.Publish(ctx, evt)
}

// MaterializeAdmittedAgentForExecution constructs process-local execution from
// an already sealed topology admission without writing durable topology truth.
func (am *AgentManager) MaterializeAdmittedAgentForExecution(ctx context.Context, rec PersistedAgent) error {
	if am == nil {
		return errors.New("agent manager is required")
	}
	var err error
	rec.Config, err = bindRuntimeCreatedIdentity(rec.Config, "manager.ephemeral_execution")
	if err != nil {
		return err
	}
	return am.spawnAgentInternal(ctx, rec, false)
}

func (am *AgentManager) spawnAgentInternal(ctx context.Context, rec PersistedAgent, persist bool) error {
	return am.spawnAgentInternalForSource(ctx, rec, persist, am.semanticSource)
}

func (am *AgentManager) spawnAgentInternalForSource(
	ctx context.Context,
	rec PersistedAgent,
	persist bool,
	source semanticview.Source,
) error {
	return am.spawnAgentInternalForSourceWithTopology(ctx, rec, persist, source, &rec.Topology)
}

func (am *AgentManager) spawnAgentInternalForSourceWithTopology(
	ctx context.Context,
	rec PersistedAgent,
	persist bool,
	source semanticview.Source,
	topology *runtimeagenttopology.Admission,
) error {
	if topology == nil {
		return errors.New("agent lifecycle topology admission is required")
	}
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("agent lifecycle topology admission: %w", err)
	}
	rec.Topology = *topology
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return fmt.Errorf("agent %q requires a concrete identity: %w", strings.TrimSpace(rec.Config.ID), err)
	}
	if err := am.resolveAgentModel(&rec.Config); err != nil {
		return err
	}
	if err := am.executionPosture.Admit(rec.Config.ExecutionMode, "agent lifecycle reconstruction"); err != nil {
		return err
	}
	if err := bindCanonicalAgentPrompt(source, &rec.Config); err != nil {
		return err
	}
	subscriptionAdmission, err := admitAgentConfigSubscriptions(source, &rec.Config, nil)
	if err != nil {
		return err
	}
	if err := am.validateNativeToolAdmission(ctx, rec.Config); err != nil {
		return err
	}
	if err := agentmemory.ValidateFlowOwnership(rec.Config.Memory, rec.Config.CanonicalFlowPath()); err != nil {
		return fmt.Errorf("invalid agent memory plan: %w", err)
	}
	a, err := am.buildAgent(rec.Config)
	if err != nil {
		return err
	}

	if _, exists := am.lifecycle.executionSnapshotByIdentity(identity); exists {
		return fmt.Errorf("%w: %s", ErrAgentAlreadyExists, a.ID())
	}
	if err := am.lifecycle.registerExecutionWithTopology(ctx, rec, persist, a, subscriptionAdmission, *topology); err != nil {
		return err
	}

	_ = am.projectLifecycleDiagnostics(context.WithoutCancel(ctx))

	runCtx, _, isRunning := am.lifecycle.runSnapshot()
	_ = persist
	if isRunning {
		if _, err := am.replaceExecutionIdentityConfigWithTopology(runCtx, identity, "start", "", nil, am.semanticSource, false, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

func bindRuntimeCreatedIdentity(cfg models.AgentConfig, owner string) (models.AgentConfig, error) {
	cfg.NormalizeRuntimeDescriptor()
	if !cfg.Identity.IsZero() {
		if _, err := cfg.ConcreteIdentity(); err != nil {
			return models.AgentConfig{}, err
		}
		return cfg, nil
	}
	name, err := runtimeagentidentity.RuntimeName(cfg.ID, owner)
	if err != nil {
		return models.AgentConfig{}, err
	}
	route := runtimeagentidentity.RootRoute()
	if flowPath := cfg.CanonicalFlowPath(); flowPath != "" {
		route, err = runtimeflowidentity.StoredRoute("", "", flowPath).AgentIdentityRoute()
		if err != nil {
			return models.AgentConfig{}, err
		}
	}
	cfg.Identity, err = runtimeagentidentity.New(name, route)
	if err != nil {
		return models.AgentConfig{}, err
	}
	return cfg, nil
}

func (am *AgentManager) publishCommittedAgent(ctx context.Context, rec PersistedAgent, a Agent, subscriptionAdmission semanticview.FlowOwnedAgentSubscriptionAdmission, result AgentLifecycleTransitionResult) error {
	rec.LifecycleEpoch = result.RuntimeEpoch
	rec.LifecycleGeneration = result.Generation
	rec.LifecyclePhase = result.Phase
	rec.LifecycleRunMode = result.RunMode
	if err := am.lifecycle.registerExecution(ctx, rec, false, a, subscriptionAdmission); err != nil {
		return err
	}
	_ = am.projectLifecycleDiagnostics(ctx)
	runCtx, _, isRunning := am.lifecycle.runSnapshot()
	if isRunning {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		if _, err := am.replaceExecutionIdentityConfigWithTopology(runCtx, identity, "start", "", nil, am.semanticSource, false, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

func (am *AgentManager) adoptPersistedAgentForLifecycle(
	ctx context.Context,
	source semanticview.Source,
	rec PersistedAgent,
) error {
	if err := am.resolveAgentModel(&rec.Config); err != nil {
		return err
	}
	if err := bindCanonicalAgentPrompt(source, &rec.Config); err != nil {
		return err
	}
	subscriptionAdmission, err := admitAgentConfigSubscriptions(source, &rec.Config, nil)
	if err != nil {
		return err
	}
	if err := am.validateNativeToolAdmission(ctx, rec.Config); err != nil {
		return err
	}
	agent, err := am.buildAgent(rec.Config)
	if err != nil {
		return err
	}
	return am.lifecycle.registerExecution(ctx, rec, false, agent, subscriptionAdmission)
}

func (am *AgentManager) adoptPersistedAgentLifecycleOnly(ctx context.Context, rec PersistedAgent) error {
	if err := rec.Config.ValidateIntentInputs(); err != nil {
		return err
	}
	// A removed or replaced declaration has no executable prompt owner in the
	// successor source. Register only its durable lifecycle identity so teardown
	// or replacement can fence it; never construct a provider agent or install
	// executable subscriptions.
	rec.Config.Subscriptions = nil
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID:  rec.Config.ID,
		FlowID:   rec.Config.FlowID,
		FlowPath: rec.Config.CanonicalFlowPath(),
	})
	if err != nil {
		return err
	}
	sentinel := lifecycleOnlyAgent{
		id:        strings.TrimSpace(rec.Config.ID),
		agentType: strings.TrimSpace(rec.Config.Type),
	}
	return am.lifecycle.registerExecution(ctx, rec, false, sentinel, admission)
}

func (am *AgentManager) resolveAgentModel(cfg *models.AgentConfig) error {
	if cfg == nil {
		return fmt.Errorf("agent config is required")
	}
	cfg.NormalizeRuntimeDescriptor()
	configuredProfile, err := llmselection.ResolveActiveBackend(am.llmBackend)
	if err != nil {
		return fmt.Errorf("agent %s invalid configured llm backend %q: %w", strings.TrimSpace(cfg.ID), strings.TrimSpace(am.llmBackend), err)
	}
	resolved, err := runtimellm.ResolveAgentExecution(configuredProfile, am.modelAliases, *cfg)
	if err != nil {
		return fmt.Errorf("agent %s execution selection failed: %w", strings.TrimSpace(cfg.ID), err)
	}
	*cfg = resolved.Actor
	if strings.TrimSpace(cfg.Model) == "" {
		if am.requireModelResolution {
			return fmt.Errorf("agent %s missing model", strings.TrimSpace(cfg.ID))
		}
		return nil
	}
	return nil
}

func (am *AgentManager) validateNativeToolAdmission(ctx context.Context, cfg models.AgentConfig) error {
	if am == nil || am.nativeToolAdmissionValidator == nil || !cfg.NativeTools.Any() {
		return nil
	}
	if cfg.ResolvedLLMBackend == llmselection.BackendMock {
		return nil
	}
	if ctx == nil {
		ctx = am.runtimeContext()
	}
	if err := am.nativeToolAdmissionValidator(ctx, cfg); err != nil {
		return fmt.Errorf("native tool admission failed: %w", err)
	}
	return nil
}

func (am *AgentManager) buildAgent(cfg models.AgentConfig) (Agent, error) {
	if err := models.ValidateNoAuthoredSystemPrompt(cfg.Config); err != nil {
		return nil, err
	}
	if err := cfg.ValidateIntentCarrier(); err != nil {
		return nil, err
	}
	if am.factory != nil {
		return am.factory(cfg)
	}
	return newGenericAgent(cfg), nil
}

func bindCanonicalAgentPrompt(source semanticview.Source, cfg *models.AgentConfig) error {
	if cfg == nil {
		return fmt.Errorf("agent config is required")
	}
	if err := cfg.ValidateIntentInputs(); err != nil {
		return err
	}
	if !cfg.Prompt.Empty() {
		return cfg.ValidateIntentCarrier()
	}
	if source == nil {
		return fmt.Errorf("agent %s cannot reconstruct its derived prompt without a semantic source", strings.TrimSpace(cfg.ID))
	}
	var matched *semanticview.AgentDeclaration
	for _, declaration := range semanticview.AgentDeclarations(source) {
		if declaration.Entry.ResolvedIntent != cfg.Intent {
			continue
		}
		if matched != nil {
			return fmt.Errorf("agent %s resolved intent matches multiple declarations", strings.TrimSpace(cfg.ID))
		}
		candidate := declaration
		matched = &candidate
	}
	if matched == nil {
		return fmt.Errorf("agent %s resolved intent does not match a canonical declaration", strings.TrimSpace(cfg.ID))
	}
	bundle, _ := semanticview.Bundle(source)
	prompt, err := runtimecontracts.AssembleAgentPrompt(bundle, matched.OwnerFlowID, matched.Entry, cfg.Criteria)
	if err != nil {
		return fmt.Errorf("agent %s reconstruct derived prompt: %w", strings.TrimSpace(cfg.ID), err)
	}
	cfg.Prompt = prompt
	return cfg.ValidateIntentCarrier()
}

func (am *AgentManager) reconfigureAgentIdentityExactWithTopology(
	ctx context.Context,
	source semanticview.Source,
	identity runtimeagentidentity.Identity,
	cfg models.AgentConfig,
	topology *runtimeagenttopology.Admission,
) error {
	result, err := am.replaceExecutionIdentityConfigWithTopology(
		ctx,
		identity,
		"reconfigure",
		"",
		&cfg,
		source,
		true,
		topology,
		nil,
	)
	if err != nil {
		return err
	}
	_ = result
	return nil
}

func (am *AgentManager) teardownIdentity(
	ctx context.Context,
	identity runtimeagentidentity.Identity,
	trigger string,
) error {
	return am.teardownIdentityWithTopology(ctx, identity, trigger, nil)
}

func (am *AgentManager) teardownIdentityAfterTerminalEvent(
	ctx context.Context,
	identity runtimeagentidentity.Identity,
	trigger string,
	deferRouteRetirement bool,
) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if _, err := am.lifecycle.terminateIdentityWithTopologyExpected(
		ctx,
		identity,
		trigger,
		AgentLifecycleTerminated,
		nil,
		nil,
		deferRouteRetirement,
	); err != nil {
		return err
	}
	_ = am.projectLifecycleDiagnostics(context.WithoutCancel(ctx))
	return nil
}

func (am *AgentManager) teardownIdentityWithTopology(
	ctx context.Context,
	identity runtimeagentidentity.Identity,
	trigger string,
	topology *runtimeagenttopology.Admission,
) error {
	_, err := am.lifecycle.terminateIdentityWithTopology(ctx, identity, trigger, AgentLifecycleTerminated, topology)
	if err != nil {
		return err
	}
	_ = am.projectLifecycleDiagnostics(context.WithoutCancel(ctx))
	return nil
}

func reconfigureSessionMutationPlan(current, updated models.AgentConfig) sessions.LifecycleMutationPlan {
	if !current.Memory.Enabled {
		return sessions.LifecycleMutationPlan{Action: sessions.LifecycleMutationNone}
	}
	plan := sessions.LifecycleMutationPlan{
		Action:            sessions.LifecycleMutationTerminateCurrentSet,
		TerminationReason: sessions.TerminationReasonNormal,
		TerminationDetail: "agent_reconfigured_identity_changed",
	}
	if !updated.Memory.Enabled {
		return plan
	}
	return sessions.LifecycleMutationPlan{
		Action:            sessions.LifecycleMutationRotateCurrentSet,
		TerminationReason: sessions.TerminationReasonNormal,
		TerminationDetail: "agent_reconfigured",
		CheckpointSummary: "agent reconfigured",
	}
}
