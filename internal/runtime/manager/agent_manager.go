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
	promptResolver                  runtimecontracts.PromptResolver
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

var (
	ErrAgentAlreadyExists = errors.New("agent already exists")
	ErrAgentNotFound      = errors.New("agent not found")
)

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
		promptResolver:                  opts.PromptResolver,
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

func (am *AgentManager) SpawnAgent(cfg models.AgentConfig) error {
	var err error
	cfg, err = bindRuntimeCreatedIdentity(cfg, "manager.spawn_agent")
	if err != nil {
		return err
	}
	cfg.NormalizeEntityID()
	rec := PersistedAgent{
		Config:  cfg,
		Status:  "active",
		HiredBy: "runtime",
	}
	return am.spawnAgentInternal(am.runtimeContext(), rec, true)
}

func (am *AgentManager) SpawnAgentForEntity(entityID string, cfg models.AgentConfig) error {
	if strings.TrimSpace(cfg.EntityID) == "" {
		cfg.EntityID = strings.TrimSpace(entityID)
	}
	cfg.NormalizeEntityID()
	return am.SpawnAgent(cfg)
}

// RegisterEphemeralAgentForExecution constructs an in-memory agent with the
// normal runtime construction path without persisting it as current-run truth.
func (am *AgentManager) RegisterEphemeralAgentForExecution(ctx context.Context, rec PersistedAgent) error {
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

// SpawnEphemeralClone creates a task-scoped clone of a base agent. Ephemeral
// clones are persisted with status=ephemeral so crash recovery does not hydrate
// them as permanent agents.
func (am *AgentManager) SpawnEphemeralClone(baseIdentity runtimeagentidentity.Identity, cloneAgentID string) error {
	baseIdentity = baseIdentity.Normalize()
	cloneAgentID = strings.TrimSpace(cloneAgentID)
	if err := baseIdentity.Validate(); err != nil {
		return fmt.Errorf("base agent identity: %w", err)
	}
	if cloneAgentID == "" {
		return errors.New("cloneAgentID is required")
	}
	baseExecution, ok := am.lifecycle.executionSnapshotByIdentity(baseIdentity)
	if !ok {
		return fmt.Errorf("%w: %s", ErrAgentNotFound, baseIdentity.Description())
	}
	baseCfg := baseExecution.Config
	cloneCfg := baseCfg
	cloneCfg.ID = cloneAgentID
	cloneCfg.Identity = runtimeagentidentity.Identity{}
	var err error
	cloneCfg, err = bindRuntimeCreatedIdentity(cloneCfg, "manager.ephemeral_clone")
	if err != nil {
		return err
	}
	if strings.TrimSpace(cloneCfg.ParentAgent) == "" {
		cloneCfg.ParentAgent = baseIdentity.AgentID()
	}
	rec := PersistedAgent{
		Config:        cloneCfg,
		ParentAgentID: baseIdentity.AgentID(),
		Status:        "ephemeral",
		HiredBy:       "shard-dispatcher",
		StartedAt:     time.Now().UTC(),
	}
	if err := am.spawnAgentInternal(am.runtimeContext(), rec, true); err != nil {
		if errors.Is(err, ErrAgentAlreadyExists) {
			return nil
		}
		return err
	}
	return nil
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
	return am.spawnAgentInternalForSourceWithTopology(ctx, rec, persist, source, nil)
}

func (am *AgentManager) spawnAgentInternalForSourceWithTopology(
	ctx context.Context,
	rec PersistedAgent,
	persist bool,
	source semanticview.Source,
	topology *DynamicAgentTopologyMutation,
) error {
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
	if err := am.lifecycle.registerExecutionWithTopology(ctx, rec, persist, a, subscriptionAdmission, topology); err != nil {
		return err
	}
	if persist && am.lifecycle.store == nil && am.store != nil {
		if err := am.store.UpsertAgent(ctx, rec); err != nil {
			am.lifecycle.unregisterIdentity(identity)
			return fmt.Errorf("persist agent %s: %w", rec.Config.ID, err)
		}
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
	var err error
	cfg, err = am.applyContractPrompt(cfg)
	if err != nil {
		return nil, err
	}
	if am.factory != nil {
		return am.factory(cfg)
	}
	return newGenericAgent(cfg), nil
}

func (am *AgentManager) applyContractPrompt(cfg models.AgentConfig) (models.AgentConfig, error) {
	if am.promptResolver == nil {
		return cfg, nil
	}
	prompt, found, err := am.promptResolver.LoadPromptForAgent(cfg, "")
	if err != nil {
		return cfg, fmt.Errorf(
			"contract prompt load failed agent_id=%s role=%s: %w",
			strings.TrimSpace(cfg.ID),
			strings.TrimSpace(cfg.Role),
			err,
		)
	}
	if !found {
		return cfg, nil
	}
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return cfg, nil
	}
	updated, err := withSystemPrompt(cfg.Config, prompt)
	if err != nil {
		return cfg, err
	}
	cfg.Config = updated
	return cfg, nil
}

func (am *AgentManager) ReconfigureAgentTarget(
	agentID, flowInstance string,
	cfg models.AgentConfig,
	expected *models.AgentConfig,
) (models.AgentTargetMutationResult, error) {
	identity, err := am.lifecycle.resolveAgentTarget(agentID, flowInstance, false)
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	result, err := am.replaceExecutionIdentityConfigWithTopology(
		am.runtimeContext(),
		identity,
		"reconfigure",
		"",
		&cfg,
		am.semanticSource,
		false,
		nil,
		expected,
	)
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	if result.transitioned && am.lifecycle.store == nil && am.store != nil {
		rec := PersistedAgent{Config: result.config, Status: "active", HiredBy: "reconfigure"}
		if err := am.store.UpsertAgent(am.runtimeContext(), rec); err != nil {
			return models.AgentTargetMutationResult{}, fmt.Errorf("persist reconfigured agent %s: %w", identity.Description(), err)
		}
	}
	return models.AgentTargetMutationResult{
		PreviousConfig: result.previous,
		CurrentConfig:  result.config,
		Transitioned:   result.transitioned,
	}, nil
}

func (am *AgentManager) reconfigureAgentIdentityExactWithTopology(
	ctx context.Context,
	source semanticview.Source,
	identity runtimeagentidentity.Identity,
	cfg models.AgentConfig,
	topology *DynamicAgentTopologyMutation,
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
	if result.transitioned && am.lifecycle.store == nil && am.store != nil {
		rec := PersistedAgent{Config: result.config, Status: "active", HiredBy: "reconfigure"}
		if err := am.store.UpsertAgent(ctx, rec); err != nil {
			return fmt.Errorf("persist reconfigured agent %s: %w", identity.Description(), err)
		}
	}
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
	topology *DynamicAgentTopologyMutation,
) error {
	_, err := am.lifecycle.terminateIdentityWithTopology(ctx, identity, trigger, AgentLifecycleTerminated, topology)
	if err != nil {
		return err
	}
	_ = am.projectLifecycleDiagnostics(context.WithoutCancel(ctx))
	return nil
}

func (am *AgentManager) TeardownAgentTarget(
	agentID, flowInstance string,
	expected *models.AgentConfig,
) (models.AgentTargetMutationResult, error) {
	identity, err := am.lifecycle.resolveAgentTarget(agentID, flowInstance, false)
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	previous, err := am.lifecycle.terminateIdentityWithTopologyExpected(
		am.runtimeContext(),
		identity,
		"teardown",
		AgentLifecycleTerminated,
		nil,
		expected,
		false,
	)
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	_ = am.projectLifecycleDiagnostics(context.Background())
	return models.AgentTargetMutationResult{PreviousConfig: previous, Transitioned: true}, nil
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
