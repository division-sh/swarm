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
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
	sessions                        sessions.Registry
	semanticSource                  semanticview.Source
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
	throttleSuppressPrefixes        []string
	inFlightMu                      sync.Mutex
	inFlight                        map[string]struct{}
	workflowInstances               flowInstancePersistence
	selectedContractRouteRecoveries map[string]SelectedContractRouteRecoveryTruth
	directiveHeartbeat              directiveHeartbeatConfig
	lifecycle                       *agentLifecycleCoordinator
	baseContext                     context.Context

	runMu              sync.Mutex
	authBreakerTripped bool
	runWG              sync.WaitGroup

	poisonMu            sync.Mutex
	poisonPanicCounts   map[string]int
	poisonEventEntities map[string]map[string]struct{}
	poisonEventEmitted  map[string]bool

	deadLetterMu         sync.Mutex
	deadLetterWindows    map[string][]deadLetterEscalationSample
	deadLetterLastRaised map[string]time.Time
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
	receiptWriteTimeout           = 3 * time.Second
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
	return NewAgentManagerWithOptions(bus, factory, AgentManagerOptions{}, stores...)
}

func NewAgentManagerWithOptions(bus Bus, factory AgentFactory, opts AgentManagerOptions, stores ...ManagerPersistence) *AgentManager {
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
	lifecycle := newAgentLifecycleCoordinator(opts.LifecycleStore, opts.Sessions)
	lifecycle.baseContext = opts.BaseContext
	lifecycle.bindRoutes(bus)
	return &AgentManager{
		bus:                             bus,
		factory:                         factory,
		store:                           store,
		workspaces:                      opts.Workspaces,
		sessions:                        opts.Sessions,
		semanticSource:                  opts.SemanticSource,
		promptResolver:                  opts.PromptResolver,
		workflowInstances:               opts.WorkflowInstances,
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
		inFlight:                        make(map[string]struct{}),
		lifecycle:                       lifecycle,
		baseContext:                     opts.BaseContext,
		poisonPanicCounts:               make(map[string]int),
		poisonEventEntities:             make(map[string]map[string]struct{}),
		poisonEventEmitted:              make(map[string]bool),
		deadLetterWindows:               make(map[string][]deadLetterEscalationSample),
		deadLetterLastRaised:            make(map[string]time.Time),
	}
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
		if current, currentOK := runtimecorrelation.BundleSourceFactFromContext(ctx); currentOK && current.BundleHash != "" && current.BundleHash != fact.BundleHash {
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

func (am *AgentManager) InFlightCount() int {
	if am == nil {
		return 0
	}
	am.inFlightMu.Lock()
	defer am.inFlightMu.Unlock()
	return len(am.inFlight)
}

func (am *AgentManager) WaitForQuiescence(ctx context.Context) error {
	if am == nil {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if am.InFlightCount() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (am *AgentManager) PublishEvent(ctx context.Context, evt events.Event) error {
	if am == nil || am.bus == nil {
		return errors.New("event bus is not configured")
	}
	return am.bus.Publish(ctx, evt)
}

func (am *AgentManager) SpawnAgent(cfg models.AgentConfig) error {
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
	return am.spawnAgentInternal(ctx, rec, false)
}

// SpawnEphemeralClone creates a task-scoped clone of a base agent. Ephemeral
// clones are persisted with status=ephemeral so crash recovery does not hydrate
// them as permanent agents.
func (am *AgentManager) SpawnEphemeralClone(baseAgentID, cloneAgentID string) error {
	baseAgentID = strings.TrimSpace(baseAgentID)
	cloneAgentID = strings.TrimSpace(cloneAgentID)
	if baseAgentID == "" {
		return errors.New("baseAgentID is required")
	}
	if cloneAgentID == "" {
		return errors.New("cloneAgentID is required")
	}
	baseExecution, ok := am.lifecycle.executionSnapshot(baseAgentID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrAgentNotFound, baseAgentID)
	}
	baseCfg := baseExecution.Config
	cloneCfg := baseCfg
	cloneCfg.ID = cloneAgentID
	if strings.TrimSpace(cloneCfg.ParentAgent) == "" {
		cloneCfg.ParentAgent = baseAgentID
	}
	rec := PersistedAgent{
		Config:        cloneCfg,
		ParentAgentID: baseAgentID,
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
	if strings.TrimSpace(rec.Config.LLMBackend) == "" {
		rec.Config.LLMBackend = am.llmBackend
	}
	if err := am.resolveAgentModel(&rec.Config); err != nil {
		return err
	}
	subscriptionAdmission, err := admitAgentConfigSubscriptions(am.semanticSource, &rec.Config, nil)
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

	if _, exists := am.lifecycle.executionSnapshot(a.ID()); exists {
		return fmt.Errorf("%w: %s", ErrAgentAlreadyExists, a.ID())
	}
	if persist {
		if _, txActive := runtimepipeline.PipelineSQLTxFromContext(ctx); txActive {
			if am.lifecycle == nil || am.lifecycle.store == nil {
				return fmt.Errorf("transactional agent registration requires lifecycle persistence")
			}
			result, err := am.lifecycle.persistRegistration(ctx, rec)
			if err != nil {
				return err
			}
			postCommitCtx := runtimepipeline.WithoutPipelineSQLConnContext(runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(ctx)))
			if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func() {
				if err := am.publishCommittedAgent(postCommitCtx, rec, a, subscriptionAdmission, result); err != nil && am.bus != nil {
					_ = am.bus.LogRuntime(postCommitCtx, runtimepipeline.RuntimeLogEntry{
						Level: "error", Message: "Post-commit agent publication failed",
						Component: "flow_activation", Action: "agent_post_commit_publish_failed",
						Detail: map[string]any{"agent_id": a.ID()}, Failure: failureEnvelope(err, "flow_activation", "publish_agent"),
					})
				}
			}) {
				return fmt.Errorf("transactional agent registration requires post-commit publication owner")
			}
			return nil
		}
	}
	if err := am.lifecycle.registerExecution(ctx, rec, persist, a, subscriptionAdmission); err != nil {
		return err
	}
	if persist && am.lifecycle.store == nil && am.store != nil {
		if err := am.store.UpsertAgent(ctx, rec); err != nil {
			am.lifecycle.unregisterLocal(a.ID())
			return fmt.Errorf("persist agent %s: %w", rec.Config.ID, err)
		}
	}

	_ = am.projectLifecycleDiagnostics(context.WithoutCancel(ctx))

	runCtx, _, isRunning := am.lifecycle.runSnapshot()
	_ = persist
	if isRunning {
		if _, err := am.replaceExecution(runCtx, a.ID(), "start", "", nil); err != nil {
			return err
		}
	}
	return nil
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
		if _, err := am.replaceExecution(runCtx, a.ID(), "start", "", nil); err != nil {
			return err
		}
	}
	return nil
}

func (am *AgentManager) resolveAgentModel(cfg *models.AgentConfig) error {
	if cfg == nil {
		return fmt.Errorf("agent config is required")
	}
	cfg.NormalizeRuntimeDescriptor()
	if strings.TrimSpace(cfg.Model) == "" {
		if am.requireModelResolution {
			return fmt.Errorf("agent %s missing model", strings.TrimSpace(cfg.ID))
		}
		return nil
	}
	profile, err := llmselection.ResolveActiveBackend(cfg.LLMBackend)
	if err != nil {
		return fmt.Errorf("agent %s invalid llm_backend %q: %w", strings.TrimSpace(cfg.ID), strings.TrimSpace(cfg.LLMBackend), err)
	}
	resolved, err := llmselection.ResolveModel(profile, llmselection.ModelResolution{
		Model:  cfg.Model,
		Models: am.modelAliases,
	})
	if err != nil {
		return fmt.Errorf("agent %s model resolution failed: %w", strings.TrimSpace(cfg.ID), err)
	}
	cfg.Model = resolved.ModelAlias
	cfg.LLMBackend = resolved.Backend
	cfg.ResolvedModel = resolved.ConcreteModel
	cfg.ResolvedLLMProvider = resolved.Provider
	cfg.ResolvedLLMTransport = resolved.Transport
	executionMode, err := llmselection.ExecutionModeForProfile(profile)
	if err != nil {
		return fmt.Errorf("agent %s execution mode resolution failed: %w", strings.TrimSpace(cfg.ID), err)
	}
	cfg.ExecutionMode = executionMode
	if executionMode == runtimeeffects.ExecutionModeMock {
		if !cfg.Mock.Configured() {
			return fmt.Errorf("agent %s selects llm_backend %q but does not declare a mock performance", strings.TrimSpace(cfg.ID), profile.ID)
		}
	} else {
		cfg.Mock = mockperformance.Performance{}
	}
	return nil
}

func (am *AgentManager) validateNativeToolAdmission(ctx context.Context, cfg models.AgentConfig) error {
	if am == nil || am.nativeToolAdmissionValidator == nil || !cfg.NativeTools.Any() {
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

func (am *AgentManager) ReconfigureAgent(agentID string, cfg models.AgentConfig) error {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return errors.New("agentID is required")
	}
	result, err := am.replaceExecution(am.runtimeContext(), agentID, "reconfigure", "", &cfg)
	if err != nil {
		return err
	}
	if result.transitioned && am.lifecycle.store == nil && am.store != nil {
		rec := PersistedAgent{Config: result.config, Status: "active", HiredBy: "reconfigure"}
		if err := am.store.UpsertAgent(am.runtimeContext(), rec); err != nil {
			return fmt.Errorf("persist reconfigured agent %s: %w", agentID, err)
		}
	}
	return nil
}

func (am *AgentManager) TeardownAgent(agentID string) error {
	if strings.TrimSpace(agentID) == "" {
		return errors.New("agentID is required")
	}
	_, exists := am.lifecycle.executionSnapshot(agentID)
	if !exists {
		return fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)
	}
	if err := am.lifecycle.terminate(am.runtimeContext(), agentID, "teardown", AgentLifecycleTerminated); err != nil {
		return err
	}
	_ = am.projectLifecycleDiagnostics(context.Background())

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
