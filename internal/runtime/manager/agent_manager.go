package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
	llmselection "swarm/internal/runtime/llm/selection"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	workspace "swarm/internal/runtime/workspace"
)

type AgentManager struct {
	mu                              sync.RWMutex
	agents                          map[string]Agent
	agentCfg                        map[string]models.AgentConfig
	agentUpAt                       map[string]time.Time
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
	runtimeIngressSafetyPause       func(context.Context, string) error
	runtimeMode                     string
	llmBackend                      string
	throttleSuppressPrefixes        []string
	inFlightMu                      sync.Mutex
	inFlight                        map[string]struct{}
	workflowInstances               flowInstancePersistence
	selectedContractRouteRecoveries map[string]SelectedContractRouteRecoveryTruth

	runMu              sync.Mutex
	running            bool
	shuttingDown       bool
	authBreakerTripped bool
	runCtx             context.Context
	cancelRun          context.CancelFunc
	loopCancel         map[string]context.CancelFunc
	runWG              sync.WaitGroup

	poisonMu            sync.Mutex
	poisonPanicCounts   map[string]int
	poisonEventEntities map[string]map[string]struct{}
	poisonEventEmitted  map[string]bool

	deadLetterMu         sync.Mutex
	deadLetterWindows    map[string][]deadLetterEscalationSample
	deadLetterLastRaised map[string]time.Time
}

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
	errText    string
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
	return &AgentManager{
		agents:                          make(map[string]Agent),
		agentCfg:                        make(map[string]models.AgentConfig),
		agentUpAt:                       make(map[string]time.Time),
		bus:                             bus,
		factory:                         factory,
		store:                           store,
		workspaces:                      opts.Workspaces,
		sessions:                        opts.Sessions,
		semanticSource:                  opts.SemanticSource,
		promptResolver:                  opts.PromptResolver,
		workflowInstances:               opts.WorkflowInstances,
		selectedContractRouteRecoveries: map[string]SelectedContractRouteRecoveryTruth{},
		runtimeMode:                     strings.TrimSpace(opts.RuntimeMode),
		budget:                          opts.Budget,
		resetRuntimeOwnedState:          opts.ResetRuntimeOwnedState,
		runtimeShutdownAdmissionClosed:  opts.RuntimeShutdownAdmissionClosed,
		runtimeIngressSafetyPause:       opts.RuntimeIngressSafetyPause,
		throttleSuppressPrefixes:        throttleSuppressPrefixes,
		llmBackend:                      normalizeManagerLLMBackend(opts.LLMBackend),
		inFlight:                        make(map[string]struct{}),
		loopCancel:                      make(map[string]context.CancelFunc),
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
	am.runMu.Lock()
	ctx := am.runCtx
	am.runMu.Unlock()
	if ctx != nil && ctx.Err() == nil {
		return ctx
	}
	return context.Background()
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
	am.mu.RLock()
	baseCfg, ok := am.agentCfg[baseAgentID]
	am.mu.RUnlock()
	if !ok {
		return fmt.Errorf("base agent not found: %s", baseAgentID)
	}
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
		if strings.Contains(err.Error(), "agent already exists") {
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
	a, err := am.buildAgent(rec.Config)
	if err != nil {
		return err
	}

	am.mu.Lock()
	if _, exists := am.agents[a.ID()]; exists {
		am.mu.Unlock()
		return fmt.Errorf("agent already exists: %s", a.ID())
	}
	am.agents[a.ID()] = a
	am.agentCfg[a.ID()] = rec.Config
	startedAt := rec.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	am.agentUpAt[a.ID()] = startedAt
	am.mu.Unlock()

	if persist && am.store != nil {
		if err := am.store.UpsertAgent(ctx, rec); err != nil {
			am.mu.Lock()
			delete(am.agents, a.ID())
			delete(am.agentCfg, a.ID())
			delete(am.agentUpAt, a.ID())
			am.mu.Unlock()
			return fmt.Errorf("persist agent %s: %w", rec.Config.ID, err)
		}
	}

	am.runMu.Lock()
	isRunning := am.running
	runCtx := am.runCtx
	am.runMu.Unlock()
	_ = persist
	if isRunning {
		am.startAgentLoop(runCtx, a)
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
	if strings.TrimSpace(agentID) == "" {
		return errors.New("agentID is required")
	}
	am.mu.RLock()
	current, ok := am.agentCfg[agentID]
	am.mu.RUnlock()
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}

	updated := mergeAgentConfig(current, cfg)
	if updated.ID == "" {
		updated.ID = agentID
	}
	if updated.ID != agentID {
		return fmt.Errorf("agent id mismatch: target=%s config.id=%s", agentID, updated.ID)
	}

	newAgent, err := am.buildAgent(updated)
	if err != nil {
		return err
	}

	// Spec v2.0: reconfigure triggers session rotation so the agent restarts on a
	// clean session with the new prompt/tool set. Fail fast if rotation fails.
	am.mu.RLock()
	sessionRegistry := am.sessions
	am.mu.RUnlock()
	conversationMode := strings.TrimSpace(updated.ConversationMode)
	if sessionRegistry != nil && conversationMode != "" && sessions.IsLiveSessionRuntimeMode(conversationMode) {
		runtimeMode := sessions.NormalizeConversationRuntimeMode(conversationMode)
		scopeKey, err := sessions.DeclaredScopeKey(updated)
		if err != nil {
			return fmt.Errorf("agent reconfigure session rotation failed: agent=%s runtime=%s: %w", agentID, conversationMode, err)
		}
		rotationCtx := models.WithActor(am.runtimeContext(), updated)
		rotated, err := sessionRegistry.Rotate(rotationCtx, agentID, runtimeMode, sessions.NormalizeSessionScope(updated.SessionScope), "reconfigure", sessions.RotationMetadata{
			CheckpointSummary: "agent reconfigured",
		}, scopeKey)
		if err != nil {
			return fmt.Errorf("agent reconfigure session rotation failed: agent=%s runtime=%s: %w", agentID, conversationMode, err)
		} else if rotated != nil {
			llm.LogSessionRotatedForRun(am.runtimeContext(), am.bus, agentID, conversationMode, "", rotated.SessionID, "", "agent_reconfigured", 0, 0)
		}
	}
	if am.store != nil {
		if err := am.store.UpsertAgent(am.runtimeContext(), PersistedAgent{
			Config:  updated,
			Status:  "active",
			HiredBy: "reconfigure",
		}); err != nil {
			return fmt.Errorf("persist reconfigured agent %s: %w", agentID, err)
		}
	}

	am.mu.Lock()
	am.agents[agentID] = newAgent
	am.agentCfg[agentID] = updated
	am.mu.Unlock()

	am.runMu.Lock()
	if cancel, ok := am.loopCancel[agentID]; ok {
		cancel()
		delete(am.loopCancel, agentID)
	}
	ctx := am.runCtx
	running := am.running
	am.runMu.Unlock()
	if running {
		am.startAgentLoop(ctx, newAgent)
	}
	return nil
}

func (am *AgentManager) TeardownAgent(agentID string) error {
	if strings.TrimSpace(agentID) == "" {
		return errors.New("agentID is required")
	}
	am.runMu.Lock()
	if cancel, ok := am.loopCancel[agentID]; ok {
		cancel()
		delete(am.loopCancel, agentID)
	}
	am.runMu.Unlock()

	am.mu.Lock()
	_, exists := am.agents[agentID]
	if !exists {
		am.mu.Unlock()
		return fmt.Errorf("agent not found: %s", agentID)
	}
	delete(am.agents, agentID)
	delete(am.agentCfg, agentID)
	delete(am.agentUpAt, agentID)
	am.mu.Unlock()

	if am.bus != nil {
		am.bus.Unsubscribe(agentID)
	}
	if am.store != nil {
		if err := am.store.MarkAgentTerminated(am.runtimeContext(), agentID); err != nil {
			return err
		}
	}
	return nil
}
