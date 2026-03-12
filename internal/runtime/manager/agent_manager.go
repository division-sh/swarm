package manager

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimecontracts "empireai/internal/runtime/contracts"
	llm "empireai/internal/runtime/llm"
	"empireai/internal/runtime/sessions"
	workspace "empireai/internal/runtime/workspace"
	"github.com/google/uuid"
)

type OpCOTeardownCompletePayload struct {
	VerticalID       string `json:"vertical_id"`
	AgentsRemoved    int    `json:"agents_removed"`
	RoutingCleared   bool   `json:"routing_cleared"`
	WorkspaceStopped bool   `json:"workspace_stopped"`
	Priority         string `json:"priority"`
}

type AgentManager struct {
	mu          sync.RWMutex
	agents      map[string]Agent
	agentCfg    map[string]models.AgentConfig
	agentUpAt   map[string]time.Time
	workspaces  workspace.Lifecycle
	bus         Bus
	factory     AgentFactory
	store       ManagerPersistence
	sessions    sessions.Registry
	budget      BudgetGuard
	runtimeMode string
	inFlightMu  sync.Mutex
	inFlight    map[string]struct{}

	runMu                sync.Mutex
	running              bool
	authBreakerTripped   bool
	runCtx               context.Context
	cancelRun            context.CancelFunc
	loopCancel           map[string]context.CancelFunc
	controlCancel        context.CancelFunc
	runWG                sync.WaitGroup
	disableSpinupControl bool

	poisonMu          sync.Mutex
	poisonPanicCounts map[string]int
}

const (
	managerShutdownTimeout  = 15 * time.Second
	poisonPanicQuarantineAt = 3
	receiptWriteTimeout     = 3 * time.Second
	runtimeSpecVersion      = "v2.2.1"
)

func NewAgentManager(bus Bus, factory AgentFactory, stores ...ManagerPersistence) *AgentManager {
	return NewAgentManagerWithOptions(bus, factory, AgentManagerOptions{}, stores...)
}

func NewAgentManagerWithOptions(bus Bus, factory AgentFactory, opts AgentManagerOptions, stores ...ManagerPersistence) *AgentManager {
	var store ManagerPersistence
	if len(stores) > 0 {
		store = stores[0]
	}
	disableSpinupControl := true
	if opts.EnableLegacySpinupControl {
		disableSpinupControl = false
	}
	if opts.DisableSpinupControl {
		disableSpinupControl = true
	}
	return &AgentManager{
		agents:               make(map[string]Agent),
		agentCfg:             make(map[string]models.AgentConfig),
		agentUpAt:            make(map[string]time.Time),
		bus:                  bus,
		factory:              factory,
		store:                store,
		workspaces:           opts.Workspaces,
		sessions:             opts.Sessions,
		runtimeMode:          strings.TrimSpace(opts.RuntimeMode),
		budget:               opts.Budget,
		disableSpinupControl: disableSpinupControl,
		inFlight:             make(map[string]struct{}),
		loopCancel:           make(map[string]context.CancelFunc),
		poisonPanicCounts:    make(map[string]int),
	}
}

// SetSessionRegistry enables spec v2.0 session rotation behavior on reconfigure.
// runtimeMode should match the LLM runtime (e.g. "api" or "cli_test").
func (am *AgentManager) SetSessionRegistry(sessions sessions.Registry, runtimeMode string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.sessions = sessions
	am.runtimeMode = strings.TrimSpace(runtimeMode)
}

func (am *AgentManager) SetBudgetTracker(tracker BudgetGuard) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.budget = tracker
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

func (am *AgentManager) PublishEvent(ctx context.Context, evt events.Event) error {
	if am == nil || am.bus == nil {
		return errors.New("event bus is not configured")
	}
	return am.bus.Publish(ctx, evt)
}

func (am *AgentManager) SpawnAgent(cfg models.AgentConfig) error {
	rec := PersistedAgent{
		Config:  cfg,
		Status:  "active",
		HiredBy: "runtime",
	}
	return am.spawnAgentInternal(am.runtimeContext(), rec, true)
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
	if persist {
		payload := mustJSON(map[string]any{
			"agent_id":    rec.Config.ID,
			"agent_type":  rec.Config.Type,
			"role":        rec.Config.Role,
			"mode":        rec.Config.Mode,
			"vertical_id": rec.Config.VerticalID,
			"hired_by":    rec.HiredBy,
		})
		if err := am.bus.Publish(am.runtimeContext(), (events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("agent.started"),
			SourceAgent: rec.Config.ID,
			Payload:     payload,
			CreatedAt:   time.Now(),
		}).WithEntityID(rec.Config.VerticalID)); err != nil {
			RuntimeWarn("agent-manager", "agent.started publish failed agent=%s err=%v", rec.Config.ID, err)
		}
	}
	if isRunning {
		am.startAgentLoop(runCtx, a)
	}
	return nil
}

func (am *AgentManager) buildAgent(cfg models.AgentConfig) (Agent, error) {
	cfg = am.applyContractPrompt(cfg)
	cfg = am.applyPromptOverride(am.runtimeContext(), cfg)
	if am.factory != nil {
		return am.factory(cfg)
	}
	return newGenericAgent(cfg), nil
}

func (am *AgentManager) applyContractPrompt(cfg models.AgentConfig) models.AgentConfig {
	prompt, found, err := runtimecontracts.LoadPromptForAgent(cfg, "")
	if err != nil {
		RuntimeWarn(
			"agent-manager",
			"contract prompt load failed agent_id=%s role=%s err=%v",
			strings.TrimSpace(cfg.ID),
			strings.TrimSpace(cfg.Role),
			err,
		)
		return cfg
	}
	if !found {
		return cfg
	}
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return cfg
	}
	prompt = ExpandConfigPromptTemplate(prompt, cfg.Config)
	cfg.Config = withSystemPrompt(cfg.Config, prompt)
	return cfg
}

func (am *AgentManager) applyPromptOverride(ctx context.Context, cfg models.AgentConfig) models.AgentConfig {
	if am == nil || am.store == nil {
		return cfg
	}
	store, ok := am.store.(PromptOverridePersistence)
	if !ok || store == nil {
		return cfg
	}
	agentID := strings.TrimSpace(cfg.ID)
	if agentID == "" {
		return cfg
	}
	override, found, err := store.GetPromptOverride(ctx, agentID)
	if err != nil || !found {
		return cfg
	}
	overridePrompt := strings.TrimSpace(override.Prompt)
	if overridePrompt == "" {
		return cfg
	}
	cfg.Config = withSystemPrompt(cfg.Config, overridePrompt)
	return cfg
}

type AgentPromptState struct {
	AgentID         string
	Role            string
	Mode            string
	TemplatePrompt  string
	EffectivePrompt string
	Override        *PromptOverrideRecord
}

func (am *AgentManager) GetAgentPromptState(ctx context.Context, agentID string) (AgentPromptState, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return AgentPromptState{}, errors.New("agentID is required")
	}
	am.mu.RLock()
	cfg, ok := am.agentCfg[agentID]
	am.mu.RUnlock()
	if !ok {
		return AgentPromptState{}, fmt.Errorf("agent not found: %s", agentID)
	}
	state := AgentPromptState{
		AgentID:         agentID,
		Role:            strings.TrimSpace(cfg.Role),
		Mode:            strings.TrimSpace(cfg.Mode),
		TemplatePrompt:  extractSystemPromptFromConfig(cfg.Config),
		EffectivePrompt: extractSystemPromptFromConfig(cfg.Config),
	}
	if am.store == nil {
		return state, nil
	}
	store, ok := am.store.(PromptOverridePersistence)
	if !ok || store == nil {
		return state, nil
	}
	override, found, err := store.GetPromptOverride(ctx, agentID)
	if err != nil {
		return AgentPromptState{}, err
	}
	if found {
		state.Override = &override
		state.EffectivePrompt = strings.TrimSpace(override.Prompt)
	}
	return state, nil
}

func (am *AgentManager) SetAgentPromptOverride(ctx context.Context, agentID, prompt, source, notes string) error {
	agentID = strings.TrimSpace(agentID)
	prompt = strings.TrimSpace(prompt)
	if agentID == "" {
		return errors.New("agentID is required")
	}
	if prompt == "" {
		return errors.New("prompt is required")
	}
	if am.store == nil {
		return errors.New("prompt overrides require persistent store")
	}
	store, ok := am.store.(PromptOverridePersistence)
	if !ok || store == nil {
		return errors.New("prompt overrides are unsupported by current store")
	}
	current, err := am.GetAgentPromptState(ctx, agentID)
	if err != nil {
		return err
	}
	if err := store.UpsertPromptOverride(ctx, PromptOverrideRecord{
		AgentID:        agentID,
		Prompt:         prompt,
		PreviousPrompt: strings.TrimSpace(current.EffectivePrompt),
		Source:         strings.TrimSpace(source),
		Notes:          strings.TrimSpace(notes),
	}); err != nil {
		return err
	}
	return am.ReconfigureAgent(agentID, models.AgentConfig{})
}

func (am *AgentManager) RevertAgentPromptOverride(ctx context.Context, agentID string) error {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return errors.New("agentID is required")
	}
	if am.store == nil {
		return errors.New("prompt overrides require persistent store")
	}
	store, ok := am.store.(PromptOverridePersistence)
	if !ok || store == nil {
		return errors.New("prompt overrides are unsupported by current store")
	}
	if err := store.DeletePromptOverride(ctx, agentID); err != nil {
		return err
	}
	return am.ReconfigureAgent(agentID, models.AgentConfig{})
}

func (am *AgentManager) SpawnAgentFor(_ string, cfg models.AgentConfig) error {
	return am.SpawnAgent(cfg)
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

	// Spec v2.0: reconfigure triggers session rotation so the agent restarts on a
	// clean session with the new prompt/tool set.
	am.mu.RLock()
	sessions := am.sessions
	runtimeMode := strings.TrimSpace(am.runtimeMode)
	am.mu.RUnlock()
	if sessions != nil && runtimeMode != "" {
		if rotated, err := sessions.Rotate(ctx, agentID, runtimeMode, "reconfigure", "agent reconfigured", ""); err != nil {
			log.Printf("agent reconfigure session rotation failed: agent=%s runtime=%s err=%v", agentID, runtimeMode, err)
		} else if rotated != nil {
			llm.LogSessionRotated(agentID, runtimeMode, "", rotated.SessionID, "", "agent_reconfigured", 0, 0)
		}
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
