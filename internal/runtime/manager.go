package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	"github.com/google/uuid"
)

type Agent interface {
	ID() string
	Type() string
	Subscriptions() []events.EventType
	OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error)
}

type BoardInteractiveAgent interface {
	BoardStep(ctx context.Context, directive string) (string, error)
}

type AgentFactory func(cfg models.AgentConfig) (Agent, error)

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
	routeMeta   map[string]PersistedRoutingRule
	workspaces  WorkspaceLifecycle
	bus         *EventBus
	factory     AgentFactory
	store       ManagerPersistence
	sessions    SessionRegistry
	budget      *BudgetTracker
	runtimeMode string
	inFlightMu  sync.Mutex
	inFlight    map[string]struct{}

	runMu              sync.Mutex
	running            bool
	authBreakerTripped bool
	runCtx             context.Context
	cancelRun          context.CancelFunc
	loopCancel         map[string]context.CancelFunc
	controlCancel      context.CancelFunc
	runWG              sync.WaitGroup

	poisonMu          sync.Mutex
	poisonPanicCounts map[string]int
}

const (
	managerShutdownTimeout  = 15 * time.Second
	poisonPanicQuarantineAt = 3
	receiptWriteTimeout     = 3 * time.Second
	runtimeSpecVersion      = "v2.0.44"
)

func NewAgentManager(bus *EventBus, factory AgentFactory, stores ...ManagerPersistence) *AgentManager {
	var store ManagerPersistence
	if len(stores) > 0 {
		store = stores[0]
	}
	return &AgentManager{
		agents:            make(map[string]Agent),
		agentCfg:          make(map[string]models.AgentConfig),
		agentUpAt:         make(map[string]time.Time),
		routeMeta:         make(map[string]PersistedRoutingRule),
		bus:               bus,
		factory:           factory,
		store:             store,
		inFlight:          make(map[string]struct{}),
		loopCancel:        make(map[string]context.CancelFunc),
		poisonPanicCounts: make(map[string]int),
	}
}

// SetSessionRegistry enables spec v2.0 session rotation behavior on reconfigure.
// runtimeMode should match the LLM runtime (e.g. "api" or "cli_test").
func (am *AgentManager) SetSessionRegistry(sessions SessionRegistry, runtimeMode string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.sessions = sessions
	am.runtimeMode = strings.TrimSpace(runtimeMode)
}

func (am *AgentManager) SetBudgetTracker(tracker *BudgetTracker) {
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
		payload, _ := json.Marshal(map[string]any{
			"agent_id":    rec.Config.ID,
			"agent_type":  rec.Config.Type,
			"role":        rec.Config.Role,
			"mode":        rec.Config.Mode,
			"vertical_id": rec.Config.VerticalID,
			"hired_by":    rec.HiredBy,
		})
		_ = am.bus.Publish(am.runtimeContext(), events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("agent.started"),
			SourceAgent: rec.Config.ID,
			VerticalID:  rec.Config.VerticalID,
			Payload:     payload,
			CreatedAt:   time.Now(),
		})
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
	prompt, found, err := loadContractPromptForAgent(cfg, "")
	if err != nil {
		runtimeWarn(
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

func (am *AgentManager) SpawnOpCo(verticalID string, mandate models.MandateDocument) error {
	if verticalID == "" {
		return errors.New("verticalID is required")
	}

	if am.workspaces != nil {
		if err := am.workspaces.EnsureVerticalWorkspace(am.runtimeContext(), verticalID); err != nil {
			return fmt.Errorf("ensure vertical workspace: %w", err)
		}
	}

	// In-memory/test mode fallback: keep legacy roster/routes so unit tests and
	// inmemory runs still work without a Postgres-backed template store.
	if am.store == nil {
		agents := defaultOpCoRoster(verticalID)
		for _, rec := range agents {
			if err := am.spawnAgentInternal(am.runtimeContext(), rec, true); err != nil {
				return err
			}
		}
		rules := defaultOpCoRoutes(verticalID)
		for _, rule := range rules {
			am.setRouteMeta(routeRuleKey(rule.VerticalID, rule.EventPattern, rule.SubscriberID), rule)
		}
		rt := &RoutingTable{VerticalID: verticalID}
		for _, r := range rules {
			if r.Status != "active" {
				continue
			}
			rt.Routes = append(rt.Routes, Route{
				EventPattern: r.EventPattern,
				SubscriberID: r.SubscriberID,
				Status:       r.Status,
			})
		}
		if err := am.bus.SetRoutingTable(verticalID, rt); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{
			"vertical_id":      verticalID,
			"ceo_agent_id":     opCoAgentID("opco-ceo", verticalID),
			"agent_count":      len(agents),
			"template_version": "inmemory",
			"priority":         "normal",
			"mandate":          mandate,
		})
		_ = am.bus.Publish(am.runtimeContext(), events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("opco.ceo_ready"),
			SourceAgent: "agent-manager",
			VerticalID:  verticalID,
			Payload:     payload,
			CreatedAt:   time.Now(),
		})
		return nil
	}

	if am.store != nil {
		if err := am.store.EnsureVerticalSchema(am.runtimeContext(), verticalID); err != nil {
			return fmt.Errorf("ensure vertical schema: %w", err)
		}
	}
	template, err := am.loadLatestTemplate(am.runtimeContext())
	if err != nil {
		return err
	}
	bootstrapVersion := am.resolveBootstrapVersion(am.runtimeContext(), template.Version)

	// Template placeholder context (spec v2.0 uses {vertical_id}, {vertical_name}, etc.).
	verticalName := ""
	verticalGeography := ""
	verticalSlug := ""
	if reader, ok := am.store.(VerticalInfoReader); ok && reader != nil {
		if info, found, err := reader.GetVerticalInfo(am.runtimeContext(), verticalID); err == nil && found {
			verticalName = strings.TrimSpace(info.Name)
			verticalGeography = strings.TrimSpace(info.Geography)
			verticalSlug = strings.TrimSpace(info.Slug)
		}
	}

	ceoID := opCoAgentID("opco-ceo", verticalID)
	if ceoID == "" {
		return errors.New("failed to derive opco ceo id")
	}

	orgRoster := renderOpCoRoster(template.Agents, verticalID)
	mandateText := renderMandateText(mandate)

	agents := make([]PersistedAgent, 0, len(template.Agents))
	for _, at := range template.Agents {
		role := strings.TrimSpace(at.Role)
		if role == "" {
			continue
		}
		systemPrompt := expandTemplateText(strings.TrimSpace(at.SystemPrompt), map[string]string{
			"{vertical_id}":        verticalID,
			"{vertical_name}":      verticalName,
			"{vertical_slug}":      verticalSlug,
			"{geography}":          verticalGeography,
			"{org_roster}":         orgRoster,
			"{mandate_document}":   mandateText,
			"{founder_directives}": strings.TrimSpace(coalesce(mandate.FounderDirectives, mandate.FounderNotes)),
		})
		cfg := models.AgentConfig{
			ID:         opCoAgentID(role, verticalID),
			Type:       strings.TrimSpace(at.Type),
			Role:       role,
			Mode:       "operating",
			VerticalID: verticalID,
			ParentAgent: func() string {
				parent := strings.TrimSpace(at.ParentRole)
				if parent == "" {
					return ""
				}
				return opCoAgentID(parent, verticalID)
			}(),
			Subscriptions: append([]string(nil), at.Subscriptions...),
		}
		if cfg.ID == "" {
			return fmt.Errorf("failed to derive agent id for role=%s", role)
		}
		// Persist runtime-only config (prompt/tools/constraints) in AgentConfig.Config.
		cfg.Config = mustJSON(map[string]any{
			"system_prompt": systemPrompt,
			"tools":         normalizeStringList(at.Tools),
			"constraints":   at.Constraints,
		})

		agents = append(agents, PersistedAgent{
			Config:          cfg,
			ParentAgentID:   cfg.ParentAgent,
			CoordinatorID:   ceoID,
			Status:          "active",
			HiredBy:         "agent-manager",
			TemplateVersion: template.Version,
		})
	}
	if len(agents) == 0 {
		return fmt.Errorf("org template %s produced no opco agents", template.Version)
	}

	// Persisting agents with parent_agent_id requires parent rows to exist first.
	// Templates are author-friendly, not guaranteed to be topologically ordered.
	agents, err = orderAgentsByParent(agents)
	if err != nil {
		return fmt.Errorf("order opco agents by parent: %w", err)
	}
	for _, rec := range agents {
		if err := am.spawnAgentInternal(am.runtimeContext(), rec, true); err != nil {
			return err
		}
	}

	installedBy := ceoID
	rules := make([]PersistedRoutingRule, 0, len(template.BootstrapRoutes)+len(template.SeededRoutes))
	for _, rt := range template.BootstrapRoutes {
		rules = append(rules, PersistedRoutingRule{
			VerticalID:       verticalID,
			EventPattern:     strings.TrimSpace(rt.EventPattern),
			SubscriberID:     resolveTemplateSubscriber(verticalID, rt),
			InstalledBy:      installedBy,
			Reason:           strings.TrimSpace(rt.Reason),
			Status:           "active",
			Source:           "bootstrap",
			BootstrapVersion: bootstrapVersion,
		})
	}
	for _, rt := range template.SeededRoutes {
		rules = append(rules, PersistedRoutingRule{
			VerticalID:       verticalID,
			EventPattern:     strings.TrimSpace(rt.EventPattern),
			SubscriberID:     resolveTemplateSubscriber(verticalID, rt),
			InstalledBy:      installedBy,
			Reason:           strings.TrimSpace(rt.Reason),
			Status:           "active",
			Source:           "seeded",
			BootstrapVersion: bootstrapVersion,
		})
	}

	for _, rule := range rules {
		if rule.EventPattern == "" || rule.SubscriberID == "" {
			continue
		}
		am.setRouteMeta(routeRuleKey(rule.VerticalID, rule.EventPattern, rule.SubscriberID), rule)
		if am.store != nil {
			if err := am.store.UpsertRoutingRule(am.runtimeContext(), rule); err != nil {
				return fmt.Errorf("persist routing rule %s -> %s: %w", rule.EventPattern, rule.SubscriberID, err)
			}
		}
	}

	rt := &RoutingTable{VerticalID: verticalID}
	for _, r := range rules {
		if r.Status != "active" {
			continue
		}
		rt.Routes = append(rt.Routes, Route{
			EventPattern: r.EventPattern,
			SubscriberID: r.SubscriberID,
			Status:       r.Status,
		})
	}
	if err := am.bus.SetRoutingTable(verticalID, rt); err != nil {
		return err
	}

	if err := am.installDefaultOpCoHeartbeats(am.runtimeContext(), verticalID); err != nil {
		log.Printf("install default opco heartbeats failed vertical=%s err=%v", verticalID, err)
	}

	if am.store != nil {
		if err := am.store.SetVerticalTemplateVersion(am.runtimeContext(), verticalID, template.Version); err != nil {
			return err
		}
	}

	payload, _ := json.Marshal(map[string]any{
		"vertical_id":      verticalID,
		"ceo_agent_id":     ceoID,
		"agent_count":      len(agents),
		"template_version": template.Version,
		"priority":         "normal",
		"mandate":          mandate,
	})
	_ = am.bus.Publish(am.runtimeContext(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.ceo_ready"),
		SourceAgent: "agent-manager",
		VerticalID:  verticalID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	})

	return nil
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

func (am *AgentManager) installDefaultOpCoHeartbeats(ctx context.Context, verticalID string) error {
	if strings.TrimSpace(verticalID) == "" {
		return nil
	}
	store, ok := am.store.(SchedulePersistence)
	if !ok || store == nil {
		return nil
	}
	specs := []struct {
		role      string
		eventType string
		interval  string
	}{
		{role: "vp-product", eventType: "heartbeat.vp_product", interval: "@every 2h"},
		{role: "chief-of-staff", eventType: "heartbeat.chief_of_staff", interval: "@every 4h"},
		{role: "opco-ceo", eventType: "heartbeat.opco_ceo", interval: "@every 8h"},
	}
	for _, spec := range specs {
		if err := store.UpsertSchedule(ctx, Schedule{
			AgentID:    opCoAgentID(spec.role, verticalID),
			EventType:  spec.eventType,
			Mode:       "cron",
			Cron:       spec.interval,
			VerticalID: verticalID,
			Payload:    []byte("{}"),
		}); err != nil {
			return fmt.Errorf("upsert schedule %s/%s: %w", spec.role, spec.eventType, err)
		}
	}
	return nil
}

// orderAgentsByParent returns a stable ordering where parents appear before children.
// This matters when persisting agent rows with parent_agent_id foreign keys.
func orderAgentsByParent(in []PersistedAgent) ([]PersistedAgent, error) {
	if len(in) == 0 {
		return in, nil
	}
	inSet := make(map[string]struct{}, len(in))
	for _, a := range in {
		if id := strings.TrimSpace(a.Config.ID); id != "" {
			inSet[id] = struct{}{}
		}
	}

	done := make(map[string]struct{}, len(in))
	pending := append([]PersistedAgent(nil), in...)
	out := make([]PersistedAgent, 0, len(in))

	for len(pending) > 0 {
		progress := false
		next := pending[:0]
		for _, a := range pending {
			id := strings.TrimSpace(a.Config.ID)
			parent := strings.TrimSpace(a.ParentAgentID)
			if parent == "" {
				parent = strings.TrimSpace(a.Config.ParentAgent)
			}
			if parent == "" {
				out = append(out, a)
				if id != "" {
					done[id] = struct{}{}
				}
				progress = true
				continue
			}
			if _, ok := inSet[parent]; !ok {
				return nil, fmt.Errorf("agent %s references missing parent %s", id, parent)
			}
			if _, ok := done[parent]; ok {
				out = append(out, a)
				if id != "" {
					done[id] = struct{}{}
				}
				progress = true
				continue
			}
			next = append(next, a)
		}
		if !progress {
			ids := make([]string, 0, len(next))
			for _, a := range next {
				ids = append(ids, strings.TrimSpace(a.Config.ID))
			}
			sort.Strings(ids)
			return nil, fmt.Errorf("cyclic parent links: %v", ids)
		}
		pending = next
	}
	return out, nil
}

type orgTemplateSnapshot struct {
	Version         string
	Agents          []orgTemplateAgent
	BootstrapRoutes []orgTemplateRoute
	SeededRoutes    []orgTemplateRoute
}

func expandTemplateText(raw string, vars map[string]string) string {
	out := raw
	if strings.TrimSpace(out) == "" || len(vars) == 0 {
		return out
	}
	for k, v := range vars {
		if strings.TrimSpace(k) == "" {
			continue
		}
		out = strings.ReplaceAll(out, k, v)
	}
	return out
}

func renderOpCoRoster(agents []orgTemplateAgent, verticalID string) string {
	parts := make([]string, 0, len(agents))
	for _, a := range agents {
		role := strings.TrimSpace(a.Role)
		if role == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("- %s (%s)", role, opCoAgentID(role, verticalID)))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "\n")
}

func renderMandateText(m models.MandateDocument) string {
	obj := map[string]any{
		"vertical_id":        strings.TrimSpace(m.VerticalID),
		"geography":          strings.TrimSpace(m.Geography),
		"founder_notes":      strings.TrimSpace(m.FounderNotes),
		"founder_directives": strings.TrimSpace(m.FounderDirectives),
	}
	if len(m.BusinessBrief) > 0 {
		obj["business_brief"] = json.RawMessage(m.BusinessBrief)
	}
	if len(m.MVPSpec) > 0 {
		obj["mvp_spec"] = json.RawMessage(m.MVPSpec)
	}
	if len(m.Brand) > 0 {
		obj["brand"] = json.RawMessage(m.Brand)
	}
	if len(m.Budget) > 0 {
		obj["budget"] = json.RawMessage(m.Budget)
	}
	if len(m.CTOFeasibility) > 0 {
		obj["cto_feasibility"] = json.RawMessage(m.CTOFeasibility)
	}
	if len(m.LaunchTargets) > 0 {
		obj["launch_targets"] = json.RawMessage(m.LaunchTargets)
	}
	b, _ := json.MarshalIndent(obj, "", "  ")
	return string(b)
}

type orgTemplateAgent struct {
	Role          string         `json:"role"`
	ParentRole    string         `json:"parent_role"`
	Type          string         `json:"type"`
	SystemPrompt  string         `json:"system_prompt"`
	Tools         []string       `json:"tools"`
	Subscriptions []string       `json:"subscriptions"`
	Constraints   map[string]any `json:"constraints,omitempty"`
}

type orgTemplateRoute struct {
	EventPattern   string `json:"event_pattern"`
	SubscriberRole string `json:"subscriber_role"`
	SubscriberID   string `json:"subscriber_id"`
	Reason         string `json:"reason"`
}

func (am *AgentManager) loadLatestTemplate(ctx context.Context) (orgTemplateSnapshot, error) {
	if am.store == nil {
		return orgTemplateSnapshot{}, errors.New("org template requires persistent store")
	}
	rec, err := am.store.LoadLatestOrgTemplate(ctx)
	if err != nil {
		return orgTemplateSnapshot{}, fmt.Errorf("load latest org template: %w", err)
	}
	snap := orgTemplateSnapshot{Version: strings.TrimSpace(rec.Version)}
	if snap.Version == "" {
		return orgTemplateSnapshot{}, errors.New("latest org template has empty version")
	}
	_ = json.Unmarshal(defaultJSON(rec.Agents, []byte("[]")), &snap.Agents)
	_ = json.Unmarshal(defaultJSON(rec.BootstrapRoutes, []byte("[]")), &snap.BootstrapRoutes)
	_ = json.Unmarshal(defaultJSON(rec.SeededRoutes, []byte("[]")), &snap.SeededRoutes)
	if len(snap.BootstrapRoutes) == 0 {
		return orgTemplateSnapshot{}, fmt.Errorf("org template %s has no bootstrap routes", snap.Version)
	}
	return snap, nil
}

func (am *AgentManager) resolveBootstrapVersion(ctx context.Context, templateVersion string) int {
	if am == nil || am.store == nil {
		return 1
	}
	resolver, ok := am.store.(BootstrapVersionResolver)
	if !ok || resolver == nil {
		return 1
	}
	version, err := resolver.ResolveBootstrapVersion(ctx, templateVersion)
	if err != nil {
		log.Printf("resolve bootstrap version failed template=%s err=%v", strings.TrimSpace(templateVersion), err)
		return 1
	}
	if version <= 0 {
		return 1
	}
	return version
}

func resolveTemplateSubscriber(verticalID string, rt orgTemplateRoute) string {
	if strings.TrimSpace(rt.SubscriberID) != "" {
		return strings.TrimSpace(rt.SubscriberID)
	}
	role := strings.TrimSpace(rt.SubscriberRole)
	if role == "" {
		return ""
	}
	return opCoAgentID(role, verticalID)
}

func defaultJSON(raw, fallback []byte) []byte {
	if len(raw) == 0 {
		return fallback
	}
	return raw
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil || len(b) == 0 {
		return json.RawMessage([]byte("{}"))
	}
	return json.RawMessage(b)
}

func normalizeStringList(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
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
		if rotated, err := sessions.Rotate(agentID, runtimeMode, "reconfigure", "agent reconfigured", ""); err != nil {
			log.Printf("agent reconfigure session rotation failed: agent=%s runtime=%s err=%v", agentID, runtimeMode, err)
		} else if rotated != nil {
			logSessionRotated(agentID, runtimeMode, "", rotated.SessionID, "", "agent_reconfigured", 0, 0)
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

func (am *AgentManager) TeardownOpCo(verticalID string) error {
	if strings.TrimSpace(verticalID) == "" {
		return errors.New("verticalID is required")
	}
	am.mu.RLock()
	toRemove := make([]string, 0)
	for id, cfg := range am.agentCfg {
		if cfg.VerticalID == verticalID {
			toRemove = append(toRemove, id)
		}
	}
	am.mu.RUnlock()

	errs := make([]string, 0)
	for _, agentID := range toRemove {
		if err := am.TeardownAgent(agentID); err != nil {
			errs = append(errs, fmt.Sprintf("teardown agent %s: %v", agentID, err))
		}
	}

	if err := am.bus.SetRoutingTable(verticalID, &RoutingTable{VerticalID: verticalID}); err != nil {
		errs = append(errs, fmt.Sprintf("reset routing table: %v", err))
	}
	if am.store != nil {
		if err := am.store.DeactivateRoutingRulesByVertical(am.runtimeContext(), verticalID); err != nil {
			errs = append(errs, fmt.Sprintf("deactivate routing: %v", err))
		}
	}
	if am.workspaces != nil {
		if err := am.workspaces.StopVerticalWorkspace(am.runtimeContext(), verticalID); err != nil {
			errs = append(errs, fmt.Sprintf("stop workspace: %v", err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	if am.bus != nil {
		payload := OpCOTeardownCompletePayload{
			VerticalID:       strings.TrimSpace(verticalID),
			AgentsRemoved:    len(toRemove),
			RoutingCleared:   true,
			WorkspaceStopped: am.workspaces != nil,
			Priority:         "normal",
		}
		_ = am.bus.Publish(am.runtimeContext(), events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("opco.teardown_complete"),
			SourceAgent: "agent-manager",
			VerticalID:  strings.TrimSpace(verticalID),
			Payload:     mustJSON(payload),
			CreatedAt:   time.Now(),
		})
	}
	return nil
}

func (am *AgentManager) SetWorkspaceLifecycle(workspaces WorkspaceLifecycle) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.workspaces = workspaces
}

func (am *AgentManager) ConfigureRouting(rule PersistedRoutingRule) error {
	return am.configureRouting(rule, false)
}

// ConfigureRoutingTemplateMigration applies routing changes as part of a template migration.
// It is the only path allowed to mutate routes whose existing source is "bootstrap".
func (am *AgentManager) ConfigureRoutingTemplateMigration(rule PersistedRoutingRule) error {
	return am.configureRouting(rule, true)
}

func (am *AgentManager) configureRouting(rule PersistedRoutingRule, allowBootstrapMutation bool) error {
	if rule.VerticalID == "" || rule.EventPattern == "" || rule.SubscriberID == "" {
		return errors.New("vertical_id, event_pattern, and subscriber_id are required")
	}
	if rule.Status == "" {
		rule.Status = "active"
	}
	if rule.InstalledBy == "" {
		rule.InstalledBy = "runtime"
	}
	if rule.Source == "" {
		rule.Source = "discovered"
	}

	key := routeRuleKey(rule.VerticalID, rule.EventPattern, rule.SubscriberID)
	if existing, ok := am.getRouteMeta(key); ok {
		if existing.Source == "bootstrap" && !allowBootstrapMutation {
			return errors.New("bootstrap routes are immutable")
		}
		if rule.Source == "" {
			rule.Source = existing.Source
		}
		if rule.BootstrapVersion == 0 {
			rule.BootstrapVersion = existing.BootstrapVersion
		}
	}

	table := am.bus.GetRoutingTable(rule.VerticalID)
	if table == nil {
		table = &RoutingTable{VerticalID: rule.VerticalID}
	}
	updated := false
	for i := range table.Routes {
		r := &table.Routes[i]
		if r.EventPattern == rule.EventPattern && r.SubscriberID == rule.SubscriberID {
			r.Status = rule.Status
			updated = true
			break
		}
	}
	if !updated {
		table.Routes = append(table.Routes, Route{
			EventPattern: rule.EventPattern,
			SubscriberID: rule.SubscriberID,
			Status:       rule.Status,
		})
	}
	if err := am.bus.SetRoutingTable(rule.VerticalID, table); err != nil {
		return err
	}
	if am.store != nil {
		if err := am.store.UpsertRoutingRule(am.runtimeContext(), rule); err != nil {
			return err
		}
	}
	am.setRouteMeta(key, rule)
	return nil
}

func (am *AgentManager) RestartAgent(agentID string) error {
	am.mu.RLock()
	agent, ok := am.agents[agentID]
	am.mu.RUnlock()
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}

	am.runMu.Lock()
	if cancel, ok := am.loopCancel[agentID]; ok {
		cancel()
		delete(am.loopCancel, agentID)
	}
	ctx := am.runCtx
	running := am.running
	am.runMu.Unlock()

	if running {
		am.startAgentLoop(ctx, agent)
	}
	return nil
}

func (am *AgentManager) Shutdown() error {
	am.runMu.Lock()
	if am.cancelRun != nil {
		am.cancelRun()
		am.cancelRun = nil
	}
	if am.controlCancel != nil {
		am.controlCancel()
		am.controlCancel = nil
	}
	for id, cancel := range am.loopCancel {
		cancel()
		delete(am.loopCancel, id)
	}
	am.running = false
	am.runMu.Unlock()

	done := make(chan struct{})
	go func() {
		am.runWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(managerShutdownTimeout):
		return fmt.Errorf("agent manager shutdown timed out after %s", managerShutdownTimeout)
	}
}

func (am *AgentManager) Count() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.agents)
}

func (am *AgentManager) IsRunning() bool {
	am.runMu.Lock()
	defer am.runMu.Unlock()
	return am.running
}

func (am *AgentManager) GetAgentConfig(agentID string) (models.AgentConfig, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	cfg, ok := am.agentCfg[agentID]
	return cfg, ok
}

func (am *AgentManager) poisonKey(agentID, eventID string) string {
	return strings.TrimSpace(agentID) + "|" + strings.TrimSpace(eventID)
}

func (am *AgentManager) incrementPoisonPanicCount(agentID, eventID string) int {
	key := am.poisonKey(agentID, eventID)
	am.poisonMu.Lock()
	defer am.poisonMu.Unlock()
	am.poisonPanicCounts[key]++
	return am.poisonPanicCounts[key]
}

func (am *AgentManager) clearPoisonPanicCount(agentID, eventID string) {
	key := am.poisonKey(agentID, eventID)
	am.poisonMu.Lock()
	defer am.poisonMu.Unlock()
	delete(am.poisonPanicCounts, key)
}

func (am *AgentManager) quarantinePoisonEvent(ctx context.Context, agentID string, evt events.Event, count int, panicText string) {
	am.writeReceipt(ctx, evt.ID, agentID, "processed", fmt.Sprintf("quarantined poison event after %d panics: %s", count, strings.TrimSpace(panicText)))
	managerID := am.resolveManagerAgentID(agentID)
	if strings.TrimSpace(managerID) == "" || managerID == agentID {
		managerID = "empire-coordinator"
	}
	payload := map[string]any{
		"agent_id":    agentID,
		"event_id":    strings.TrimSpace(evt.ID),
		"event_type":  strings.TrimSpace(string(evt.Type)),
		"vertical_id": strings.TrimSpace(evt.VerticalID),
		"panic_count": count,
		"error":       strings.TrimSpace(panicText),
	}
	_ = am.bus.PublishDirect(am.runtimeContext(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("ops.poison_event_quarantined"),
		SourceAgent: "runtime",
		VerticalID:  strings.TrimSpace(evt.VerticalID),
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}, []string{managerID})
}

func deterministicOutputEventID(inbound events.Event, agentID string, index int, out events.Event) string {
	seed := strings.Join([]string{
		strings.TrimSpace(inbound.ID),
		strings.TrimSpace(agentID),
		fmt.Sprintf("%d", index),
		strings.TrimSpace(string(out.Type)),
		strings.TrimSpace(out.VerticalID),
	}, "|")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed)).String()
}

type eventExistenceReader interface {
	EventExists(ctx context.Context, eventID string) (bool, error)
}

func (am *AgentManager) shouldSkipAlreadyPublishedOutput(ctx context.Context, eventID string) bool {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || am.store == nil {
		return false
	}
	reader, ok := am.store.(eventExistenceReader)
	if !ok {
		return false
	}
	exists, err := reader.EventExists(ctx, eventID)
	if err != nil {
		return false
	}
	return exists
}

func (am *AgentManager) safeProcessEvent(ctx context.Context, agent Agent, evt events.Event) (err error, panicked bool, panicText string) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			panicText = fmt.Sprint(r)
		}
	}()
	err = am.processEvent(ctx, agent, evt)
	return
}

func (am *AgentManager) ChatWithAgent(ctx context.Context, agentID, directive string) (string, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return "", errors.New("agent id is required")
	}
	am.mu.RLock()
	agent, ok := am.agents[agentID]
	am.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("agent not found: %s", agentID)
	}
	chatAgent, ok := agent.(BoardInteractiveAgent)
	if !ok {
		return "", fmt.Errorf("agent does not support board chat: %s", agentID)
	}
	return chatAgent.BoardStep(ctx, directive)
}

func (am *AgentManager) Run(ctx context.Context) {
	am.runMu.Lock()
	if am.running {
		am.runMu.Unlock()
		return
	}
	runRoot := WithRuntimeEpoch(ctx, CurrentRuntimeEpoch())
	am.runCtx, am.cancelRun = context.WithCancel(runRoot)
	am.running = true
	am.authBreakerTripped = false
	am.runMu.Unlock()

	am.mu.RLock()
	agents := make([]Agent, 0, len(am.agents))
	for _, a := range am.agents {
		agents = append(agents, a)
	}
	am.mu.RUnlock()

	for _, a := range agents {
		am.startAgentLoop(am.runCtx, a)
	}
	am.startControlLoop(am.runCtx)

	am.runWG.Add(1)
	go func() {
		defer am.runWG.Done()
		am.retryLoop(am.runCtx)
	}()

	go func() {
		<-am.runCtx.Done()
		am.runMu.Lock()
		am.running = false
		if am.controlCancel != nil {
			am.controlCancel()
			am.controlCancel = nil
		}
		for id, cancel := range am.loopCancel {
			cancel()
			delete(am.loopCancel, id)
		}
		am.runMu.Unlock()
	}()
}

func (am *AgentManager) Recover(ctx context.Context) error {
	if am.store == nil {
		return nil
	}

	rules, err := am.store.LoadRoutingRules(ctx)
	if err != nil {
		return fmt.Errorf("load routing rules: %w", err)
	}
	if err := am.hydrateRoutingTables(rules); err != nil {
		return err
	}

	agents, err := am.store.LoadAgents(ctx)
	if err != nil {
		return fmt.Errorf("load agents: %w", err)
	}
	sort.SliceStable(agents, func(i, j int) bool {
		return agents[i].StartedAt.Before(agents[j].StartedAt)
	})
	for _, rec := range agents {
		if rec.Config.ID == "" {
			continue
		}
		if err := am.spawnAgentInternal(ctx, rec, false); err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("hydrate agent %s: %w", rec.Config.ID, err)
		}
	}

	if err := NewRecoveryManagerWith(am.bus.store, am.bus).Recover(ctx); err != nil {
		return fmt.Errorf("recover pipeline receipts: %w", err)
	}

	if err := am.replayPendingEvents(ctx); err != nil {
		return err
	}
	return nil
}

func (am *AgentManager) retryLoop(ctx context.Context) {
	if am.store == nil {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := am.replayPendingEvents(ctx); err != nil {
				log.Printf("retry replay failed: %v", err)
			}
		}
	}
}

func (am *AgentManager) replayPendingEvents(ctx context.Context) error {
	if am.store == nil {
		return nil
	}
	if am.isAuthBreakerTripped() {
		return nil
	}

	am.mu.RLock()
	ids := make([]string, 0, len(am.agents))
	for id := range am.agents {
		ids = append(ids, id)
	}
	am.mu.RUnlock()

	for _, id := range ids {
		if am.isAuthBreakerTripped() {
			return nil
		}
		if err := am.ReplayAgentBacklog(ctx, id); err != nil {
			log.Printf("pending replay failed for agent=%s err=%v", id, err)
		}
	}
	return nil
}

func (am *AgentManager) ReplayAgentBacklog(ctx context.Context, agentID string) error {
	if am.store == nil {
		return fmt.Errorf("manager store unavailable")
	}
	if am.isAuthBreakerTripped() {
		return nil
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return errors.New("agent id is required")
	}
	am.mu.RLock()
	agent := am.agents[agentID]
	cfg, ok := am.agentCfg[agentID]
	since := time.Now().Add(-30 * 24 * time.Hour)
	if upAt, ok := am.agentUpAt[agentID]; ok && !upAt.IsZero() {
		since = upAt
	}
	am.mu.RUnlock()
	if !ok || agent == nil {
		return fmt.Errorf("agent not found: %s", agentID)
	}
	pending, err := am.pendingEventsForAgent(ctx, agentID, cfg, agent, since)
	if err != nil {
		return err
	}
	for _, evt := range pending {
		if am.isAuthBreakerTripped() {
			return nil
		}
		if err := am.processEvent(ctx, agent, evt); err != nil {
			log.Printf("pending replay failed for agent=%s event=%s err=%v", agentID, evt.ID, err)
			if isClaudeAuthError(err) {
				return nil
			}
		}
	}
	return nil
}

func (am *AgentManager) pendingEventsForAgent(
	ctx context.Context,
	agentID string,
	cfg models.AgentConfig,
	agent Agent,
	since time.Time,
) ([]events.Event, error) {
	pending := make([]events.Event, 0, 400)
	pendingByID := make(map[string]events.Event)

	direct, err := am.store.ListPendingEventsForAgent(ctx, agentID, since, 300)
	if err != nil {
		return nil, fmt.Errorf("load pending delivered events for %s: %w", agentID, err)
	}
	for _, evt := range direct {
		pendingByID[evt.ID] = evt
	}

	if cfg.Mode != "operating" {
		subscribed, err := am.store.ListPendingSubscribedEvents(ctx, agentID, agent.Subscriptions(), since, 300)
		if err != nil {
			return nil, fmt.Errorf("load pending subscribed events for %s: %w", agentID, err)
		}
		for _, evt := range subscribed {
			pendingByID[evt.ID] = evt
		}
	}

	for _, evt := range pendingByID {
		pending = append(pending, evt)
	}
	sort.SliceStable(pending, func(i, j int) bool {
		if pending[i].CreatedAt.Equal(pending[j].CreatedAt) {
			return pending[i].ID < pending[j].ID
		}
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})
	return pending, nil
}

func (am *AgentManager) ResetRuntimeState() error {
	if err := am.Shutdown(); err != nil {
		return err
	}
	if killer, ok := am.workspaces.(WorkspaceOrphanKiller); ok && killer != nil {
		if err := killer.KillOrphanProcesses(am.runtimeContext()); err != nil {
			return fmt.Errorf("kill workspace orphan processes: %w", err)
		}
	}
	resetMCPTurnContexts()
	if resetter, ok := am.sessions.(SessionResetter); ok && resetter != nil {
		if err := resetter.ResetAll(am.runtimeMode); err != nil {
			log.Printf("session reset failed: %v", err)
		}
	}
	if am.bus != nil {
		am.bus.ResetInMemoryState()
	}

	verticals := map[string]struct{}{}
	am.mu.Lock()
	for _, cfg := range am.agentCfg {
		if strings.TrimSpace(cfg.VerticalID) != "" {
			verticals[cfg.VerticalID] = struct{}{}
		}
	}
	for _, rule := range am.routeMeta {
		if strings.TrimSpace(rule.VerticalID) != "" {
			verticals[rule.VerticalID] = struct{}{}
		}
	}
	am.agents = make(map[string]Agent)
	am.agentCfg = make(map[string]models.AgentConfig)
	am.agentUpAt = make(map[string]time.Time)
	am.routeMeta = make(map[string]PersistedRoutingRule)
	am.inFlight = make(map[string]struct{})
	am.mu.Unlock()
	am.poisonMu.Lock()
	am.poisonPanicCounts = make(map[string]int)
	am.poisonMu.Unlock()

	for verticalID := range verticals {
		_ = am.bus.SetRoutingTable(verticalID, &RoutingTable{VerticalID: verticalID})
		if am.workspaces != nil {
			_ = am.workspaces.StopVerticalWorkspace(am.runtimeContext(), verticalID)
		}
	}
	return nil
}

func (am *AgentManager) startAgentLoop(parent context.Context, agent Agent) {
	loopCtx, cancel := context.WithCancel(parent)

	am.runMu.Lock()
	if old, ok := am.loopCancel[agent.ID()]; ok {
		old()
	}
	am.loopCancel[agent.ID()] = cancel
	am.runMu.Unlock()

	ch := am.bus.Subscribe(agent.ID(), agent.Subscriptions()...)
	am.runWG.Add(1)
	go func() {
		defer am.runWG.Done()
		consecutivePanics := 0
		for {
			panicked := false
			panicText := ""
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicked = true
						panicText = fmt.Sprint(r)
					}
				}()
				for {
					select {
					case <-loopCtx.Done():
						return
					case evt, ok := <-ch:
						if !ok {
							return
						}
						err, evtPanicked, evtPanicText := am.safeProcessEvent(loopCtx, agent, evt)
						if evtPanicked {
							panicCount := am.incrementPoisonPanicCount(agent.ID(), evt.ID)
							am.writeReceipt(loopCtx, evt.ID, agent.ID(), "error", "panic: "+strings.TrimSpace(evtPanicText))
							if panicCount >= poisonPanicQuarantineAt {
								am.quarantinePoisonEvent(loopCtx, agent.ID(), evt, panicCount, evtPanicText)
								am.clearPoisonPanicCount(agent.ID(), evt.ID)
								consecutivePanics = 0
								continue
							}
							panicked = true
							panicText = evtPanicText
							return
						}
						am.clearPoisonPanicCount(agent.ID(), evt.ID)
						consecutivePanics = 0
						if err != nil {
							log.Printf("agent %s failed processing event %s: %v", agent.ID(), evt.Type, err)
						}
					}
				}
			}()
			if !panicked {
				return
			}
			consecutivePanics++
			am.handleAgentLoopPanic(loopCtx, agent, consecutivePanics, panicText)
			if consecutivePanics >= 5 {
				return
			}
			wait := panicBackoff(consecutivePanics)
			select {
			case <-loopCtx.Done():
				return
			case <-time.After(wait):
			}
		}
	}()
}

func panicBackoff(consecutivePanics int) time.Duration {
	switch {
	case consecutivePanics <= 1:
		return 1 * time.Second
	case consecutivePanics == 2:
		return 5 * time.Second
	case consecutivePanics == 3:
		return 30 * time.Second
	case consecutivePanics == 4:
		return 2 * time.Minute
	default:
		return 10 * time.Minute
	}
}

func (am *AgentManager) handleAgentLoopPanic(ctx context.Context, agent Agent, consecutivePanics int, panicText string) {
	panicText = strings.TrimSpace(panicText)
	if panicText == "" {
		panicText = "unknown panic"
	}
	log.Printf("agent loop panic: agent=%s count=%d err=%s", agent.ID(), consecutivePanics, panicText)

	verticalID := ""
	am.mu.RLock()
	cfg, ok := am.agentCfg[agent.ID()]
	am.mu.RUnlock()
	if ok {
		verticalID = strings.TrimSpace(cfg.VerticalID)
	}

	_ = am.bus.Publish(am.runtimeContext(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("ops.agent_panic"),
		SourceAgent: "runtime",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"agent_id":           agent.ID(),
			"vertical_id":        verticalID,
			"consecutive_panics": consecutivePanics,
			"error":              panicText,
			"backoff_seconds":    int(panicBackoff(consecutivePanics).Seconds()),
		}),
		CreatedAt: time.Now(),
	})

	if consecutivePanics < 5 {
		return
	}

	if ok && am.store != nil {
		_ = am.store.UpsertAgent(ctx, PersistedAgent{
			Config:          cfg,
			ParentAgentID:   cfg.ParentAgent,
			CoordinatorID:   am.resolveManagerAgentID(agent.ID()),
			Status:          "failed",
			HiredBy:         "runtime",
			TemplateVersion: "",
			StartedAt:       time.Now(),
		})
	}

	managerID := am.resolveManagerAgentID(agent.ID())
	if strings.TrimSpace(managerID) == "" || managerID == agent.ID() {
		managerID = "empire-coordinator"
	}
	if managerID != agent.ID() {
		_ = am.bus.PublishDirect(am.runtimeContext(), events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("ops.agent_failed"),
			SourceAgent: "runtime",
			VerticalID:  verticalID,
			Payload: mustJSON(map[string]any{
				"agent_id":           agent.ID(),
				"manager_id":         managerID,
				"vertical_id":        verticalID,
				"consecutive_panics": consecutivePanics,
				"error":              panicText,
				"instruction":        "Agent loop failed after repeated panics. Decide: reconfigure, restart, or replace agent.",
			}),
			CreatedAt: time.Now(),
		}, []string{managerID})
	}
}

func (am *AgentManager) startControlLoop(parent context.Context) {
	ctrlCtx, cancel := context.WithCancel(parent)
	am.runMu.Lock()
	if am.controlCancel != nil {
		am.controlCancel()
	}
	am.controlCancel = cancel
	am.runMu.Unlock()

	ch := am.bus.Subscribe("agent-manager-controller", events.EventType("opco.spinup_requested"))
	am.runWG.Add(1)
	go func() {
		defer am.runWG.Done()
		for {
			select {
			case <-ctrlCtx.Done():
				return
			case evt, ok := <-ch:
				if !ok {
					return
				}
				am.handleControlEvent(evt)
			}
		}
	}()
}

func (am *AgentManager) handleControlEvent(evt events.Event) {
	switch string(evt.Type) {
	case "opco.spinup_requested":
		var payload struct {
			VerticalID string                 `json:"vertical_id"`
			Mandate    models.MandateDocument `json:"mandate"`
		}
		if err := json.Unmarshal(evt.Payload, &payload); err != nil {
			return
		}
		verticalID := strings.TrimSpace(payload.VerticalID)
		if verticalID == "" {
			verticalID = strings.TrimSpace(evt.VerticalID)
		}
		if verticalID == "" {
			return
		}
		if strings.TrimSpace(payload.Mandate.VerticalID) == "" {
			payload.Mandate.VerticalID = verticalID
		}
		_ = am.SpawnOpCo(verticalID, payload.Mandate)
	}
}

func (am *AgentManager) processEvent(ctx context.Context, agent Agent, evt events.Event) error {
	if !am.markEventInFlight(agent.ID(), evt.ID) {
		return nil
	}
	defer am.unmarkEventInFlight(agent.ID(), evt.ID)
	if am.shouldSkipEvent(agent.ID(), evt.ID) {
		return nil
	}
	if suppress, reason := am.shouldSuppressForBudget(agent.ID(), evt); suppress {
		am.writeReceipt(ctx, evt.ID, agent.ID(), "processed", reason)
		return nil
	}
	if strings.TrimSpace(agent.ID()) == "empire-coordinator" &&
		strings.TrimSpace(string(evt.Type)) == "system.directive" &&
		!directiveRequiresCoordinator(evt) {
		am.writeReceipt(ctx, evt.ID, agent.ID(), "processed", "intercepted simple directive (runtime-handled)")
		return nil
	}
	out, err := agent.OnEvent(ctx, evt)
	if err != nil {
		if isTransientAgentError(err) {
			// Transient lock/contention errors should be retried without poisoning receipts.
			return nil
		}
		agentErr := WrapRuntimeError(
			"agent_on_event_failed",
			"agent-manager",
			"process_event.on_event",
			false,
			err,
			"agent %s failed processing event %s (%s)",
			agent.ID(),
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(string(evt.Type)),
		)
		am.maybeTripAuthCircuitBreaker(agent.ID(), evt.ID, err)
		am.writeReceipt(ctx, evt.ID, agent.ID(), "error", FormatRuntimeError(agentErr))
		return agentErr
	}
	for idx, e := range out {
		if strings.TrimSpace(e.ID) == "" {
			e.ID = deterministicOutputEventID(evt, agent.ID(), idx, e)
		}
		if am.shouldSkipAlreadyPublishedOutput(ctx, e.ID) {
			continue
		}
		if err := am.bus.Publish(ctx, e); err != nil {
			pubErr := WrapRuntimeError(
				"event_publish_failed",
				"agent-manager",
				"process_event.publish_output",
				true,
				err,
				"failed publishing output event id=%s type=%s from agent=%s",
				strings.TrimSpace(e.ID),
				strings.TrimSpace(string(e.Type)),
				agent.ID(),
			)
			am.writeReceipt(ctx, evt.ID, agent.ID(), "error", FormatRuntimeError(pubErr))
			return pubErr
		}
	}
	am.writeReceipt(ctx, evt.ID, agent.ID(), "processed", "")
	return nil
}

func directiveRequiresCoordinator(evt events.Event) bool {
	if strings.TrimSpace(evt.SourceAgent) == "scan-campaign-manager" {
		return true
	}
	if len(evt.Payload) == 0 {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return false
	}
	text := strings.TrimSpace(asString(payload["directive_text"]))
	if text == "" {
		text = strings.TrimSpace(asString(payload["message"]))
	}
	if text == "" {
		return false
	}
	return isComplexDirectiveText(text)
}

func (am *AgentManager) shouldSuppressForBudget(agentID string, evt events.Event) (bool, string) {
	am.mu.RLock()
	cfg, ok := am.agentCfg[agentID]
	tracker := am.budget
	am.mu.RUnlock()
	if !ok || tracker == nil {
		return false, ""
	}
	eventType := strings.ToLower(strings.TrimSpace(string(evt.Type)))
	if strings.HasPrefix(eventType, "budget.") {
		return false, ""
	}
	role := strings.ToLower(strings.TrimSpace(cfg.Role))
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		verticalID = strings.TrimSpace(cfg.VerticalID)
	}

	if tracker.IsEmergency(verticalID) {
		if isEmergencyAllowedFlow(role, eventType) {
			return false, ""
		}
		return true, "suppressed by budget emergency guardrail"
	}
	if tracker.IsThrottle(verticalID) {
		if isGrowthRole(role) {
			return true, "suppressed by budget throttle: growth paused"
		}
		if isProactiveHeartbeat(role, eventType) {
			return true, "suppressed by budget throttle: proactive heartbeat paused"
		}
		if strings.HasPrefix(eventType, "scan.") {
			return true, "suppressed by budget throttle: scan work paused"
		}
	}
	return false, ""
}

func isEmergencyAllowedFlow(role, eventType string) bool {
	if role == "support-agent" {
		return true
	}
	if strings.Contains(eventType, "bug") || strings.Contains(eventType, "incident") || strings.Contains(eventType, "outage") {
		return true
	}
	if strings.HasPrefix(eventType, "human_task.") || strings.HasPrefix(eventType, "runtime.") || strings.HasPrefix(eventType, "ops.") {
		return true
	}
	return false
}

func isGrowthRole(role string) bool {
	switch strings.TrimSpace(strings.ToLower(role)) {
	case "vp-growth", "marketing-agent", "discovery-coordinator",
		"market-research-agent", "trend-research-agent", "scanner-agent", "analysis-agent":
		return true
	default:
		return false
	}
}

func isProactiveHeartbeat(role, eventType string) bool {
	if !strings.HasPrefix(eventType, "heartbeat.") {
		return false
	}
	switch strings.TrimSpace(strings.ToLower(role)) {
	case "opco-ceo", "chief-of-staff", "vp-product", "vp-growth":
		return true
	default:
		return false
	}
}

func (am *AgentManager) markEventInFlight(agentID, eventID string) bool {
	agentID = strings.TrimSpace(agentID)
	eventID = strings.TrimSpace(eventID)
	if agentID == "" || eventID == "" {
		return true
	}
	key := agentID + "|" + eventID
	am.inFlightMu.Lock()
	defer am.inFlightMu.Unlock()
	if _, exists := am.inFlight[key]; exists {
		return false
	}
	am.inFlight[key] = struct{}{}
	return true
}

func (am *AgentManager) unmarkEventInFlight(agentID, eventID string) {
	agentID = strings.TrimSpace(agentID)
	eventID = strings.TrimSpace(eventID)
	if agentID == "" || eventID == "" {
		return
	}
	key := agentID + "|" + eventID
	am.inFlightMu.Lock()
	delete(am.inFlight, key)
	am.inFlightMu.Unlock()
}

func (am *AgentManager) shouldSkipEvent(agentID, eventID string) bool {
	reader, ok := am.store.(EventReceiptReader)
	if !ok || reader == nil {
		return false
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return false
	}
	receipt, found, err := reader.GetEventReceipt(am.runtimeContext(), eventID, agentID)
	if err != nil || !found {
		return false
	}
	status := strings.TrimSpace(receipt.Status)
	return status == "processed" || status == "dead_letter"
}

func isTransientAgentError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "session currently leased") {
		return true
	}
	if strings.Contains(msg, "budget emergency") {
		return true
	}
	return false
}

func isClaudeAuthError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrClaudeAuthRequired) {
		return true
	}
	msg := strings.ToLower(strings.Join(strings.Fields(err.Error()), " "))
	return strings.Contains(msg, "not logged in") ||
		strings.Contains(msg, "please run /login") ||
		strings.Contains(msg, "/login") ||
		strings.Contains(msg, "claude auth required") ||
		(strings.Contains(msg, "claude") && strings.Contains(msg, "auth"))
}

func isClaudeCreditExhaustedError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.Join(strings.Fields(err.Error()), " "))
	return strings.Contains(msg, "you've hit your limit") ||
		strings.Contains(msg, "you have hit your limit") ||
		strings.Contains(msg, "insufficient credit") ||
		strings.Contains(msg, "insufficient credits") ||
		strings.Contains(msg, "credit balance") ||
		strings.Contains(msg, "quota exceeded") ||
		(strings.Contains(msg, "resets") && strings.Contains(msg, "utc") && strings.Contains(msg, "limit"))
}

func (am *AgentManager) maybeTripAuthCircuitBreaker(agentID, eventID string, err error) {
	reason := ""
	eventType := events.EventType("runtime.paused")
	instruction := ""
	switch {
	case isClaudeAuthError(err):
		reason = "claude_auth_required"
		eventType = events.EventType("runtime.auth_required")
		instruction = "Claude authentication is required. Runtime paused to prevent retry storm."
	case isClaudeCreditExhaustedError(err):
		reason = "claude_credit_exhausted"
		instruction = "Claude usage limit reached. Runtime paused globally until credits reset or billing is updated."
	default:
		return
	}
	am.runMu.Lock()
	if am.authBreakerTripped {
		am.runMu.Unlock()
		return
	}
	am.authBreakerTripped = true
	running := am.running
	am.runMu.Unlock()

	PauseRuntimeIngress()
	log.Printf("runtime pause breaker tripped: reason=%s agent=%s event=%s err=%v", reason, agentID, eventID, err)
	payload, _ := json.Marshal(map[string]any{
		"agent_id":     strings.TrimSpace(agentID),
		"event_id":     strings.TrimSpace(eventID),
		"reason":       reason,
		"instruction":  instruction,
		"spec_version": runtimeSpecVersion,
	})
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	_ = am.bus.Publish(am.runtimeContext(), events.Event{
		ID:          uuid.NewString(),
		Type:        eventType,
		SourceAgent: "runtime",
		Payload:     payload,
		CreatedAt:   time.Now(),
	})
	if running {
		_ = am.Shutdown()
	}
}

func (am *AgentManager) isAuthBreakerTripped() bool {
	am.runMu.Lock()
	defer am.runMu.Unlock()
	return am.authBreakerTripped
}

func (am *AgentManager) writeReceipt(ctx context.Context, eventID, agentID, status, errText string) {
	if am.store == nil || eventID == "" || agentID == "" {
		return
	}
	writeCtx := ctx
	if writeCtx == nil {
		writeCtx = context.Background()
	}
	if err := am.store.UpsertEventReceipt(writeCtx, eventID, agentID, status, errText); err != nil {
		// Agent loop contexts are canceled aggressively during teardown; receipts
		// must still persist so pending deliveries do not get stuck indefinitely.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			retryCtx, cancel := context.WithTimeout(context.Background(), receiptWriteTimeout)
			retryErr := am.store.UpsertEventReceipt(retryCtx, eventID, agentID, status, errText)
			cancel()
			if retryErr == nil {
				if status == "error" {
					am.maybeEscalateDeadLetter(context.Background(), eventID, agentID)
				}
				return
			}
			err = retryErr
		}
		log.Printf("receipt write failed event=%s agent=%s status=%s err=%v", eventID, agentID, status, err)
		return
	}

	// Spec v2.0: dead-letter events are escalated to the agent's manager. The manager
	// decides whether to retry, skip, or escalate further.
	if status == "error" {
		am.maybeEscalateDeadLetter(ctx, eventID, agentID)
	}
}

func (am *AgentManager) maybeEscalateDeadLetter(ctx context.Context, eventID, agentID string) {
	reader, ok := am.store.(EventReceiptReader)
	if !ok || reader == nil {
		return
	}
	receipt, found, err := reader.GetEventReceipt(ctx, eventID, agentID)
	if err != nil || !found {
		return
	}
	if strings.TrimSpace(receipt.Status) != "dead_letter" {
		return
	}

	managerID := am.resolveManagerAgentID(agentID)
	if strings.TrimSpace(managerID) == "" || managerID == agentID {
		managerID = "empire-coordinator"
	}
	if managerID == agentID {
		// Prevent infinite self-escalation chains.
		log.Printf("dead-letter escalation suppressed for self-managed agent=%s event=%s", agentID, eventID)
		return
	}

	am.mu.RLock()
	cfg, cfgOK := am.agentCfg[agentID]
	am.mu.RUnlock()
	verticalID := ""
	if cfgOK {
		verticalID = strings.TrimSpace(cfg.VerticalID)
	}

	payload, _ := json.Marshal(map[string]any{
		"event_id":     eventID,
		"agent_id":     agentID,
		"manager_id":   managerID,
		"vertical_id":  verticalID,
		"retry_count":  receipt.RetryCount,
		"error":        strings.TrimSpace(receipt.Error),
		"instruction":  "Event delivery dead-lettered after 3 retries. Decide: retry (requeue), skip, or escalate.",
		"spec_version": runtimeSpecVersion,
	})
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	_ = am.bus.PublishDirect(am.runtimeContext(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("ops.dead_letter_escalation"),
		SourceAgent: "runtime",
		VerticalID:  verticalID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}, []string{managerID})
}

func (am *AgentManager) resolveManagerAgentID(agentID string) string {
	am.mu.RLock()
	cfg, ok := am.agentCfg[agentID]
	am.mu.RUnlock()
	if ok {
		if p := strings.TrimSpace(cfg.ParentAgent); p != "" {
			return p
		}
		if strings.TrimSpace(cfg.Mode) == "operating" && strings.TrimSpace(cfg.VerticalID) != "" && strings.TrimSpace(cfg.Role) != "opco-ceo" {
			return opCoAgentID("opco-ceo", cfg.VerticalID)
		}
	}
	return "empire-coordinator"
}

func (am *AgentManager) hydrateRoutingTables(rules []PersistedRoutingRule) error {
	perVertical := make(map[string]*RoutingTable)
	for _, r := range rules {
		if r.VerticalID == "" {
			continue
		}
		rt := perVertical[r.VerticalID]
		if rt == nil {
			rt = &RoutingTable{VerticalID: r.VerticalID}
			perVertical[r.VerticalID] = rt
		}
		rt.Routes = append(rt.Routes, Route{
			EventPattern: r.EventPattern,
			SubscriberID: r.SubscriberID,
			Status:       r.Status,
		})
		am.setRouteMeta(routeRuleKey(r.VerticalID, r.EventPattern, r.SubscriberID), r)
	}
	for verticalID, rt := range perVertical {
		if err := am.bus.SetRoutingTable(verticalID, rt); err != nil {
			return fmt.Errorf("set routing table for %s: %w", verticalID, err)
		}
	}
	return nil
}

type genericAgent struct {
	id            string
	agentType     string
	subscriptions []events.EventType
}

func newGenericAgent(cfg models.AgentConfig) Agent {
	if cfg.Type == "" {
		cfg.Type = "generic"
	}
	merged := make([]string, 0, len(cfg.Subscriptions))
	merged = append(merged, cfg.Subscriptions...)
	if len(cfg.Config) > 0 {
		var aux struct {
			Subscriptions []string `json:"subscriptions"`
		}
		if err := json.Unmarshal(cfg.Config, &aux); err == nil {
			merged = append(merged, aux.Subscriptions...)
		}
	}

	uniq := make(map[string]struct{})
	subs := make([]events.EventType, 0, len(merged))
	for _, s := range merged {
		if s == "" {
			continue
		}
		if _, ok := uniq[s]; ok {
			continue
		}
		uniq[s] = struct{}{}
		subs = append(subs, events.EventType(s))
	}

	return &genericAgent{
		id:            cfg.ID,
		agentType:     cfg.Type,
		subscriptions: subs,
	}
}

func (a *genericAgent) ID() string                        { return a.id }
func (a *genericAgent) Type() string                      { return a.agentType }
func (a *genericAgent) Subscriptions() []events.EventType { return a.subscriptions }
func (a *genericAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func defaultOpCoRoster(verticalID string) []PersistedAgent {
	mk := func(role, typ, parent string, subs ...string) PersistedAgent {
		return PersistedAgent{
			Config: models.AgentConfig{
				ID:            opCoAgentID(role, verticalID),
				Type:          typ,
				Role:          role,
				Mode:          "operating",
				VerticalID:    verticalID,
				ParentAgent:   parent,
				Subscriptions: subs,
			},
			ParentAgentID:   parent,
			CoordinatorID:   opCoAgentID("opco-ceo", verticalID),
			Status:          "active",
			HiredBy:         "agent-manager",
			TemplateVersion: "2.0.44",
		}
	}

	ceo := opCoAgentID("opco-ceo", verticalID)
	vpProduct := opCoAgentID("vp-product", verticalID)
	vpGrowth := opCoAgentID("vp-growth", verticalID)
	cto := opCoAgentID("cto-agent", verticalID)

	return []PersistedAgent{
		mk("opco-ceo", "operating", "", "opco.spinup_requested", "product_report", "growth_report", "cross_domain_report", "product_escalation", "growth_escalation", "spend_request", "spend.approved", "spend.rejected", "cto.architecture_directive", "founder_input.response", "opco.escalation_response"),
		mk("chief-of-staff", "operating", ceo, "product_report", "growth_report", "feature_deployed", "churn_risk", "build_complete", "prelaunch_ready", "support_critical", "channel_blocked"),
		mk("vp-product", "operating", ceo, "build_complete", "build_blocked", "product_escalation", "support_digest", "support_critical", "build_progress"),
		mk("vp-growth", "operating", ceo, "outreach_digest", "channel_blocked", "user_onboarded", "prelaunch_ready", "spend_needed"),
		mk("cto-agent", "operating", vpProduct),
		mk("pm-agent", "operating", vpProduct),
		mk("support-agent", "operating", vpProduct),
		mk("marketing-agent", "operating", vpGrowth),
		mk("tech-writer", "operating", cto),
		mk("backend-agent", "operating", cto),
		mk("frontend-agent", "operating", cto),
		mk("qa-agent", "operating", cto),
		mk("devops-agent", "operating", cto),
	}
}

func defaultOpCoRoutes(verticalID string) []PersistedRoutingRule {
	ceo := opCoAgentID("opco-ceo", verticalID)
	cto := opCoAgentID("cto-agent", verticalID)
	pm := opCoAgentID("pm-agent", verticalID)
	backend := opCoAgentID("backend-agent", verticalID)
	frontend := opCoAgentID("frontend-agent", verticalID)
	devops := opCoAgentID("devops-agent", verticalID)
	marketing := opCoAgentID("marketing-agent", verticalID)
	support := opCoAgentID("support-agent", verticalID)

	bootstrap := func(pattern, sub string) PersistedRoutingRule {
		return PersistedRoutingRule{
			VerticalID:       verticalID,
			EventPattern:     pattern,
			SubscriberID:     sub,
			InstalledBy:      ceo,
			Reason:           "bootstrap",
			Status:           "active",
			Source:           "bootstrap",
			BootstrapVersion: 1,
		}
	}
	seeded := func(pattern, sub string) PersistedRoutingRule {
		return PersistedRoutingRule{
			VerticalID:       verticalID,
			EventPattern:     pattern,
			SubscriberID:     sub,
			InstalledBy:      ceo,
			Reason:           "seeded",
			Status:           "active",
			Source:           "seeded",
			BootstrapVersion: 1,
		}
	}

	rules := []PersistedRoutingRule{
		bootstrap("product_spec_ready", cto),
		bootstrap("cto.tech_spec_review_requested", opCoAgentID("tech-writer", verticalID)),
		bootstrap("technical_spec_ready", cto),
		bootstrap("technical_spec_ready", backend),
		bootstrap("technical_spec_ready", frontend),
		bootstrap("build_progress", cto),
		bootstrap("build_blocked", cto),
		bootstrap("deploy_requested", cto),
		bootstrap("deploy_requested", devops),
		bootstrap("qa.validation_passed", cto),
		bootstrap("qa.validation_failed", cto),
		bootstrap("devops.infra_change_needed", "holding-devops"),
		bootstrap("bug_reported", cto),
		bootstrap("feature_request", pm),
		bootstrap("feature_request", cto),
		bootstrap("cycle_limit_reached", cto),
		bootstrap("inbound.whatsapp_message", support),
		bootstrap("inbound.email", support),
		bootstrap("feature_deployed", marketing),
		seeded("bug_fix_deployed", support),
	}
	return rules
}

func opCoAgentID(role, verticalID string) string {
	return fmt.Sprintf("%s-%s", role, verticalID)
}

func mergeAgentConfig(base, patch models.AgentConfig) models.AgentConfig {
	out := base
	if patch.ID != "" {
		out.ID = patch.ID
	}
	if patch.Type != "" {
		out.Type = patch.Type
	}
	if patch.Role != "" {
		out.Role = patch.Role
	}
	if patch.Mode != "" {
		out.Mode = patch.Mode
	}
	if patch.VerticalID != "" {
		out.VerticalID = patch.VerticalID
	}
	if patch.ParentAgent != "" {
		out.ParentAgent = patch.ParentAgent
	}
	if len(patch.Subscriptions) > 0 {
		out.Subscriptions = patch.Subscriptions
	}
	if len(patch.Config) > 0 {
		out.Config = patch.Config
	}
	if patch.BudgetEnvelope != 0 {
		out.BudgetEnvelope = patch.BudgetEnvelope
	}
	return out
}

func extractSystemPromptFromConfig(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	v, _ := obj["system_prompt"].(string)
	return strings.TrimSpace(v)
}

func withSystemPrompt(raw json.RawMessage, prompt string) json.RawMessage {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return raw
	}
	obj := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &obj)
	}
	obj["system_prompt"] = prompt
	return mustJSON(obj)
}

func routeRuleKey(verticalID, eventPattern, subscriberID string) string {
	return verticalID + "|" + eventPattern + "|" + subscriberID
}

func (am *AgentManager) getRouteMeta(key string) (PersistedRoutingRule, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	r, ok := am.routeMeta[key]
	return r, ok
}

func (am *AgentManager) setRouteMeta(key string, rule PersistedRoutingRule) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.routeMeta[key] = rule
}
