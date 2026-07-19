package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

// BudgetTracker is a pragmatic Phase-1 guardrail:
// - projects retained spend into threshold and control-plane state
// - emits internal budget.threshold_crossed signals for control-plane handling
//
// Completion accounting is owned by the selected-store completion settlement.
type BudgetTracker struct {
	store          budgetspend.Store
	bus            *runtimebus.EventBus
	cfg            *config.Config
	logger         *RuntimeLogger
	mailbox        runtimetools.MailboxPersistence
	mailboxFrom    string
	thresholds     budgetThresholds
	terminalStates []string

	mu        sync.Mutex
	lastState map[string]string // key(scope|entity_id) => ok|warning|throttle|emergency
}

type SpendRecord = budgetspend.SpendRecord

type budgetThresholds struct {
	Enabled   bool
	Warning   float64
	Throttle  float64
	Emergency float64
}

func NewBudgetTracker(store budgetspend.Store, bus *runtimebus.EventBus, cfg *config.Config, mailbox runtimetools.MailboxPersistence, logger *RuntimeLogger, source semanticview.Source) *BudgetTracker {
	var terminalStates []string
	if source != nil {
		terminalStates = source.FlowTerminalStages("")
	}
	return &BudgetTracker{
		store:          store,
		bus:            bus,
		cfg:            cfg,
		logger:         logger,
		mailbox:        mailbox,
		mailboxFrom:    "runtime",
		thresholds:     budgetThresholdsFromSource(source),
		terminalStates: normalizeBudgetStateList(terminalStates),
		lastState:      make(map[string]string),
	}
}

func (t *BudgetTracker) CurrentState(scope string, entityID string) string {
	if t == nil {
		return "ok"
	}
	scope = strings.TrimSpace(scope)
	entityID = strings.TrimSpace(entityID)
	key := scope + "|" + entityID
	t.mu.Lock()
	defer t.mu.Unlock()
	state := strings.TrimSpace(t.lastState[key])
	if state == "" {
		state = "ok"
	}
	return state
}

func (t *BudgetTracker) IsEntityEmergency(entityID string) bool {
	entityID = strings.TrimSpace(entityID)
	if t == nil {
		return false
	}
	if t.CurrentState("system", "") == "emergency" {
		return true
	}
	if entityID != "" && t.CurrentState("entity", entityID) == "emergency" {
		return true
	}
	return false
}

func (t *BudgetTracker) IsEmergency(entityID string) bool {
	return t.IsEntityEmergency(entityID)
}

func (t *BudgetTracker) IsEntityThrottle(entityID string) bool {
	entityID = strings.TrimSpace(entityID)
	if t == nil {
		return false
	}
	// Emergency implies throttle semantics too (harder).
	if t.IsEntityEmergency(entityID) {
		return true
	}
	if t.CurrentState("system", "") == "throttle" {
		return true
	}
	if entityID != "" && t.CurrentState("entity", entityID) == "throttle" {
		return true
	}
	return false
}

func (t *BudgetTracker) IsThrottle(entityID string) bool {
	return t.IsEntityThrottle(entityID)
}

// ProjectRecoveryBudgetState reconstructs runtime budget state from retained
// selected-store spend without relying on one ambient run identity. Process
// scopes are projected before the store enumerates active run-scoped entities.
func (t *BudgetTracker) ProjectRecoveryBudgetState(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if t.store == nil {
		return fmt.Errorf("budget recovery projection store is required")
	}
	if t.bus == nil || t.cfg == nil {
		return fmt.Errorf("budget recovery projection runtime dependencies are required")
	}

	budget := t.cfg.Budget()
	if budget.SystemMonthlyCap > 0 {
		if err := t.evaluateScope(ctx, string(budgetspend.ScopeSystem), "", budget.SystemMonthlyCap); err != nil {
			return fmt.Errorf("project recovered system budget state: %w", err)
		}
	}
	if budget.GlobalMonthlyCap > 0 {
		if err := t.evaluateScope(ctx, string(budgetspend.ScopeGlobal), "", budget.GlobalMonthlyCap); err != nil {
			return fmt.Errorf("project recovered global budget state: %w", err)
		}
	}
	if budget.PerEntityMonthlyCap <= 0 {
		return nil
	}

	targets, err := t.store.ListBudgetProjectionTargets(ctx, t.TerminalInstanceStates())
	if err != nil {
		return fmt.Errorf("list recovered budget projection targets: %w", err)
	}
	projected := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		entityID := strings.TrimSpace(target.EntityID)
		if entityID == "" {
			return fmt.Errorf("budget recovery projection target for run %q has no entity_id", strings.TrimSpace(target.RunID))
		}
		if _, ok := projected[entityID]; ok {
			continue
		}
		projected[entityID] = struct{}{}
		if err := t.evaluateScope(ctx, string(budgetspend.ScopeEntity), entityID, budget.PerEntityMonthlyCap); err != nil {
			return fmt.Errorf("project recovered entity budget state for run %s entity %s: %w", strings.TrimSpace(target.RunID), entityID, err)
		}
	}
	return nil
}

func (t *BudgetTracker) TerminalInstanceStates() []string {
	if t == nil {
		return nil
	}
	out := make([]string, len(t.terminalStates))
	copy(out, t.terminalStates)
	return out
}

func (t *BudgetTracker) RecordSpend(ctx context.Context, rec SpendRecord) error {
	if t == nil || t.store == nil {
		return nil
	}
	if rec.RecordedAt.IsZero() {
		rec.RecordedAt = time.Now()
	}
	rec.NormalizeEntityID()
	rec.FlowInstance = strings.TrimSpace(rec.FlowInstance)
	rec.AgentID = strings.TrimSpace(rec.AgentID)
	rec.Model = strings.TrimSpace(rec.Model)
	rec.ModelAlias = strings.TrimSpace(rec.ModelAlias)
	rec.BackendProfile = strings.TrimSpace(rec.BackendProfile)
	rec.Provider = strings.TrimSpace(rec.Provider)
	rec.Transport = strings.TrimSpace(rec.Transport)
	rec.ResolvedModel = strings.TrimSpace(rec.ResolvedModel)
	if rec.ModelAlias == "" {
		rec.ModelAlias = "unknown"
	}
	if rec.BackendProfile == "" {
		rec.BackendProfile = "unknown"
	}
	if rec.Provider == "" {
		rec.Provider = "unknown"
	}
	if rec.Transport == "" {
		rec.Transport = "unknown"
	}
	if rec.ResolvedModel == "" {
		rec.ResolvedModel = rec.Model
	}
	rec.InvocationType = strings.TrimSpace(strings.ToLower(rec.InvocationType))
	rec.UsageAccounting = strings.TrimSpace(strings.ToLower(rec.UsageAccounting))
	if !rec.ExecutionMode.Valid() {
		return fmt.Errorf("spend execution_mode must be live or mock")
	}
	if rec.FlowInstance == "" {
		return fmt.Errorf("spend flow_instance is required")
	}
	if rec.AgentID == "" {
		return fmt.Errorf("spend agent_id is required")
	}
	if rec.Model == "" {
		return fmt.Errorf("spend model is required")
	}
	if rec.InvocationType == "" {
		return fmt.Errorf("spend invocation_type is required")
	}
	switch rec.UsageAccounting {
	case string(llm.BudgetUsageExact), string(llm.BudgetUsageEstimated):
	default:
		return fmt.Errorf("spend usage_accounting must be exact or estimated")
	}
	if rec.InputTokens < 0 {
		rec.InputTokens = 0
	}
	if rec.OutputTokens < 0 {
		rec.OutputTokens = 0
	}
	if rec.CostUSD < 0 {
		rec.CostUSD = 0
	}

	if err := t.store.RecordSpend(ctx, rec); err != nil {
		return fmt.Errorf("record spend: %w", err)
	}

	// Best-effort guardrail evaluation.
	if err := t.evaluateAndEmit(ctx, rec.EffectiveEntityID()); err != nil {
		if t.logger != nil {
			handleRuntimeLogPersistenceError("budget", "evaluate_on_spend_failed", t.logger.Warn(ctx, "budget", "evaluate_on_spend_failed", map[string]any{"entity_id": rec.EffectiveEntityID()}, err))
		}
	}
	return nil
}

func (t *BudgetTracker) ProjectCommittedCompletionSpend(ctx context.Context, projection runtimeeffects.CompletionSpendProjection) {
	if t == nil || t.store == nil {
		return
	}
	entityID := strings.TrimSpace(projection.EntityID)
	if err := t.evaluateAndEmit(ctx, entityID); err != nil && t.logger != nil {
		handleRuntimeLogPersistenceError("budget", "project_completion_spend_failed", t.logger.Warn(ctx, "budget", "project_completion_spend_failed", map[string]any{
			"attempt_id": strings.TrimSpace(projection.AttemptID),
			"entity_id":  entityID,
		}, err))
	}
}

func (t *BudgetTracker) evaluateAndEmit(ctx context.Context, entityID string) error {
	if t == nil || t.store == nil || t.bus == nil || t.cfg == nil {
		return nil
	}
	entityID = strings.TrimSpace(entityID)

	// System cap applies to all spend across all entities + global scope.
	if t.cfg.Budget().SystemMonthlyCap > 0 {
		if err := t.evaluateScope(ctx, "system", "", t.cfg.Budget().SystemMonthlyCap); err != nil {
			return err
		}
	}

	// Per-entity cap.
	if entityID != "" && t.cfg.Budget().PerEntityMonthlyCap > 0 {
		if err := t.evaluateScope(ctx, "entity", entityID, t.cfg.Budget().PerEntityMonthlyCap); err != nil {
			return err
		}
	}

	// Global cap (spend rows with NULL entity scope).
	if entityID == "" && t.cfg.Budget().GlobalMonthlyCap > 0 {
		if err := t.evaluateScope(ctx, "global", "", t.cfg.Budget().GlobalMonthlyCap); err != nil {
			return err
		}
	}
	return nil
}

func (t *BudgetTracker) evaluateScope(ctx context.Context, scope string, entityID string, capUSD int) error {
	if capUSD <= 0 || !t.thresholds.Enabled {
		return nil
	}
	now := time.Now().UTC()
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	spent, err := t.store.SumSpendUSD(ctx, budgetspend.SpendQuery{
		Scope:    budgetScope(scope, entityID),
		EntityID: entityID,
		Since:    start,
		LiveOnly: true,
	})
	if err != nil {
		return err
	}

	ratio := spent / float64(capUSD)
	thresholds := t.thresholds
	state := "ok"
	switch {
	case ratio >= thresholds.Emergency:
		state = "emergency"
	case ratio >= thresholds.Throttle:
		state = "throttle"
	case ratio >= thresholds.Warning:
		state = "warning"
	default:
		state = "ok"
	}

	key := scope + "|" + entityID
	t.mu.Lock()
	prev := t.lastState[key]
	// Don't spam. Only emit on transitions, except resumed should only emit when
	// we previously had a non-ok state.
	shouldEmit := prev != state && !(state == "ok" && prev == "")
	if state == "ok" && (prev == "" || prev == "ok") {
		shouldEmit = false
	}
	if shouldEmit {
		t.lastState[key] = state
	} else if prev == "" {
		t.lastState[key] = state
	}
	t.mu.Unlock()
	if !shouldEmit {
		return nil
	}

	payload := map[string]any{
		"level":         state,
		"current_spend": spent,
		"budget_cap":    float64(capUSD),
		"percentage":    ratio * 100.0,
		"period":        start.Format("2006-01"),
		"timestamp":     now.Format(time.RFC3339),
	}
	evtID := uuid.NewString()
	evt, err := events.NewRuntimeDiagnosticEvent(events.RuntimeEventInput{Facts: events.EventFacts{
		ID: evtID, Type: events.EventType("platform.budget_threshold_crossed"),
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"}, Payload: mustJSON(payload),
		CreatedAt: time.Now(), ExecutionMode: executionmode.Live,
	}})
	if err != nil {
		return err
	}
	if err := t.bus.Publish(ctx, evt); err != nil {
		return err
	}

	// Spec v2.0: budget.emergency must also create a critical mailbox item.
	if state == "emergency" && t.mailbox != nil {
		summary := fmt.Sprintf("Budget emergency: scope=%s spent=$%.2f cap=$%.2f (%.0f%%)",
			scope, spent, float64(capUSD), ratio*100.0)
		// Best-effort: avoid breaking spend path if mailbox insert fails.
		if _, err := t.mailbox.InsertMailboxItem(ctx, runtimetools.MailboxItem{
			EventID:   evtID,
			EntityID:  entityID,
			FromAgent: t.mailboxFrom,
			Type:      "alert",
			Priority:  "critical",
			Status:    "pending",
			Context:   mustJSON(payload),
			Summary:   summary,
		}); err != nil {
			if t.logger != nil {
				handleRuntimeLogPersistenceError("budget", "budget_emergency_mailbox_insert_failed", t.logger.Warn(ctx, "budget", "budget_emergency_mailbox_insert_failed", map[string]any{
					"event_id":  strings.TrimSpace(evtID),
					"entity_id": strings.TrimSpace(entityID),
					"scope":     strings.TrimSpace(scope),
				}, err))
			} else {
				processWarn("budget", "failed to insert emergency budget mailbox item entity=%s scope=%s: %v", entityID, scope, err)
			}
		}
	}
	return nil
}

func budgetScope(scope string, entityID string) budgetspend.Scope {
	switch strings.TrimSpace(scope) {
	case string(budgetspend.ScopeSystem):
		return budgetspend.ScopeSystem
	case string(budgetspend.ScopeEntity):
		return budgetspend.ScopeEntity
	case string(budgetspend.ScopeGlobal):
		return budgetspend.ScopeGlobal
	default:
		if strings.TrimSpace(entityID) != "" {
			return budgetspend.ScopeEntity
		}
		return budgetspend.ScopeGlobal
	}
}

func budgetThresholdsFromSource(source semanticview.Source) budgetThresholds {
	warning, okWarning := percentPolicyValue(source, "budget_warning_percent")
	throttle, okThrottle := percentPolicyValue(source, "budget_throttle_percent")
	emergency, okEmergency := percentPolicyValue(source, "budget_emergency_percent")
	if !okWarning || !okThrottle || !okEmergency {
		return budgetThresholds{}
	}
	return budgetThresholds{
		Enabled:   true,
		Warning:   warning,
		Throttle:  throttle,
		Emergency: emergency,
	}
}

func percentPolicyValue(source semanticview.Source, key string) (float64, bool) {
	value, ok := semanticview.PolicyValueForFlow(source, "", key)
	if !ok {
		return 0, false
	}
	switch typed := value.Value.(type) {
	case int:
		return normalizePercentValue(float64(typed)), true
	case int64:
		return normalizePercentValue(float64(typed)), true
	case float64:
		return normalizePercentValue(typed), true
	case float32:
		return normalizePercentValue(float64(typed)), true
	default:
		return 0, false
	}
}

func normalizePercentValue(value float64) float64 {
	if value <= 0 {
		return 0
	}
	if value > 1.0 {
		return value / 100.0
	}
	return value
}

func normalizeBudgetStateList(states []string) []string {
	if len(states) == 0 {
		return nil
	}
	out := make([]string, 0, len(states))
	seen := make(map[string]struct{}, len(states))
	for _, state := range states {
		state = strings.TrimSpace(state)
		if state == "" {
			continue
		}
		if _, ok := seen[state]; ok {
			continue
		}
		seen[state] = struct{}{}
		out = append(out, state)
	}
	return out
}
