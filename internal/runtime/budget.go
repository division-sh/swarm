package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"swarm/internal/config"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	models "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
)

// budgetExecutionScopeKey controls intra-process serialization for LLM budget
// preflight/recording. Entity-scoped agents keep per-entity locking.
// Global/no-entity agents get per-agent scope keys so sharded work can execute
// concurrently instead of funneling through one global lock.
func budgetExecutionScopeKey(actor models.AgentConfig) string {
	entityID := actor.EffectiveEntityID()
	if entityID != "" {
		return entityID
	}
	if agentID := strings.TrimSpace(actor.ID); agentID != "" {
		return "__agent__:" + agentID
	}
	return ""
}

// BudgetTracker is a pragmatic Phase-1 guardrail:
// - records spend in spend_ledger (exact for API usage, estimated for CLI usage)
// - emits internal budget.threshold_crossed signals for control-plane handling
//
// It is not accounting-grade. The intent is runaway-spend prevention.
type BudgetTracker struct {
	db             *sql.DB
	bus            *runtimebus.EventBus
	cfg            *config.Config
	logger         *RuntimeLogger
	mailbox        runtimetools.MailboxPersistence
	mailboxFrom    string
	thresholds     budgetThresholds
	terminalStates []string

	mu        sync.Mutex
	lastState map[string]string // key(scope|entity_id) => ok|warning|throttle|emergency
	scopeMu   sync.Map          // key(scope) => *sync.Mutex
}

type budgetThresholds struct {
	Enabled   bool
	Warning   float64
	Throttle  float64
	Emergency float64
}

func NewBudgetTracker(db *sql.DB, bus *runtimebus.EventBus, cfg *config.Config, mailbox runtimetools.MailboxPersistence, logger *RuntimeLogger, source semanticview.Source) *BudgetTracker {
	return &BudgetTracker{
		db:             db,
		bus:            bus,
		cfg:            cfg,
		logger:         logger,
		mailbox:        mailbox,
		mailboxFrom:    "runtime",
		thresholds:     budgetThresholdsFromSource(source),
		terminalStates: normalizeBudgetStateList(source.WorkflowTerminalStages()),
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

func (t *BudgetTracker) RecordEntityLLMUsage(ctx context.Context, entityID string, agentID string, runtimeMode string, usage llm.UsageTokens, exact bool, meta any) error {
	return t.RecordLLMUsage(ctx, entityID, agentID, runtimeMode, usage, exact, meta)
}

// LockExecutionScope serializes budget-critical LLM execution checks/records
// per entity scope within the current process.
func (t *BudgetTracker) LockExecutionScope(entityID string) func() {
	if t == nil {
		return func() {}
	}
	scopeKey := strings.TrimSpace(entityID)
	if scopeKey == "" {
		scopeKey = "__system__"
	}
	muAny, _ := t.scopeMu.LoadOrStore(scopeKey, &sync.Mutex{})
	mu, _ := muAny.(*sync.Mutex)
	if mu == nil {
		return func() {}
	}
	mu.Lock()
	return mu.Unlock
}

// EvaluateAll periodically re-evaluates budget state to ensure month-boundary
// "resume" transitions are emitted even when spend is paused (spec v2.0).
func (t *BudgetTracker) EvaluateAll(ctx context.Context) {
	if t == nil || t.db == nil {
		return
	}
	// Evaluate system-wide budget.
	if err := t.evaluateAndEmit(ctx, ""); err != nil {
		if t.logger != nil {
			handleRuntimeLogPersistenceError("budget", "evaluate_system_failed", t.logger.Warn(ctx, "budget", "evaluate_system_failed", nil, err))
		}
	}

	// Evaluate each active entity with any spend/metrics.
	rows, err := t.db.QueryContext(ctx, `
		SELECT entity_id::text
		FROM entity_state
		WHERE NOT (current_state = ANY($1::text[]))
		ORDER BY created_at ASC
	`, pq.Array(t.TerminalInstanceStates()))
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return
		}
		if err := t.evaluateAndEmit(ctx, strings.TrimSpace(id)); err != nil {
			if t.logger != nil {
				handleRuntimeLogPersistenceError("budget", "evaluate_entity_failed", t.logger.Warn(ctx, "budget", "evaluate_entity_failed", map[string]any{"entity_id": strings.TrimSpace(id)}, err))
			}
		}
	}
}

func (t *BudgetTracker) TerminalInstanceStates() []string {
	if t == nil {
		return nil
	}
	out := make([]string, len(t.terminalStates))
	copy(out, t.terminalStates)
	return out
}

type SpendRecord struct {
	EntityID       string
	FlowInstance   string
	AgentID        string
	Model          string
	InputTokens    int
	OutputTokens   int
	CostUSD        float64
	InvocationType string
	RecordedAt     time.Time
}

func (r SpendRecord) EffectiveEntityID() string {
	return strings.TrimSpace(r.EntityID)
}

func (r *SpendRecord) NormalizeEntityID() {
	if r == nil {
		return
	}
	entityID := r.EffectiveEntityID()
	r.EntityID = entityID
}

func (t *BudgetTracker) RecordSpend(ctx context.Context, rec SpendRecord) error {
	if t == nil || t.db == nil {
		return nil
	}
	if rec.RecordedAt.IsZero() {
		rec.RecordedAt = time.Now()
	}
	rec.NormalizeEntityID()
	rec.FlowInstance = strings.TrimSpace(rec.FlowInstance)
	rec.AgentID = strings.TrimSpace(rec.AgentID)
	rec.Model = strings.TrimSpace(rec.Model)
	rec.InvocationType = strings.TrimSpace(strings.ToLower(rec.InvocationType))
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
	if rec.InputTokens < 0 {
		rec.InputTokens = 0
	}
	if rec.OutputTokens < 0 {
		rec.OutputTokens = 0
	}
	if rec.CostUSD < 0 {
		rec.CostUSD = 0
	}

	const q = `
		INSERT INTO spend_ledger (
			entity_id, flow_instance, agent_id, model, input_tokens, output_tokens, cost_usd, invocation_type, created_at
		) VALUES (
			NULLIF($1,'')::uuid, $2, $3, $4, $5, $6, $7, $8, $9
		)
	`
	if _, err := t.db.ExecContext(ctx, q,
		rec.EffectiveEntityID(),
		rec.FlowInstance,
		rec.AgentID,
		rec.Model,
		rec.InputTokens,
		rec.OutputTokens,
		rec.CostUSD,
		rec.InvocationType,
		rec.RecordedAt,
	); err != nil {
		return fmt.Errorf("insert spend_ledger: %w", err)
	}

	// Best-effort guardrail evaluation.
	if err := t.evaluateAndEmit(ctx, rec.EffectiveEntityID()); err != nil {
		if t.logger != nil {
			handleRuntimeLogPersistenceError("budget", "evaluate_on_spend_failed", t.logger.Warn(ctx, "budget", "evaluate_on_spend_failed", map[string]any{"entity_id": rec.EffectiveEntityID()}, err))
		}
	}
	return nil
}

func (t *BudgetTracker) RecordLLMUsage(ctx context.Context, entityID string, agentID string, runtimeMode string, usage llm.UsageTokens, exact bool, meta any) error {
	if t == nil || t.db == nil {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	agentID = strings.TrimSpace(agentID)
	runtimeMode = strings.TrimSpace(runtimeMode)
	usage.Model = strings.TrimSpace(usage.Model)
	if usage.Model == "" {
		usage.Model = "unknown"
	}

	flowInstance, err := t.resolveSpendFlowInstance(ctx, entityID, meta)
	if err != nil {
		return err
	}
	return t.RecordSpend(ctx, SpendRecord{
		EntityID:       entityID,
		FlowInstance:   flowInstance,
		AgentID:        agentID,
		Model:          usage.Model,
		InputTokens:    usage.InputTokens,
		OutputTokens:   usage.OutputTokens,
		CostUSD:        t.estimateLLMCostUSD(usage.Model, usage.InputTokens, usage.OutputTokens),
		InvocationType: runtimeMode,
	})
}

func (t *BudgetTracker) resolveSpendFlowInstance(ctx context.Context, entityID string, meta any) (string, error) {
	if values, ok := meta.(map[string]any); ok {
		if flowInstance := strings.TrimSpace(asString(values["flow_instance"])); flowInstance != "" {
			return flowInstance, nil
		}
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return "global", nil
	}
	var flowInstance string
	if err := t.db.QueryRowContext(ctx, `
		SELECT COALESCE(flow_instance, '')
		FROM entity_state
		WHERE entity_id = $1::uuid
	`, entityID).Scan(&flowInstance); err != nil {
		return "", fmt.Errorf("resolve spend flow_instance for entity %s: %w", entityID, err)
	}
	flowInstance = strings.TrimSpace(flowInstance)
	if flowInstance == "" {
		return "", fmt.Errorf("resolve spend flow_instance for entity %s: empty flow_instance", entityID)
	}
	return flowInstance, nil
}

func (t *BudgetTracker) estimateLLMCostUSD(model string, inputTokens, outputTokens int) float64 {
	// Rough defaults; intended to be "good enough" until provider usage/cost is plumbed precisely.
	// Prices are treated as USD per 1M tokens.
	tier := modelTier(model)

	inUSDPerM, outUSDPerM := 0.0, 0.0
	switch tier {
	case "haiku":
		inUSDPerM = 0.80
		outUSDPerM = 4.00
	case "opus":
		inUSDPerM = 15.00
		outUSDPerM = 75.00
	default: // sonnet-ish default
		inUSDPerM = 3.00
		outUSDPerM = 15.00
	}
	if inputTokens < 0 {
		inputTokens = 0
	}
	if outputTokens < 0 {
		outputTokens = 0
	}
	return float64(inputTokens)/1_000_000.0*inUSDPerM + float64(outputTokens)/1_000_000.0*outUSDPerM
}

func modelTier(model string) string {
	m := strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.Contains(m, "haiku"):
		return "haiku"
	case strings.Contains(m, "opus"):
		return "opus"
	case strings.Contains(m, "sonnet"):
		return "sonnet"
	default:
		return "sonnet"
	}
}

func (t *BudgetTracker) evaluateAndEmit(ctx context.Context, entityID string) error {
	if t == nil || t.db == nil || t.bus == nil || t.cfg == nil {
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

	var spent float64
	var err error
	switch {
	case scope == "system":
		err = t.db.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE created_at >= $1
		`, start).Scan(&spent)
	case entityID == "":
		err = t.db.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE entity_id IS NULL
			  AND created_at >= $1
		`, start).Scan(&spent)
	default:
		err = t.db.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE entity_id = $1::uuid
			  AND created_at >= $2
		`, entityID, start).Scan(&spent)
	}
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
	evt := (events.Event{
		ID:          evtID,
		Type:        events.EventType("platform.budget_threshold_crossed"),
		SourceAgent: "runtime",
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	})
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
