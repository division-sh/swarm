package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	models "empireai/internal/runtime/core/actors"
	llm "empireai/internal/runtime/llm"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/google/uuid"
	"github.com/lib/pq"
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
	db          *sql.DB
	bus         *runtimebus.EventBus
	cfg         *config.Config
	mailbox     runtimetools.MailboxPersistence
	mailboxFrom string

	mu        sync.Mutex
	lastState map[string]string // key(scope|entity_id) => ok|warning|throttle|emergency
	scopeMu   sync.Map          // key(scope) => *sync.Mutex
}

var activeInstanceStates = []string{"approved", "building", "pre_launch", "launched", "active", "expanding"}

func ActiveInstanceStates() []string {
	out := make([]string, len(activeInstanceStates))
	copy(out, activeInstanceStates)
	return out
}

func NewBudgetTracker(db *sql.DB, bus *runtimebus.EventBus, cfg *config.Config, mailbox runtimetools.MailboxPersistence) *BudgetTracker {
	return &BudgetTracker{
		db:          db,
		bus:         bus,
		cfg:         cfg,
		mailbox:     mailbox,
		mailboxFrom: "runtime",
		lastState:   make(map[string]string),
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
	_ = t.evaluateAndEmit(ctx, "")

	// Evaluate each active entity with any spend/metrics.
	rows, err := t.db.QueryContext(ctx, `
		SELECT instance_id::text
		FROM workflow_instances
			WHERE COALESCE(metadata->>'instance_kind', '') = 'entity'
		  AND current_state = ANY($1::text[])
		ORDER BY created_at ASC
	`, pq.Array(activeInstanceStates))
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return
		}
		_ = t.evaluateAndEmit(ctx, strings.TrimSpace(id))
	}
}

type SpendRecord struct {
	EntityID     string
	Category     string
	AmountCents  int
	Currency     string
	Description  string
	ApprovedBy   string
	Source       string // exact|estimated
	Meta         any
	RecordedBy   string
	RecordedAt   time.Time
	RequestingID string
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
	if strings.TrimSpace(rec.Category) == "" {
		return fmt.Errorf("spend category is required")
	}
	if rec.Currency == "" {
		rec.Currency = "USD"
	}
	if rec.RecordedAt.IsZero() {
		rec.RecordedAt = time.Now()
	}
	rec.Source = strings.TrimSpace(strings.ToLower(rec.Source))
	if rec.Source == "" {
		rec.Source = "exact"
	}
	rec.NormalizeEntityID()

	metaJSON := []byte("null")
	if rec.Meta != nil {
		if b, err := json.Marshal(rec.Meta); err == nil && len(b) > 0 {
			metaJSON = b
		}
	}

	const q = `
		INSERT INTO spend_ledger (
			entity_id, agent_id, category, amount_cents, currency, description, approved_by, source, metadata, created_at
		) VALUES (
			NULLIF($1,'')::uuid, NULLIF($2,''), $3, $4, $5, NULLIF($6,''), NULLIF($7,''), $8, $9::jsonb, $10
		)
	`
	if _, err := t.db.ExecContext(ctx, q,
		rec.EffectiveEntityID(),
		strings.TrimSpace(rec.RecordedBy),
		strings.TrimSpace(rec.Category),
		rec.AmountCents,
		strings.TrimSpace(rec.Currency),
		strings.TrimSpace(rec.Description),
		strings.TrimSpace(rec.ApprovedBy),
		rec.Source,
		metaJSON,
		rec.RecordedAt,
	); err != nil {
		return fmt.Errorf("insert spend_ledger: %w", err)
	}

	// Best-effort guardrail evaluation.
	_ = t.evaluateAndEmit(ctx, rec.EffectiveEntityID())
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

	amount := t.estimateLLMCostCents(usage.Model, usage.InputTokens, usage.OutputTokens)
	source := "estimated"
	if exact {
		source = "exact"
	}

	desc := fmt.Sprintf("llm usage agent=%s runtime=%s model=%s in=%d out=%d",
		agentID, runtimeMode, usage.Model, usage.InputTokens, usage.OutputTokens)
	return t.RecordSpend(ctx, SpendRecord{
		EntityID:    entityID,
		Category:    "llm",
		AmountCents: amount,
		Currency:    "USD",
		Description: desc,
		Source:      source,
		RecordedBy:  agentID,
		Meta: mergeMeta(meta, map[string]any{
			"agent_id":      agentID,
			"runtime_mode":  runtimeMode,
			"model":         usage.Model,
			"input_tokens":  usage.InputTokens,
			"output_tokens": usage.OutputTokens,
			"exact":         exact,
		}),
	})
}

func mergeMeta(a any, b map[string]any) any {
	if a == nil {
		return b
	}
	// If a is already a map, merge into it; otherwise, wrap.
	if m, ok := a.(map[string]any); ok {
		out := make(map[string]any, len(m)+len(b))
		for k, v := range m {
			out[k] = v
		}
		for k, v := range b {
			out[k] = v
		}
		return out
	}
	return map[string]any{"meta": a, "extra": b}
}

func (t *BudgetTracker) estimateLLMCostCents(model string, inputTokens, outputTokens int) int {
	// Rough defaults; intended to be "good enough" until provider usage/cost is plumbed precisely.
	// Prices are treated as USD per 1M tokens.
	tier := modelTier(model)

	inCentsPerM, outCentsPerM := 0, 0
	switch tier {
	case "haiku":
		inCentsPerM = 80
		outCentsPerM = 400
	case "opus":
		inCentsPerM = 1500
		outCentsPerM = 7500
	default: // sonnet-ish default
		inCentsPerM = 300
		outCentsPerM = 1500
	}
	if inputTokens < 0 {
		inputTokens = 0
	}
	if outputTokens < 0 {
		outputTokens = 0
	}
	return int(float64(inputTokens)/1_000_000.0*float64(inCentsPerM) + float64(outputTokens)/1_000_000.0*float64(outCentsPerM))
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

func (t *BudgetTracker) evaluateScope(ctx context.Context, scope string, entityID string, capCents int) error {
	if capCents <= 0 {
		return nil
	}
	now := time.Now().UTC()
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	var spent int
	var err error
	switch {
	case scope == "system":
		err = t.db.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(amount_cents), 0)
			FROM spend_ledger
			WHERE created_at >= $1
		`, start).Scan(&spent)
	case entityID == "":
		err = t.db.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(amount_cents), 0)
			FROM spend_ledger
			WHERE entity_id IS NULL
			  AND created_at >= $1
		`, start).Scan(&spent)
	default:
		err = t.db.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(amount_cents), 0)
			FROM spend_ledger
			WHERE entity_id = $1::uuid
			  AND created_at >= $2
		`, entityID, start).Scan(&spent)
	}
	if err != nil {
		return err
	}

	ratio := float64(spent) / float64(capCents)
	state := "ok"
	switch {
	case ratio >= 1.0:
		state = "emergency"
	case ratio >= 0.9:
		state = "throttle"
	case ratio >= 0.8:
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
		"scope":        scope,
		"entity_id":    entityID,
		"cap_cents":    capCents,
		"spent_cents":  spent,
		"month_start":  start.Format(time.RFC3339),
		"ratio":        ratio,
		"state":        state,
		"evaluated_at": now.Format(time.RFC3339),
	}
	evtID := uuid.NewString()
	evt := (events.Event{
		ID:          evtID,
		Type:        events.EventType("budget.threshold_crossed"),
		SourceAgent: "runtime",
		Payload: mustJSON(map[string]any{
			"scope":           scope,
			"entity_id":       entityID,
			"cap_cents":       capCents,
			"spent_cents":     spent,
			"month_start":     start.Format(time.RFC3339),
			"ratio":           ratio,
			"state":           state,
			"next_event_type": string(budgetEventTypeForState(state)),
			"evaluated_at":    now.Format(time.RFC3339),
		}),
		CreatedAt: time.Now(),
	}).WithEntityID(entityID)
	if err := t.bus.Publish(ctx, evt); err != nil {
		return err
	}

	// Spec v2.0: budget.emergency must also create a critical mailbox item.
	if state == "emergency" && t.mailbox != nil {
		summary := fmt.Sprintf("Budget emergency: scope=%s spent=%d cap=%d (%.0f%%)",
			scope, spent, capCents, ratio*100.0)
		// Best-effort: avoid breaking spend path if mailbox insert fails.
		if _, err := t.mailbox.InsertMailboxItem(ctx, runtimetools.MailboxItem{
			EventID:   evtID,
			EntityID:  entityID,
			FromAgent: t.mailboxFrom,
			Type:      "budget_increase",
			Priority:  "critical",
			Status:    "pending",
			Context:   mustJSON(payload),
			Summary:   summary,
		}); err != nil {
			runtimeWarn("budget", "failed to insert emergency budget mailbox item entity=%s scope=%s: %v", entityID, scope, err)
		}
	}
	return nil
}

func budgetEventTypeForState(state string) events.EventType {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "emergency":
		return events.EventType("budget.emergency")
	case "throttle":
		return events.EventType("budget.throttle")
	case "warning":
		return events.EventType("budget.warning")
	default:
		return events.EventType("budget.resumed")
	}
}
