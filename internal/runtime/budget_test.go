package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"github.com/DATA-DOG/go-sqlmock"
)

type mailboxCapture struct {
	calls int
	last  MailboxItem
}

func (m *mailboxCapture) InsertMailboxItem(_ context.Context, item MailboxItem) (string, error) {
	m.calls++
	m.last = item
	if item.ID != "" {
		return item.ID, nil
	}
	return "mail-1", nil
}
func (m *mailboxCapture) ListMailboxItems(context.Context, string, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *mailboxCapture) CountMailboxItems(context.Context, string) (int, error) { return 0, nil }
func (m *mailboxCapture) GetMailboxItem(context.Context, string) (MailboxItem, error) {
	return MailboxItem{}, nil
}
func (m *mailboxCapture) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}
func (m *mailboxCapture) ExpireMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *mailboxCapture) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *mailboxCapture) MarkMailboxItemNotified(context.Context, string) error { return nil }

func TestBudgetTracker_TransitionsAndEmergencyMailbox(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	bus := NewEventBus(InMemoryEventStore{})
	// Subscribe watcher using wildcard so budget broadcast can resolve recipients.
	ch := bus.Subscribe("watcher", events.EventType("budget.*"))

	mb := &mailboxCapture{}
	cfg := &config.Config{
		Budget: config.BudgetConfig{
			PortfolioMonthlyCap: 1000,
		},
	}
	bt := NewBudgetTracker(db, bus, cfg, mb)

	ctx := context.Background()
	cap := 1000

	expectSum := func(spent int) {
		mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE created_at >= \\$1").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(spent))
	}

	// Initial OK should not emit.
	expectSum(100) // 10%
	if err := bt.evaluateScope(ctx, "portfolio", "", cap); err != nil {
		t.Fatalf("evaluate ok: %v", err)
	}
	select {
	case evt := <-ch:
		t.Fatalf("unexpected event emitted in ok: %s", evt.Type)
	case <-time.After(40 * time.Millisecond):
	}

	expectBudgetThreshold := func(t *testing.T, wantState, wantEventType string) {
		t.Helper()
		select {
		case evt := <-ch:
			if string(evt.Type) != "budget.threshold_crossed" {
				t.Fatalf("expected budget.threshold_crossed, got %s", evt.Type)
			}
			var payload map[string]any
			if err := json.Unmarshal(evt.Payload, &payload); err != nil {
				t.Fatalf("decode threshold payload: %v", err)
			}
			if got := asString(payload["state"]); got != wantState {
				t.Fatalf("expected threshold state=%s, got %q payload=%v", wantState, got, payload)
			}
			if got := asString(payload["next_event_type"]); got != wantEventType {
				t.Fatalf("expected next_event_type=%s, got %q payload=%v", wantEventType, got, payload)
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("expected %s threshold event", wantState)
		}
	}

	// Warning at 80%.
	expectSum(800)
	if err := bt.evaluateScope(ctx, "portfolio", "", cap); err != nil {
		t.Fatalf("evaluate warning: %v", err)
	}
	expectBudgetThreshold(t, "warning", "budget.warning")

	// Throttle at 90%.
	expectSum(900)
	if err := bt.evaluateScope(ctx, "portfolio", "", cap); err != nil {
		t.Fatalf("evaluate throttle: %v", err)
	}
	expectBudgetThreshold(t, "throttle", "budget.throttle")

	// Emergency at 100% creates critical mailbox item.
	expectSum(1000)
	if err := bt.evaluateScope(ctx, "portfolio", "", cap); err != nil {
		t.Fatalf("evaluate emergency: %v", err)
	}
	expectBudgetThreshold(t, "emergency", "budget.emergency")
	if mb.calls != 1 {
		t.Fatalf("expected 1 mailbox insert on emergency, got %d", mb.calls)
	}
	if mb.last.Type != "budget_increase" || mb.last.Priority != "critical" || mb.last.Status != "pending" {
		t.Fatalf("unexpected mailbox item: %+v", mb.last)
	}

	// Resume back to OK emits budget.resumed.
	expectSum(100)
	if err := bt.evaluateScope(ctx, "portfolio", "", cap); err != nil {
		t.Fatalf("evaluate resumed: %v", err)
	}
	expectBudgetThreshold(t, "ok", "budget.resumed")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestBudgetTracker_RecordSpend_And_RecordLLMUsage(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	bus := NewEventBus(InMemoryEventStore{})
	_ = bus.Subscribe("watcher", events.EventType("budget.*"))
	cfg := &config.Config{
		Budget: config.BudgetConfig{
			PortfolioMonthlyCap:   10_000,
			PerVerticalMonthlyCap: 5_000,
			FactoryMonthlyCap:     2_000,
		},
	}
	bt := NewBudgetTracker(db, bus, cfg, nil)
	ctx := context.Background()

	// RecordSpend with vertical_id should insert and evaluate portfolio + vertical.
	mock.ExpectExec("INSERT INTO spend_ledger").
		WithArgs("v1", "", "ads", 123, "USD", "x", "", "exact", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE created_at >= \\$1").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(100))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE vertical_id = \\$1::uuid").
		WithArgs("v1", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(100))

	if err := bt.RecordSpend(ctx, SpendRecord{
		VerticalID:   "v1",
		Category:     "ads",
		AmountCents:  123,
		Currency:     "USD",
		Description:  "x",
		ApprovedBy:   "",
		Source:       "",
		RecordedAt:   time.Now(),
		RequestingID: "req-1",
	}); err != nil {
		t.Fatalf("RecordSpend: %v", err)
	}

	// RecordSpend without vertical_id should evaluate portfolio + factory.
	mock.ExpectExec("INSERT INTO spend_ledger").
		WithArgs("", "", "infra", 50, "USD", "y", "", "exact", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE created_at >= \\$1").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(50))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE vertical_id IS NULL").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(50))

	if err := bt.RecordSpend(ctx, SpendRecord{
		Category:    "infra",
		AmountCents: 50,
		Description: "y",
	}); err != nil {
		t.Fatalf("RecordSpend factory: %v", err)
	}

	// RecordLLMUsage should compute cost and call through to RecordSpend (category=llm).
	// Use 1M+1M tokens to avoid rounding to 0.
	mock.ExpectExec("INSERT INTO spend_ledger").
		WithArgs("v1", "agent-1", "llm", 480, "USD", sqlmock.AnyArg(), "", "exact", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE created_at >= \\$1").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(0))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE vertical_id = \\$1::uuid").
		WithArgs("v1", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(0))

	if err := bt.RecordLLMUsage(ctx, "v1", "agent-1", "api", UsageTokens{
		InputTokens:  1_000_000,
		OutputTokens: 1_000_000,
		Model:        "claude-haiku-4-5",
	}, true, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("RecordLLMUsage: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestBudgetTracker_StateHelpers_NilAndNonNil(t *testing.T) {
	var nilBT *BudgetTracker
	if got := nilBT.CurrentState("portfolio", ""); got != "ok" {
		t.Fatalf("expected ok for nil tracker, got %q", got)
	}
	if nilBT.IsEmergency("v1") {
		t.Fatal("expected IsEmergency false for nil tracker")
	}
	if nilBT.IsThrottle("v1") {
		t.Fatal("expected IsThrottle false for nil tracker")
	}

	bt := &BudgetTracker{lastState: map[string]string{
		"portfolio|":      "throttle",
		"vertical|v1":     "emergency",
		"factory|":        "warning",
		"vertical|other":  "",
		"portfolio|other": "",
	}}
	if bt.CurrentState("missing", "x") != "ok" {
		t.Fatal("expected default ok for missing key")
	}
	if !bt.IsEmergency("v1") {
		t.Fatal("expected emergency for v1")
	}
	if !bt.IsThrottle("v1") {
		t.Fatal("expected throttle semantics implied by emergency")
	}
	if !bt.IsThrottle("") {
		t.Fatal("expected throttle for portfolio throttle")
	}
}

func TestBudgetTracker_EvaluateAll_IteratesVerticals(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	bus := NewEventBus(InMemoryEventStore{})
	cfg := &config.Config{
		Budget: config.BudgetConfig{
			PortfolioMonthlyCap:   10_000,
			PerVerticalMonthlyCap: 5_000,
			FactoryMonthlyCap:     2_000,
		},
	}
	bt := NewBudgetTracker(db, bus, cfg, nil)
	ctx := context.Background()

	// evaluateAndEmit(""): portfolio + factory sums
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE created_at >= \\$1").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(0))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE vertical_id IS NULL").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(0))

	// list verticals
	mock.ExpectQuery("SELECT id::text\\s+FROM verticals\\s+WHERE stage IN").
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("v1").AddRow("v2"))

	// evaluateAndEmit("v1"): portfolio + vertical sums
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE created_at >= \\$1").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(0))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE vertical_id = \\$1::uuid").
		WithArgs("v1", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(0))

	// evaluateAndEmit("v2"): portfolio + vertical sums
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE created_at >= \\$1").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(0))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(amount_cents\\), 0\\)\\s+FROM spend_ledger\\s+WHERE vertical_id = \\$1::uuid").
		WithArgs("v2", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(0))

	bt.EvaluateAll(ctx)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestBudgetHelpers_ModelTierAndEstimateLLMCost(t *testing.T) {
	if got := modelTier("claude-haiku-4-5"); got != "haiku" {
		t.Fatalf("expected haiku tier, got %q", got)
	}
	if got := modelTier("CLAUDE-OPUS-4"); got != "opus" {
		t.Fatalf("expected opus tier, got %q", got)
	}
	if got := modelTier("claude-sonnet-4-5"); got != "sonnet" {
		t.Fatalf("expected sonnet tier, got %q", got)
	}
	if got := modelTier("unknown-model"); got != "sonnet" {
		t.Fatalf("expected default sonnet tier, got %q", got)
	}

	bt := &BudgetTracker{}
	if got := bt.estimateLLMCostCents("claude-haiku-4-5", 1_000_000, 1_000_000); got != 480 {
		t.Fatalf("haiku cost mismatch: got %d want 480", got)
	}
	if got := bt.estimateLLMCostCents("claude-opus-4", 1_000_000, 1_000_000); got != 9000 {
		t.Fatalf("opus cost mismatch: got %d want 9000", got)
	}
	if got := bt.estimateLLMCostCents("claude-sonnet-4-5", 1_000_000, 1_000_000); got != 1800 {
		t.Fatalf("sonnet cost mismatch: got %d want 1800", got)
	}
	if got := bt.estimateLLMCostCents("claude-haiku-4-5", -100, -100); got != 0 {
		t.Fatalf("negative token clamp mismatch: got %d want 0", got)
	}
}

func TestBudgetHelpers_MergeMeta_AndEvaluateAndEmitGuard(t *testing.T) {
	merged := mergeMeta(map[string]any{"a": 1}, map[string]any{"b": 2})
	m, ok := merged.(map[string]any)
	if !ok || m["a"] != 1 || m["b"] != 2 {
		t.Fatalf("mergeMeta map branch mismatch: %#v", merged)
	}
	wrapped := mergeMeta("value", map[string]any{"k": "v"})
	w, ok := wrapped.(map[string]any)
	if !ok || w["meta"] != "value" {
		t.Fatalf("mergeMeta wrap branch mismatch: %#v", wrapped)
	}

	ctx := context.Background()
	var nilTracker *BudgetTracker
	if err := nilTracker.evaluateAndEmit(ctx, ""); err != nil {
		t.Fatalf("nil tracker evaluateAndEmit should noop, got %v", err)
	}
	partial := &BudgetTracker{cfg: &config.Config{}}
	if err := partial.evaluateAndEmit(ctx, "vertical-1"); err != nil {
		t.Fatalf("partial tracker evaluateAndEmit should noop, got %v", err)
	}
}
