package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func TestBudgetTracker_KeepsTerminalStatesInstanceOwned(t *testing.T) {
	trackerA := NewBudgetTracker(nil, nil, nil, nil, nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			TerminalStages: []string{"done"},
		},
	}))
	trackerB := NewBudgetTracker(nil, nil, nil, nil, nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			TerminalStages: []string{"closed"},
		},
	}))

	if got, want := trackerA.TerminalInstanceStates(), []string{"done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("trackerA.TerminalInstanceStates() = %#v, want %#v", got, want)
	}
	if got, want := trackerB.TerminalInstanceStates(), []string{"closed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("trackerB.TerminalInstanceStates() = %#v, want %#v", got, want)
	}
}

func TestBudgetTrackerUsesRootTerminalStagesNotChildAggregate(t *testing.T) {
	tracker := NewBudgetTracker(nil, nil, nil, nil, nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			StageDeclarations: runtimecontracts.FlowStageDeclarations{
				Declared: true,
				Entries: []runtimecontracts.FlowStageDeclaration{
					{ID: "ready", Initial: true},
					{ID: "done"},
					{ID: "archived", Terminal: true},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			TerminalStages: []string{"done", "archived"},
			FlowTerminal: map[string][]string{
				"child": {"done"},
			},
		},
	}))

	if got, want := tracker.TerminalInstanceStates(), []string{"archived"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("TerminalInstanceStates() = %#v, want root-only %#v", got, want)
	}
}

func TestBudgetTracker_RecordLLMUsagePersistsUsageAccounting(t *testing.T) {
	store := &budgetSpendStoreCapture{}
	tracker := &BudgetTracker{store: store}
	ctx := context.Background()

	if err := tracker.RecordLLMUsage(ctx, "", "agent-1", "anthropic", llm.UsageTokens{
		Model:        "claude-3-5-sonnet",
		InputTokens:  11,
		OutputTokens: 7,
	}, true, map[string]any{
		"flow_instance":   "flow/1",
		"model_alias":     "regular",
		"backend_profile": "anthropic",
		"provider":        "anthropic",
		"transport":       "api",
		"resolved_model":  "claude-3-5-sonnet",
	}); err != nil {
		t.Fatalf("RecordLLMUsage exact: %v", err)
	}
	if len(store.records) != 1 {
		t.Fatalf("records after exact = %d, want 1", len(store.records))
	}
	assertBudgetSpendRecord(t, store.records[0], budgetspend.SpendRecord{
		FlowInstance:    "flow/1",
		AgentID:         "agent-1",
		Model:           "claude-3-5-sonnet",
		ModelAlias:      "regular",
		BackendProfile:  "anthropic",
		Provider:        "anthropic",
		Transport:       "api",
		ResolvedModel:   "claude-3-5-sonnet",
		InputTokens:     11,
		OutputTokens:    7,
		InvocationType:  "anthropic",
		UsageAccounting: "exact",
	})

	if err := tracker.RecordLLMUsage(ctx, "", "agent-1", "claude_cli", llm.UsageTokens{
		Model:        "sonnet",
		InputTokens:  13,
		OutputTokens: 5,
	}, false, map[string]any{
		"flow_instance":   "flow/1",
		"model_alias":     "regular",
		"backend_profile": "claude_cli",
		"provider":        "claude",
		"transport":       "cli",
		"resolved_model":  "sonnet",
	}); err != nil {
		t.Fatalf("RecordLLMUsage estimated: %v", err)
	}
	if len(store.records) != 2 {
		t.Fatalf("records after estimated = %d, want 2", len(store.records))
	}
	assertBudgetSpendRecord(t, store.records[1], budgetspend.SpendRecord{
		FlowInstance:    "flow/1",
		AgentID:         "agent-1",
		Model:           "sonnet",
		ModelAlias:      "regular",
		BackendProfile:  "claude_cli",
		Provider:        "claude",
		Transport:       "cli",
		ResolvedModel:   "sonnet",
		InputTokens:     13,
		OutputTokens:    5,
		InvocationType:  "claude_cli",
		UsageAccounting: "estimated",
	})
}

func assertBudgetSpendRecord(t *testing.T, got budgetspend.SpendRecord, want budgetspend.SpendRecord) {
	t.Helper()
	if got.FlowInstance != want.FlowInstance || got.AgentID != want.AgentID || got.Model != want.Model || got.ModelAlias != want.ModelAlias || got.BackendProfile != want.BackendProfile || got.Provider != want.Provider || got.Transport != want.Transport || got.ResolvedModel != want.ResolvedModel || got.InputTokens != want.InputTokens || got.OutputTokens != want.OutputTokens || got.InvocationType != want.InvocationType || got.UsageAccounting != want.UsageAccounting {
		t.Fatalf("spend record = %#v, want matching %#v", got, want)
	}
	if got.RecordedAt.IsZero() {
		t.Fatal("spend record RecordedAt is zero")
	}
}

type budgetSpendStoreCapture struct {
	records    []budgetspend.SpendRecord
	sum        float64
	sumQueries []budgetspend.SpendQuery
}

func (s *budgetSpendStoreCapture) RecordSpend(_ context.Context, rec budgetspend.SpendRecord) error {
	s.records = append(s.records, rec)
	return nil
}

func (s *budgetSpendStoreCapture) ResolveFlowInstance(context.Context, string, string) (string, error) {
	return "", nil
}

func (s *budgetSpendStoreCapture) ListActiveEntityIDs(context.Context, string, []string) ([]string, error) {
	return nil, nil
}

func (s *budgetSpendStoreCapture) SumSpendUSD(_ context.Context, query budgetspend.SpendQuery) (float64, error) {
	s.sumQueries = append(s.sumQueries, query)
	return s.sum, nil
}

func TestBudgetTracker_RecordSpendNormalizesThroughBudgetSpendOwner(t *testing.T) {
	store := &budgetSpendStoreCapture{}
	tracker := &BudgetTracker{store: store}
	if err := tracker.RecordSpend(context.Background(), SpendRecord{
		FlowInstance:    " flow/1 ",
		AgentID:         " agent-1 ",
		Model:           " claude ",
		InputTokens:     -1,
		OutputTokens:    -2,
		CostUSD:         -3,
		InvocationType:  " API ",
		UsageAccounting: " EXACT ",
		RecordedAt:      time.Time{},
	}); err != nil {
		t.Fatalf("RecordSpend: %v", err)
	}
	if len(store.records) != 1 {
		t.Fatalf("records = %d, want 1", len(store.records))
	}
	got := store.records[0]
	if got.FlowInstance != "flow/1" || got.AgentID != "agent-1" || got.Model != "claude" || got.InputTokens != 0 || got.OutputTokens != 0 || got.CostUSD != 0 || got.InvocationType != "api" || got.UsageAccounting != "exact" {
		t.Fatalf("normalized spend record = %#v", got)
	}
}

func TestBudgetTrackerThresholdEventAndEmergencyMailboxConsumeBudgetSpendOwner(t *testing.T) {
	store := &budgetSpendStoreCapture{sum: 0.95}
	eventStore := &bootSelfCheckDescriptorStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	mailbox := &budgetMailboxCapture{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"budget_warning_percent":   {Value: 50},
			"budget_throttle_percent":  {Value: 75},
			"budget_emergency_percent": {Value: 90},
		}},
	})
	tracker := NewBudgetTracker(store, bus, &config.Config{Extensions: map[string]any{
		"budget": map[string]any{"system_monthly_cap": 1},
	}}, mailbox, nil, source)

	if err := tracker.RecordSpend(context.Background(), SpendRecord{
		FlowInstance:    "global",
		AgentID:         "agent-1",
		Model:           "claude",
		InputTokens:     1,
		OutputTokens:    1,
		CostUSD:         0.95,
		InvocationType:  "api",
		UsageAccounting: "exact",
	}); err != nil {
		t.Fatalf("RecordSpend: %v", err)
	}
	events := eventStore.appendedEvents()
	if len(events) != 1 || string(events[0].Type()) != "platform.budget_threshold_crossed" {
		t.Fatalf("events = %#v, want one platform.budget_threshold_crossed", events)
	}
	var payload map[string]any
	if err := json.Unmarshal(events[0].Payload(), &payload); err != nil {
		t.Fatalf("unmarshal budget event payload: %v", err)
	}
	if payload["level"] != "emergency" {
		t.Fatalf("budget event level = %#v, want emergency", payload["level"])
	}
	if len(mailbox.items) != 1 || mailbox.items[0].Priority != "critical" || mailbox.items[0].Type != "alert" {
		t.Fatalf("mailbox items = %#v, want one critical alert", mailbox.items)
	}
	if len(store.sumQueries) != 1 || store.sumQueries[0].Scope != budgetspend.ScopeSystem {
		t.Fatalf("sum queries = %#v, want one system aggregate", store.sumQueries)
	}
}

func TestNewRuntimeConstructsBudgetTrackerFromBackendNeutralStore(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	store := &budgetSpendStoreCapture{}
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{
		EventStore:       &minimalRuntimeEventStore{},
		BudgetSpendStore: store,
	}, Options: RuntimeOptions{
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if rt.Budget == nil {
		t.Fatal("Runtime Budget = nil, want backend-neutral budget tracker")
	}
	if rt.Budget.store != store {
		t.Fatalf("Runtime Budget store = %#v, want backend-neutral store %#v", rt.Budget.store, store)
	}
	if rt.Stores.SQLDB != nil {
		t.Fatalf("Runtime Stores.SQLDB = %#v, want nil raw SQL handle", rt.Stores.SQLDB)
	}
}

type budgetMailboxCapture struct {
	items []runtimetools.MailboxItem
}

func (m *budgetMailboxCapture) InsertMailboxItem(_ context.Context, item runtimetools.MailboxItem) (string, error) {
	m.items = append(m.items, item)
	return "mailbox-1", nil
}

func (*budgetMailboxCapture) ListMailboxItems(context.Context, string, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}

func (*budgetMailboxCapture) CountMailboxItems(context.Context, string) (int, error) {
	return 0, nil
}

func (*budgetMailboxCapture) GetMailboxItem(context.Context, string) (runtimetools.MailboxItem, error) {
	return runtimetools.MailboxItem{}, nil
}

func (*budgetMailboxCapture) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}

func (*budgetMailboxCapture) ExpireMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}

func (*budgetMailboxCapture) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}

func (*budgetMailboxCapture) MarkMailboxItemNotified(context.Context, string) error {
	return nil
}
