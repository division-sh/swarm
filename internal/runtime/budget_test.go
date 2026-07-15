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
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
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

type budgetSpendStoreCapture struct {
	records    []budgetspend.SpendRecord
	sum        float64
	sumQueries []budgetspend.SpendQuery
	targets    []budgetspend.ProjectionTarget
	calls      []string
}

func (s *budgetSpendStoreCapture) RecordSpend(_ context.Context, rec budgetspend.SpendRecord) error {
	s.records = append(s.records, rec)
	return nil
}

func (s *budgetSpendStoreCapture) ResolveFlowInstance(context.Context, string, string) (string, error) {
	return "", nil
}

func (s *budgetSpendStoreCapture) ListBudgetProjectionTargets(context.Context, []string) ([]budgetspend.ProjectionTarget, error) {
	s.calls = append(s.calls, "targets")
	return append([]budgetspend.ProjectionTarget(nil), s.targets...), nil
}

func (s *budgetSpendStoreCapture) SumSpendUSD(_ context.Context, query budgetspend.SpendQuery) (float64, error) {
	s.sumQueries = append(s.sumQueries, query)
	s.calls = append(s.calls, string(query.Scope)+"|"+query.EntityID)
	return s.sum, nil
}

func TestBudgetTracker_RecordSpendNormalizesThroughBudgetSpendOwner(t *testing.T) {
	store := &budgetSpendStoreCapture{}
	tracker := &BudgetTracker{store: store}
	if err := tracker.RecordSpend(context.Background(), SpendRecord{
		ExecutionMode:   "live",
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

func TestBudgetTrackerProjectsCommittedCompletionIntoThresholdEventAndEmergencyState(t *testing.T) {
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

	tracker.ProjectCommittedCompletionSpend(context.Background(), runtimeeffects.CompletionSpendProjection{AttemptID: "attempt-1"})
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
	if len(store.records) != 0 {
		t.Fatalf("completion projection wrote %d spend rows, want read-only projection", len(store.records))
	}
	if !tracker.IsEmergency("") || !tracker.IsThrottle("") {
		t.Fatalf("projected budget state emergency=%v throttle=%v, want both true", tracker.IsEmergency(""), tracker.IsThrottle(""))
	}
}

func TestBudgetTrackerProjectsRecoveryScopesBeforeAllRunTargetsWithoutRunContext(t *testing.T) {
	entityA := "10000000-0000-4000-8000-000000000001"
	entityB := "20000000-0000-4000-8000-000000000002"
	store := &budgetSpendStoreCapture{
		sum: 0.95,
		targets: []budgetspend.ProjectionTarget{
			{RunID: "10000000-0000-4000-8000-000000000010", EntityID: entityA},
			{RunID: "20000000-0000-4000-8000-000000000020", EntityID: entityB},
		},
	}
	eventStore := &bootSelfCheckDescriptorStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"budget_warning_percent":   {Value: 50},
			"budget_throttle_percent":  {Value: 75},
			"budget_emergency_percent": {Value: 90},
		}},
	})
	tracker := NewBudgetTracker(store, bus, &config.Config{Extensions: map[string]any{
		"budget": map[string]any{
			"system_monthly_cap":     1,
			"global_monthly_cap":     1,
			"per_entity_monthly_cap": 1,
		},
	}}, nil, nil, source)

	if err := tracker.ProjectRecoveryBudgetState(context.Background()); err != nil {
		t.Fatalf("ProjectRecoveryBudgetState: %v", err)
	}
	wantCalls := []string{
		"system|",
		"global|",
		"targets",
		"entity|" + entityA,
		"entity|" + entityB,
	}
	if !reflect.DeepEqual(store.calls, wantCalls) {
		t.Fatalf("recovery projection calls = %#v, want %#v", store.calls, wantCalls)
	}
	if !tracker.IsEmergency("") || !tracker.IsEmergency(entityA) || !tracker.IsEmergency(entityB) {
		t.Fatalf("recovered emergency states system=%v entityA=%v entityB=%v, want all true", tracker.IsEmergency(""), tracker.IsEmergency(entityA), tracker.IsEmergency(entityB))
	}
	if got := len(eventStore.appendedEvents()); got != 4 {
		t.Fatalf("threshold events = %d, want system, global, and two entity transitions", got)
	}

	if err := tracker.ProjectRecoveryBudgetState(context.Background()); err != nil {
		t.Fatalf("second ProjectRecoveryBudgetState: %v", err)
	}
	if got := len(eventStore.appendedEvents()); got != 4 {
		t.Fatalf("threshold events after repeated recovery = %d, want idempotent 4", got)
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

func (*budgetMailboxCapture) ExpireMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}

func (*budgetMailboxCapture) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]runtimetools.MailboxItem, error) {
	return nil, nil
}

func (*budgetMailboxCapture) MarkMailboxItemNotified(context.Context, string) error {
	return nil
}
