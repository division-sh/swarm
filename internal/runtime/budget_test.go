package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestBudgetRecoveryStageClassifierKeepsFlowScopedTerminality(t *testing.T) {
	root := runtimecontracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "done"}, []string{"done"}, nil, nil, nil)
	child := runtimecontracts.BuildWorkflowStageTopology("child", "done", []string{"ready", "done"}, []string{"ready"}, nil, nil, nil)
	owner, err := runtimecontracts.NewWorkflowStageClassifier(root, map[string]runtimecontracts.WorkflowStageTopology{"child": child})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		flow, stage     string
		terminal, known bool
	}{
		{"", "done", true, true}, {"child", "done", false, true},
		{"child", "ready", true, true}, {"", "ready", false, true},
		{"child", "Done", false, false}, {"foreign", "done", false, false},
	} {
		got, known := owner.Terminal(tc.flow, tc.flow, tc.stage)
		if got != tc.terminal || known != tc.known {
			t.Errorf("flow %q stage %q: terminal=%v known=%v, want %v/%v", tc.flow, tc.stage, got, known, tc.terminal, tc.known)
		}
	}
}

func TestBudgetRecoveryUsesEachRunsSelectedStageSource(t *testing.T) {
	artifactA := sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# selected-budget-source-a\n"))
	hashA := artifactA.BundleHash()
	artifactB := sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# selected-budget-source-b\n"))
	hashB := artifactB.BundleHash()
	makeSource := func(hash string, terminal string) semanticview.Source {
		return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Semantics: runtimecontracts.WorkflowSemanticView{Version: hash, StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{
				".": runtimecontracts.BuildWorkflowStageTopology(".", "shared", []string{"shared", "fresh"}, []string{terminal}, nil, nil, nil),
			}},
			Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
				"budget_warning_percent": {Value: 50}, "budget_throttle_percent": {Value: 75}, "budget_emergency_percent": {Value: 90},
			}},
		})
	}
	sourceA, sourceB := makeSource(hashA, "fresh"), makeSource(hashB, "shared")
	bundleA, _ := semanticview.Bundle(sourceA)
	bundleA.SourceArtifact = artifactA
	bundleB, _ := semanticview.Bundle(sourceB)
	bundleB.SourceArtifact = artifactB
	const entityA, entityB, terminalB = "entity-a", "entity-b", "terminal-b"
	store := &budgetSpendStoreCapture{sum: 0.95, targets: []budgetspend.ProjectionTarget{
		{RunID: "run-a", EntityID: entityA, BundleHash: hashA, Stage: "shared"},
		{RunID: "run-b", EntityID: terminalB, BundleHash: hashB, Stage: "shared"},
		{RunID: "run-b", EntityID: entityB, BundleHash: hashB, Stage: "fresh"},
		{RunID: "run-b", EntityID: entityA, BundleHash: hashB, Stage: "fresh"},
	}}
	eventStore := &bootSelfCheckDescriptorStore{}
	bus, err := newRuntimeTestEventBus(t, eventStore)
	if err != nil {
		t.Fatal(err)
	}
	loads := 0
	tracker := NewBudgetTracker(store, bus, &config.Config{Extensions: map[string]any{
		"budget": map[string]any{"per_entity_monthly_cap": 1},
	}}, nil, nil, sourceA, executionposture.Live, BudgetRecoveryStageSources{
		CurrentBundleHash: hashA,
		Load: func(_ context.Context, hash string) (semanticview.Source, error) {
			loads++
			if hash != hashB {
				t.Fatalf("loaded unexpected bundle %q", hash)
			}
			return sourceB, nil
		},
	})
	if err := tracker.ProjectRecoveryBudgetState(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatal(err)
	}
	if loads != 1 || tracker.CurrentState("entity", entityA) != "emergency" || tracker.CurrentState("entity", entityB) != "emergency" || tracker.CurrentState("entity", terminalB) != "ok" {
		t.Fatalf("selected source recovery: loads=%d a=%q b=%q terminal=%q", loads, tracker.CurrentState("entity", entityA), tracker.CurrentState("entity", entityB), tracker.CurrentState("entity", terminalB))
	}
	if len(store.sumQueries) != 2 {
		t.Fatalf("entity dedup/spend queries = %#v, want exactly two", store.sumQueries)
	}
	bundleA.SourceArtifact = artifactB
	store.sumQueries = nil
	if err := tracker.ProjectRecoveryBudgetState(testAuthorActivityContext(context.Background())); err == nil {
		t.Fatal("current runtime source with foreign artifact was accepted")
	}
	if len(store.sumQueries) != 0 {
		t.Fatalf("mismatched current source changed entity budget projection: %#v", store.sumQueries)
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

func (s *budgetSpendStoreCapture) ListBudgetProjectionTargets(context.Context) ([]budgetspend.ProjectionTarget, error) {
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
	if err := tracker.RecordSpend(testAuthorActivityContext(context.Background()), SpendRecord{
		ExecutionMode:   "live",
		FlowInstance:    " flow/1 ",
		AgentID:         " agent-1 ",
		AgentIdentity:   agentidentitytest.Runtime(t, "agent-1", "budget-test", "flow", "1", "flow/1"),
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
	bus, err := newRuntimeTestEventBus(t, eventStore)
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
	}}, mailbox, nil, source, executionposture.Live, BudgetRecoveryStageSources{})

	tracker.ProjectCommittedCompletionSpend(testAuthorActivityContext(context.Background()), runtimeeffects.CompletionSpendProjection{AttemptID: "attempt-1"})
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

func TestBudgetTrackerActiveWorkThresholdPreservesInboundLineage(t *testing.T) {
	spendStore := &budgetSpendStoreCapture{sum: 0.95}
	eventStore := &bootSelfCheckDescriptorStore{}
	bus, err := newRuntimeTestEventBus(t, eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"budget_warning_percent": {Value: 50}, "budget_throttle_percent": {Value: 75}, "budget_emergency_percent": {Value: 90},
	}}})
	tracker := NewBudgetTracker(spendStore, bus, &config.Config{Extensions: map[string]any{"budget": map[string]any{"system_monthly_cap": 1}}}, nil, nil, source, executionposture.Live, BudgetRecoveryStageSources{})
	runID, parentID := uuid.NewString(), uuid.NewString()
	inbound := eventtest.RunCreatingRootIngressWithMode(parentID, "work.received", "gateway", "task-1", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC(), executionmode.Mock)
	ctx := runtimecorrelation.WithInboundEvent(testAuthorActivityContext(context.Background()), inbound)

	tracker.ProjectCommittedCompletionSpend(ctx, runtimeeffects.CompletionSpendProjection{AttemptID: "attempt-contextual"})
	published := eventStore.appendedEvents()
	if len(published) != 1 {
		t.Fatalf("budget events = %d, want 1", len(published))
	}
	got := published[0]
	if got.RunID() != runID || got.ParentEventID() != parentID || got.TaskID() != "task-1" || got.ExecutionMode() != executionmode.Mock {
		t.Fatalf("budget lineage = run:%q parent:%q task:%q mode:%q", got.RunID(), got.ParentEventID(), got.TaskID(), got.ExecutionMode())
	}
}

func TestBudgetTrackerProjectsRecoveryScopesBeforeAllRunTargetsWithoutRunContext(t *testing.T) {
	artifact := sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# recovery-scopes\n"))
	bundleHash := artifact.BundleHash()
	entityA := "10000000-0000-4000-8000-000000000001"
	entityB := "20000000-0000-4000-8000-000000000002"
	store := &budgetSpendStoreCapture{
		sum: 0.95,
		targets: []budgetspend.ProjectionTarget{
			{RunID: "10000000-0000-4000-8000-000000000010", EntityID: entityA, BundleHash: bundleHash, Stage: "active"},
			{RunID: "20000000-0000-4000-8000-000000000020", EntityID: entityB, BundleHash: bundleHash, Stage: "active"},
		},
	}
	eventStore := &bootSelfCheckDescriptorStore{}
	bus, err := newRuntimeTestEventBus(t, eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		SourceArtifact: artifact,
		Semantics: runtimecontracts.WorkflowSemanticView{StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{
			".": runtimecontracts.BuildWorkflowStageTopology(".", "active", []string{"active", "done"}, []string{"done"}, nil, nil, nil),
		}},
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
	}}, nil, nil, source, executionposture.Live, BudgetRecoveryStageSources{CurrentBundleHash: bundleHash})

	if err := tracker.ProjectRecoveryBudgetState(testAuthorActivityContext(context.Background())); err != nil {
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

	if err := tracker.ProjectRecoveryBudgetState(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("second ProjectRecoveryBudgetState: %v", err)
	}
	if got := len(eventStore.appendedEvents()); got != 4 {
		t.Fatalf("threshold events after repeated recovery = %d, want idempotent 4", got)
	}
}

func TestNewRuntimeConstructsBudgetTrackerFromBackendNeutralStore(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	store := &budgetSpendStoreCapture{}
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: testOperationalRuntimeConfig(),
		EventStore:       &minimalRuntimeEventStore{},
		BudgetSpendStore: store,
		Options: RuntimeOptions{
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

func (*budgetMailboxCapture) CountUnreadInformationalNotices(context.Context) (int, error) {
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
