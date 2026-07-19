package apiv1

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	storepkg "github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestSQLiteAgentUsageOwnerBacksSupportedAPISurface(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newSQLiteAgentUsageStoreFixture(t, ctx)
	seedSQLiteAgentUsageAgent(t, ctx, sqliteStore, "agent-1")
	seedSQLiteAgentUsageAgent(t, ctx, sqliteStore, "agent-2")

	since := time.Date(2026, 5, 21, 9, 0, 0, 0, time.UTC)
	until := time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC)
	for _, rec := range []budgetspend.SpendRecord{
		{ExecutionMode: "live", FlowInstance: "flow/a", AgentID: "agent-1", Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 100, OutputTokens: 25, CostUSD: 0.000675, InvocationType: "anthropic", UsageAccounting: storepkg.AgentUsageAccountingExact, RecordedAt: since},
		{ExecutionMode: "live", FlowInstance: "flow/a", AgentID: "agent-1", Model: "sonnet", ModelAlias: "regular", BackendProfile: "claude_cli", Provider: "claude", Transport: "cli", ResolvedModel: "sonnet", InputTokens: 50, OutputTokens: 10, CostUSD: 0.000300, InvocationType: "claude_cli", UsageAccounting: storepkg.AgentUsageAccountingEstimated, RecordedAt: since.Add(time.Minute)},
		{ExecutionMode: "mock", FlowInstance: "flow/a", AgentID: "agent-1", Model: "mock-regular", ModelAlias: "regular", BackendProfile: "mock", Provider: "mock", Transport: "in_process", ResolvedModel: "mock-regular", InputTokens: 5, OutputTokens: 2, CostUSD: 0.000025, InvocationType: "mock_python", UsageAccounting: storepkg.AgentUsageAccountingEstimated, RecordedAt: since.Add(2 * time.Minute)},
		{ExecutionMode: "live", FlowInstance: "flow/a", AgentID: "agent-1", Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 7, OutputTokens: 3, CostUSD: 0.000010, InvocationType: "anthropic", UsageAccounting: storepkg.AgentUsageAccountingExact, RecordedAt: until},
		{ExecutionMode: "live", FlowInstance: "flow/a", AgentID: "agent-2", Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 999, OutputTokens: 999, CostUSD: 1.000000, InvocationType: "anthropic", UsageAccounting: storepkg.AgentUsageAccountingExact, RecordedAt: since.Add(time.Minute)},
	} {
		if err := sqliteStore.RecordSpend(ctx, rec); err != nil {
			t.Fatalf("RecordSpend(%s/%s): %v", rec.AgentID, rec.UsageAccounting, err)
		}
	}

	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			AgentUsage: sqliteStore,
		}),
	})
	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"usage","method":"agent.usage","params":{"agent_id":"agent-1","since":"2026-05-21T09:00:00Z","until":"2026-05-21T10:00:00Z"}}`)
	if resp.Error != nil {
		t.Fatalf("agent.usage error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["agent_id"] != "agent-1" {
		t.Fatalf("agent_id = %#v", result["agent_id"])
	}
	usage := asMap(t, result["usage"])
	exact := asMap(t, usage["exact"])
	estimated := asMap(t, usage["estimated"])
	if exact["ledger_entries"] != float64(1) || exact["input_tokens"] != float64(100) || estimated["ledger_entries"] != float64(2) || estimated["input_tokens"] != float64(55) {
		t.Fatalf("usage totals = %#v", usage)
	}
	breakdown, _ := result["breakdown"].([]any)
	if len(breakdown) != 3 {
		t.Fatalf("breakdown = %#v, want three rows", result["breakdown"])
	}
	first := asMap(t, breakdown[0])
	if first["usage_accounting"] != storepkg.AgentUsageAccountingExact || first["model"] != "claude-3-5-sonnet" || first["provider"] != "anthropic" || first["transport"] != "api" || first["resolved_model"] != "claude-3-5-sonnet" {
		t.Fatalf("first breakdown = %#v", first)
	}
	mock := asMap(t, breakdown[2])
	if mock["execution_mode"] != "mock" || mock["provider"] != "mock" || mock["cost_display"] != "~$0.000025 (mock estimate)" {
		t.Fatalf("mock breakdown = %#v", mock)
	}
	for _, forbidden := range []string{"prompt", "response", "raw_request", "raw_response"} {
		if _, ok := result[forbidden]; ok {
			t.Fatalf("agent.usage exposed forbidden field %q: %#v", forbidden, result)
		}
	}
}

func TestSQLiteAgentDeliveryLifecycleOwnerBacksSupportedAPISurface(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newSQLiteAgentUsageStoreFixture(t, ctx)
	seedSQLiteAgentUsageAgent(t, ctx, sqliteStore, "agent-1")

	now := time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	deliveryID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	storetest.CommitSemanticEvent(t, ctx, sqliteStore, eventtest.RootIngress(
		eventID,
		events.EventType("task.ready"),
		"agent-usage-fixture",
		"",
		[]byte(`{}`),
		0,
		runID,
		"",
		events.EventEnvelope{EntityID: entityID, Scope: events.EventScopeEntity},
		now.Add(-time.Minute),
	))
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, failure, created_at
		) VALUES (
			?, ?, ?, 'agent', 'agent-1', 'pending', 1, 'retry_scheduled', ?, ?
		)
	`, deliveryID, runID, eventID, mustMarshalTestFailure(t, testFailure("temporary_failure")), now); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			AgentDeliveryLifecycle: sqliteStore,
		}),
	})
	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"lifecycle","method":"agent.delivery_lifecycle","params":{"agent_id":"agent-1","run_id":"`+runID+`","limit":10}}`)
	if resp.Error != nil {
		t.Fatalf("agent.delivery_lifecycle error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["agent_id"] != "agent-1" {
		t.Fatalf("agent_id = %#v", result["agent_id"])
	}
	deliveries, ok := result["deliveries"].([]any)
	if !ok || len(deliveries) != 1 {
		t.Fatalf("deliveries = %#v", result["deliveries"])
	}
	delivery := asMap(t, deliveries[0])
	if delivery["delivery_id"] != deliveryID || delivery["event_id"] != eventID || delivery["run_id"] != runID || delivery["entity_id"] != entityID || delivery["status"] != "pending" || delivery["retry_count"] != float64(1) {
		t.Fatalf("delivery = %#v", delivery)
	}
}

func newSQLiteAgentUsageStoreFixture(t *testing.T, ctx context.Context) *storepkg.SQLiteRuntimeStore {
	t.Helper()
	return storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
}

func seedSQLiteAgentUsageAgent(t *testing.T, ctx context.Context, sqliteStore *storepkg.SQLiteRuntimeStore, agentID string) {
	t.Helper()
	if err := sqliteStore.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            agentID,
			Role:          "researcher",
			Type:          "managed",
			Model:         "cheap",
			ExecutionMode: "live",
			Config:        json.RawMessage(`{"system_prompt":"usage"}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent %s: %v", agentID, err)
	}
}
