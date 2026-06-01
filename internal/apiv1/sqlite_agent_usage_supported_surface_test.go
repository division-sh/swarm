package apiv1

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	storepkg "github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestSQLiteAgentUsageOwnerBacksSupportedAPISurface(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newSQLiteAgentUsageStoreFixture(t, ctx)
	seedSQLiteAgentUsageAgent(t, ctx, sqliteStore, "agent-1")
	seedSQLiteAgentUsageAgent(t, ctx, sqliteStore, "agent-2")

	since := time.Date(2026, 5, 21, 9, 0, 0, 0, time.UTC)
	until := time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC)
	for _, rec := range []budgetspend.SpendRecord{
		{FlowInstance: "flow/a", AgentID: "agent-1", Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 100, OutputTokens: 25, CostUSD: 0.000675, InvocationType: "anthropic", UsageAccounting: storepkg.AgentUsageAccountingExact, RecordedAt: since},
		{FlowInstance: "flow/a", AgentID: "agent-1", Model: "sonnet", ModelAlias: "regular", BackendProfile: "claude_cli", Provider: "claude", Transport: "cli", ResolvedModel: "sonnet", InputTokens: 50, OutputTokens: 10, CostUSD: 0.000300, InvocationType: "claude_cli", UsageAccounting: storepkg.AgentUsageAccountingEstimated, RecordedAt: since.Add(time.Minute)},
		{FlowInstance: "flow/a", AgentID: "agent-1", Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 7, OutputTokens: 3, CostUSD: 0.000010, InvocationType: "anthropic", UsageAccounting: storepkg.AgentUsageAccountingExact, RecordedAt: until},
		{FlowInstance: "flow/a", AgentID: "agent-2", Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 999, OutputTokens: 999, CostUSD: 1.000000, InvocationType: "anthropic", UsageAccounting: storepkg.AgentUsageAccountingExact, RecordedAt: since.Add(time.Minute)},
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
	if exact["ledger_entries"] != float64(1) || exact["input_tokens"] != float64(100) || estimated["ledger_entries"] != float64(1) || estimated["input_tokens"] != float64(50) {
		t.Fatalf("usage totals = %#v", usage)
	}
	breakdown, _ := result["breakdown"].([]any)
	if len(breakdown) != 2 {
		t.Fatalf("breakdown = %#v, want two rows", result["breakdown"])
	}
	first := asMap(t, breakdown[0])
	if first["usage_accounting"] != storepkg.AgentUsageAccountingExact || first["model"] != "claude-3-5-sonnet" || first["provider"] != "anthropic" || first["transport"] != "api" || first["resolved_model"] != "claude-3-5-sonnet" {
		t.Fatalf("first breakdown = %#v", first)
	}
	for _, forbidden := range []string{"prompt", "response", "raw_request", "raw_response"} {
		if _, ok := result[forbidden]; ok {
			t.Fatalf("agent.usage exposed forbidden field %q: %#v", forbidden, result)
		}
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
			ID:               agentID,
			Role:             "researcher",
			Type:             "managed",
			Model:            "cheap",
			ConversationMode: runtimesessions.RuntimeModeTask.String(),
			Config:           json.RawMessage(`{"system_prompt":"usage"}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent %s: %v", agentID, err)
	}
}
