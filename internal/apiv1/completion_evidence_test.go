package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type completion2270PublicStore interface {
	storetest.ManagedAgentTurnFixtureStore
	storetest.AgentFixtureStore
	storetest.RunFixtureStore
	AgentReadStore
	AgentUsageReadStore
	ConversationReadStore
}

func TestCompletion2270UsageOwnerContradictionsRefused(t *testing.T) {
	request := effects.CompletionRequest{ModelAlias: "regular", RequestedModel: "request-A", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api"}
	evidence := effects.CompletionEvidence{Request: request, Models: []effects.CompletionModelObservation{{Model: "reported-B", Source: effects.CompletionModelResponse}}, CostProvenance: effects.CompletionCostEstimated}
	row := operatorread.OperatorAgentUsageBreakdown{ExecutionMode: "live", UsageAccounting: operatorread.AgentUsageAccountingExact,
		InvocationType: "anthropic", Model: request.RequestedModel, ModelAlias: request.ModelAlias, BackendProfile: request.BackendProfile,
		Provider: request.Provider, Transport: request.Transport, ResolvedModel: "reported-B", CompletionEvidence: &evidence,
		CostProvenance: evidence.CostProvenance, CostDisplay: "$0.000000 (local estimate)"}
	result := operatorread.OperatorAgentUsage{AgentID: "agent-1", Breakdown: []operatorread.OperatorAgentUsageBreakdown{row}}
	if err := validateAgentUsageResult(result); err != nil {
		t.Fatalf("valid owner result: %v", err)
	}
	for name, mutate := range map[string]func(*operatorread.OperatorAgentUsageBreakdown){
		"request":      func(r *operatorread.OperatorAgentUsageBreakdown) { r.Model = "current-config" },
		"alias":        func(r *operatorread.OperatorAgentUsageBreakdown) { r.ModelAlias = "cheap" },
		"profile":      func(r *operatorread.OperatorAgentUsageBreakdown) { r.BackendProfile = "openai_responses" },
		"provider":     func(r *operatorread.OperatorAgentUsageBreakdown) { r.Provider = "openai" },
		"transport":    func(r *operatorread.OperatorAgentUsageBreakdown) { r.Transport = "cli" },
		"observation":  func(r *operatorread.OperatorAgentUsageBreakdown) { r.ResolvedModel = r.Model },
		"price-source": func(r *operatorread.OperatorAgentUsageBreakdown) { r.CostProvenance = effects.CompletionCostReported },
	} {
		t.Run(name, func(t *testing.T) {
			changed := row
			mutate(&changed)
			result.Breakdown = []operatorread.OperatorAgentUsageBreakdown{changed}
			if err := validateAgentUsageResult(result); err == nil {
				t.Fatalf("accepted contradictory owner evidence: %+v", changed)
			}
		})
	}
}

func TestCompletion2270PublicReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var selected completion2270PublicStore
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
			} else {
				selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t)
			}
			runID, sessionID := uuid.NewString(), uuid.NewString()
			identity := sqliteAgentUsageIdentityForRun(t, runID, "completion-2270")
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin()})
			seedCompletion2270Agent(t, ctx, selected, identity)
			want := map[string]effects.CompletionEvidence{}
			base := time.Now().UTC()
			for i := 0; i < 4; i++ {
				turnID, eventID := uuid.NewString(), uuid.NewString()
				at := base.Add(time.Duration(i) * time.Second)
				event := eventtest.ExistingRunRootIngress(eventID, "completion.proof", "completion-2270", "", []byte(`{}`), 0, runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, at)
				storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, nil, "subscribed")
				input, output := int64(10), int64(2)
				request := effects.CompletionRequest{ModelAlias: "regular", RequestedModel: fmt.Sprintf("request-%d", i%2), BackendProfile: "claude_cli", Provider: "claude", Transport: "cli"}
				evidence := effects.CompletionEvidence{Request: request, Models: []effects.CompletionModelObservation{}, CostProvenance: effects.CompletionCostEstimated}
				for contributor := 0; contributor < 32; contributor++ {
					evidence.Models = append(evidence.Models, effects.CompletionModelObservation{Model: fmt.Sprintf("reported-%d-%02d", i, contributor), Source: effects.CompletionModelCLIUsage, InputTokens: &input})
				}
				usage := effects.CompletionUsage{Evidence: evidence.Canonical(), Exactness: effects.CompletionUsageExact, InputTokens: &input, OutputTokens: &output}
				cost := 0.2
				if i == 1 {
					zero := 0.0
					cost, usage.ProviderReportedCostUSD = 0, &zero
					usage.Evidence.CostProvenance, usage.Evidence.ProviderReportedCostUSD = effects.CompletionCostReported, &zero
				}
				storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{Store: selected, Selected: selected, Identity: identity, RunID: runID, SessionID: sessionID,
					TurnID: turnID, Event: storetest.LoadCanonicalEventRecord(t, ctx, selected, eventID), Adapter: "claude_cli", RequestedModel: request.RequestedModel,
					Usage: &usage, CostUSD: cost, ParseOK: true, CreatedAt: at})
				want[turnID] = usage.Evidence.Canonical()
				usage.Evidence.Models[0].Model = "mutated-after-settlement"
				*usage.InputTokens = 999
			}
			handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorAgentConversationHandlers(AgentConversationHandlerOptions{Agents: selected, Conversations: selected, Usage: selected})})
			cursor, seen, previousIndex := "", map[string]bool{}, 5
			for pageIndex := 0; pageIndex < 2; pageIndex++ {
				response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"conversation.list_turns","params":{"session_id":%q,"limit":2,"cursor":%q}}`, sessionID, cursor))
				var page operatorread.OperatorConversationTurnListResult
				decodeCompletion2270RPC(t, response, &page)
				if len(page.Turns) != 2 {
					t.Fatalf("page cardinality: %+v", page)
				}
				for _, turn := range page.Turns {
					if seen[turn.TurnID] || turn.Ordinal >= previousIndex || !reflect.DeepEqual(turn.CompletionEvidence, want[turn.TurnID]) || turn.ResolvedModel != "" {
						t.Fatalf("pagination collapsed/mutated evidence: %+v", turn)
					}
					seen[turn.TurnID], previousIndex = true, turn.Ordinal
					response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"conversation.get_turn","params":{"session_id":%q,"turn_id":%q}}`, sessionID, turn.TurnID))
					var detail operatorread.OperatorPublicConversationTurnDetail
					decodeCompletion2270RPC(t, response, &detail)
					if !reflect.DeepEqual(detail.Turn.CompletionEvidence, want[turn.TurnID]) {
						t.Fatal("detail disagrees with page")
					}
				}
				cursor = page.NextCursor
				if (cursor != "") != (pageIndex == 0) {
					t.Fatalf("incorrect continuation: %+v", page)
				}
			}
			var usage operatorread.OperatorAgentUsage
			response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":3,"method":"agent.usage","params":{"run_id":%q,"agent_id":%q}}`, runID, identity.AgentID()))
			decodeCompletion2270RPC(t, response, &usage)
			if len(usage.Breakdown) != 4 || usage.Usage.Exact.LedgerEntries != 4 || usage.Usage.Exact.InputTokens != 40 || usage.Usage.Exact.EstimatedCostUSD != usage.Usage.Exact.CostUSD || usage.Usage.Exact.CostUSD < 0.599999 || usage.Usage.Exact.CostUSD > 0.600001 {
				t.Fatalf("independent dimensions merged or double-charged: %+v", usage)
			}
			for _, row := range usage.Breakdown {
				if row.CompletionEvidence == nil || len(row.CompletionEvidence.Models) != 32 || row.ResolvedModel != "" {
					t.Fatalf("aggregate omitted observations: %+v", row)
				}
			}
			proveCompletion2270PublicBudget(t, ctx, selected, handler, identity, sessionID)
		})
	}
}

func proveCompletion2270PublicBudget(t *testing.T, ctx context.Context, selected completion2270PublicStore, handler *Handler, identity agentidentity.Identity, sessionID string) {
	t.Helper()
	turnID, eventID, at := uuid.NewString(), uuid.NewString(), time.Now().UTC().Add(5*time.Second)
	request := effects.CompletionRequest{ModelAlias: "regular", RequestedModel: "request-budget", BackendProfile: "claude_cli", Provider: "claude", Transport: "cli"}
	label := strings.Repeat("x", durabledata.MaxRPCMessageBytes)
	input, output := int64(10), int64(2)
	usage := effects.CompletionUsage{Exactness: effects.CompletionUsageExact, ResolvedModel: label, InputTokens: &input, OutputTokens: &output,
		Evidence: effects.CompletionEvidence{Request: request, Models: []effects.CompletionModelObservation{{Model: label, Source: effects.CompletionModelCLITerminal}}, CostProvenance: effects.CompletionCostEstimated}}
	event := eventtest.ExistingRunRootIngress(eventID, "completion.proof", "completion-2270", "", []byte(`{}`), 0, identity.RunID, events.EventEnvelope{Scope: events.EventScopeGlobal}, at)
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, nil, "subscribed")
	storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{Store: selected, Selected: selected, Identity: identity, RunID: identity.RunID,
		SessionID: sessionID, TurnID: turnID, Event: storetest.LoadCanonicalEventRecord(t, ctx, selected, eventID), Adapter: "claude_cli", RequestedModel: request.RequestedModel,
		Usage: &usage, ParseOK: true, CreatedAt: at})
	private, err := selected.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
	if err != nil || private.Turn.CompletionEvidence.Models[0].Model != label || private.Turn.ResolvedModel != label {
		t.Fatalf("stored oversized evidence was truncated: error=%v", err)
	}
	for _, call := range []struct{ method, params string }{
		{"conversation.get_turn", fmt.Sprintf(`{"session_id":%q,"turn_id":%q}`, sessionID, turnID)},
		{"conversation.list_turns", fmt.Sprintf(`{"session_id":%q,"limit":1}`, sessionID)},
		{"agent.usage", fmt.Sprintf(`{"run_id":%q,"agent_id":%q}`, identity.RunID, identity.AgentID())},
	} {
		response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":4,"method":%q,"params":%s}`, call.method, call.params))
		if response.Error == nil || response.Error.Data == nil {
			t.Fatalf("%s silently omitted oversized evidence or bypassed the response budget", call.method)
		}
		data, _ := response.Error.Data.(map[string]any)
		if data["code"] != "MESSAGE_BUDGET_EXCEEDED" {
			t.Fatalf("%s returned an unrelated refusal: %+v", call.method, response.Error)
		}
	}
}

func decodeCompletion2270RPC(t *testing.T, response rpcResponse, result any) {
	t.Helper()
	if response.Error != nil {
		t.Fatalf("public completion read: %+v", response.Error)
	}
	raw, err := json.Marshal(response.Result)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, result); err != nil {
		t.Fatal(err)
	}
}

func seedCompletion2270Agent(t *testing.T, ctx context.Context, selected completion2270PublicStore, identity agentidentity.Identity) {
	t.Helper()
	if err := storetest.UpsertStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{
		Config: withAPITestIntent(t, actors.AgentConfig{Identity: identity, ID: identity.AgentID(), Role: "researcher", Type: "managed", Model: "regular", ExecutionMode: "live", ResolvedLLMBackend: "claude_cli", FlowPath: identity.FlowInstance()}),
		Status: "active", StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
}
