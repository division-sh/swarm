package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/google/uuid"
)

func TestCompletion2270PublicHistorySurvivesReopenBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, memory := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/memory_%t", backend, memory), func(t *testing.T) {
				selected, reopen := completion2270HistoryStore(t, backend)
				ctx := originalAPIFixtureContext(t, context.Background(), selected)
				runID, sessionID, turnID := uuid.NewString(), uuid.NewString(), uuid.NewString()
				identity := sqliteAgentUsageIdentityForRun(t, runID, "completion-history-2270")
				ctx = apiTestOriginalProcessContext(t, ctx, selected)
				storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin()})
				actor := withAPITestIntent(t, actors.AgentConfig{Identity: identity, ID: identity.AgentID(), Role: "researcher", Type: "managed", Model: "regular",
					ExecutionMode: "live", ResolvedLLMBackend: "claude_cli", FlowPath: identity.FlowInstance(), Memory: agentmemory.Plan{Enabled: memory}})
				storetest.RequireStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{Config: actor, Status: "active", StartedAt: time.Now().UTC()})
				if memory {
					sessionID = completion2270HistorySession(t, selected.(sessions.Registry), identity)
				}
				want, frameHash := persistCompletion2270History(t, selected, identity, sessionID, turnID, memory, 0.125)
				actor.Model = "cheap"
				storetest.RequireStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{Config: actor, Status: "active", StartedAt: time.Now().UTC()})
				before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil {
					t.Fatal(err)
				}
				for _, owner := range []completion2270PublicStore{selected, reopen()} {
					proveCompletion2270FrozenPublicHistory(t, owner, identity, sessionID, turnID, frameHash, want, 0.125)
				}
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("history reads mutated native application rows: %v", err)
				}
			})
		}
	}
}

func TestCompletion2270SubNanodollarPublicHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, reopen := completion2270HistoryStore(t, backend)
			ctx := originalAPIFixtureContext(t, context.Background(), selected)
			runID, sessionID, turnID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			identity := sqliteAgentUsageIdentityForRun(t, runID, "fractional-price-2270")
			ctx = apiTestOriginalProcessContext(t, ctx, selected)
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin()})
			seedCompletion2270Agent(t, ctx, selected, identity)
			want, frameHash := persistCompletion2270History(t, selected, identity, sessionID, turnID, false, 0.0000000004)
			actor := withAPITestIntent(t, actors.AgentConfig{Identity: identity, ID: identity.AgentID(), Role: "researcher", Type: "managed", Model: "cheap",
				ExecutionMode: "live", ResolvedLLMBackend: "claude_cli", FlowPath: identity.FlowInstance()})
			storetest.RequireStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{Config: actor, Status: "active", StartedAt: time.Now().UTC()})
			charged := 0.0000000004
			if backend == "postgres" {
				charged = 0 // The existing spend_ledger NUMERIC(18,9) contract is unchanged.
			}
			for _, owner := range []completion2270PublicStore{selected, reopen()} {
				proveCompletion2270FrozenPublicHistory(t, owner, identity, sessionID, turnID, frameHash, want, charged)
			}
		})
	}
}

func TestConversationTurnTokenCostEstimateBothStores(t *testing.T) {
	const version = "anthropic-standard-2026-11-09"
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, reopen := completion2270HistoryStore(t, backend)
			ctx := originalAPIFixtureContext(t, context.Background(), selected)
			ctx = apiTestOriginalProcessContext(t, ctx, selected)
			runID, sessionID := uuid.NewString(), uuid.NewString()
			identity := sqliteAgentUsageIdentityForRun(t, runID, "token-cost")
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin()})
			storetest.RequireStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{
				Config: withAPITestIntent(t, actors.AgentConfig{Identity: identity, ID: identity.AgentID(), Role: "researcher", Type: "managed", Model: "regular",
					ExecutionMode: "live", ResolvedLLMBackend: "anthropic", FlowPath: identity.FlowInstance()}),
				Status: "active", StartedAt: time.Now().UTC(),
			})
			input, output := int64(2300), int64(2000)
			at := time.Now().UTC()
			type fixtureTurn struct {
				model, id    string
				charge, want float64
				evidence     effects.CompletionEvidence
			}
			fixtures := []fixtureTurn{
				{model: "claude-3-5-sonnet", id: uuid.NewString(), charge: 0.0369, want: -1},
				{model: "claude-haiku-4-5-20251001", id: uuid.NewString(), charge: 0.00984, want: 0.0123},
			}
			for i := range fixtures {
				f := &fixtures[i]
				f.evidence = effects.CompletionEvidence{
					Request: effects.CompletionRequest{ModelAlias: "regular", RequestedModel: f.model, BackendProfile: "anthropic", Provider: "anthropic", Transport: "api"},
					Models:  []effects.CompletionModelObservation{{Model: f.model, Source: effects.CompletionModelResponse}}, CostProvenance: effects.CompletionCostEstimated,
				}.Canonical()
				usage := effects.CompletionUsage{Evidence: f.evidence, ResolvedModel: f.model, Exactness: effects.CompletionUsageExact, InputTokens: &input, OutputTokens: &output}
				when := at.Add(time.Duration(i) * time.Second)
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "completion.history", "token-cost", "", []byte(`{}`), 0, runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, when)
				storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, nil, "subscribed")
				storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{Store: selected, Selected: selected, Identity: identity,
					RunID: runID, SessionID: sessionID, TurnID: f.id, Event: storetest.LoadCanonicalEventRecord(t, ctx, selected, event.ID()),
					Adapter: "anthropic_api", RequestedModel: f.model, Usage: &usage, CostUSD: f.charge, ParseOK: true, CreatedAt: when})
			}
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			var priorUsage operatorread.OperatorAgentUsage
			for i, owner := range []completion2270PublicStore{selected, reopen()} {
				handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorAgentConversationHandlers(AgentConversationHandlerOptions{Agents: owner, Conversations: owner, Usage: owner})})
				for _, f := range fixtures {
					response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"conversation.get_turn","params":{"session_id":%q,"turn_id":%q}}`, sessionID, f.id))
					var detail operatorread.OperatorPublicConversationTurnDetail
					decodeCompletion2270RPC(t, response, &detail)
					if detail.Turn.Tokens == nil || *detail.Turn.Tokens != (operatorread.OperatorConversationTokenUsage{Input: input, Output: output, Exactness: "exact"}) ||
						!reflect.DeepEqual(detail.Turn.CompletionEvidence, f.evidence) {
						t.Fatalf("detail changed the retained facts: %+v", detail.Turn)
					}
					estimate, present := asMap(t, asMap(t, response.Result)["turn"])["token_cost_estimate"]
					if f.want < 0 && present || f.want >= 0 && (!present || !reflect.DeepEqual(estimate, map[string]any{"usd": f.want, "rate_version": version})) {
						t.Fatalf("detail estimate for %s: %v, present=%v, want %v", f.model, estimate, present, f.want)
					}
				}
				list := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"conversation.list_turns","params":{"session_id":%q,"limit":2}}`, sessionID))
				rows, ok := asMap(t, list.Result)["turns"].([]any)
				if !ok || len(rows) != len(fixtures) {
					t.Fatalf("list: %+v", list)
				}
				for _, raw := range rows {
					if _, present := asMap(t, raw)["token_cost_estimate"]; present {
						t.Fatal("list included the detail-only estimate")
					}
				}
				latest, err := owner.(interface {
					LoadLatestPublicConversationTurn(context.Context, string) (*operatorread.OperatorPublicConversationTurn, error)
				}).LoadLatestPublicConversationTurn(ctx, sessionID)
				if err != nil || latest == nil || latest.TurnID != fixtures[1].id {
					t.Fatalf("latest changed: %+v, %v", latest, err)
				}
				raw, err := json.Marshal(latest)
				var latestFields map[string]json.RawMessage
				if err != nil || json.Unmarshal(raw, &latestFields) != nil || latestFields["token_cost_estimate"] != nil {
					t.Fatalf("latest leaked the detail estimate: %s, %v", raw, err)
				}
				var usage operatorread.OperatorAgentUsage
				decodeCompletion2270RPC(t, rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":3,"method":"agent.usage","params":{"run_id":%q,"agent_id":%q}}`, runID, identity.AgentID())), &usage)
				if usage.Usage.Exact.LedgerEntries != 2 || usage.Usage.Exact.InputTokens != 2*input || usage.Usage.Exact.OutputTokens != 2*output ||
					usage.Usage.Exact.EstimatedCostUSD < 0.046739 || usage.Usage.Exact.EstimatedCostUSD > 0.046741 || (i > 0 && !reflect.DeepEqual(usage, priorUsage)) {
					t.Fatalf("usage changed or used the detail estimate: %+v", usage)
				}
				priorUsage = usage
			}
			after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("price projection mutated native storage: %v", err)
			}
		})
	}
}

func completion2270HistoryStore(t *testing.T, backend string) (completion2270PublicStore, func() completion2270PublicStore) {
	t.Helper()
	if backend == "sqlite" {
		location := filepath.Join(t.TempDir(), "completion-history.db")
		selected, _ := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background(), location)
		return selected, func() completion2270PublicStore {
			owner, _ := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background(), location)
			return owner
		}
	}
	location := testutil.StartPostgresDSN(t)
	selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, location)
	return selected, func() completion2270PublicStore {
		owner, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, location)
		return owner
	}
}

func completion2270HistorySession(t *testing.T, selected sessions.Registry, identity agentidentity.Identity) string {
	t.Helper()
	ctx := effects.WithDifferentOwner(context.Background(), effects.OwnerBuildTestInfrastructure)
	ctx = originalAPIFixtureContext(t, ctx, selected)
	lease, err := selected.Acquire(ctx, agentmemory.Identity(identity), "completion-history-seed")
	if err != nil {
		t.Fatal(err)
	}
	if result, err := selected.ReleaseOutcome(ctx, lease); err != nil || !result.Acknowledged {
		t.Fatalf("release seeded memory session: %+v, %v", result, err)
	}
	return lease.SessionID
}

func persistCompletion2270History(t *testing.T, selected completion2270PublicStore, identity agentidentity.Identity, sessionID, turnID string, memory bool, total float64) (effects.CompletionEvidence, string) {
	t.Helper()
	ctx, at := context.Background(), time.Now().UTC()
	ctx = originalAPIFixtureContext(t, ctx, selected)
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "completion.history", "completion-2270", "", []byte(`{}`), 0, identity.RunID, events.EventEnvelope{Scope: events.EventScopeGlobal}, at)
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, nil, "subscribed")
	input, output := int64(12), int64(3)
	request := effects.CompletionRequest{ModelAlias: "regular", RequestedModel: "frozen-request-A", BackendProfile: "claude_cli", Provider: "claude", Transport: "cli"}
	evidence := effects.CompletionEvidence{Request: request, Models: []effects.CompletionModelObservation{{Model: "reported-B", Source: effects.CompletionModelCLITerminal},
		{Model: "contributor-C", Source: effects.CompletionModelCLIUsage, InputTokens: &input, ProviderReportedCostUSD: &total}}, CostProvenance: effects.CompletionCostReported, ProviderReportedCostUSD: &total}
	usage := effects.CompletionUsage{Evidence: evidence.Canonical(), ResolvedModel: "reported-B", Exactness: effects.CompletionUsageExact, InputTokens: &input, OutputTokens: &output, ProviderReportedCostUSD: &total}
	result := storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{Store: selected, Selected: selected, Identity: identity, RunID: identity.RunID, SessionID: sessionID,
		TurnID: turnID, Memory: agentmemory.Plan{Enabled: memory}, Event: storetest.LoadCanonicalEventRecord(t, ctx, selected, event.ID()), Adapter: "claude_cli", RequestedModel: request.RequestedModel,
		Usage: &usage, CostUSD: total, ParseOK: true, CreatedAt: at})
	return evidence.Canonical(), result.Frame.ContentHash
}

func proveCompletion2270FrozenPublicHistory(t *testing.T, selected completion2270PublicStore, identity agentidentity.Identity, sessionID, turnID, frameHash string, want effects.CompletionEvidence, charged float64) {
	t.Helper()
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorAgentConversationHandlers(AgentConversationHandlerOptions{Agents: selected, Conversations: selected, Usage: selected})})
	params := fmt.Sprintf(`{"run_id":%q,"agent_id":%q}`, identity.RunID, identity.AgentID())
	var agent operatorread.OperatorAgentDetail
	decodeCompletion2270RPC(t, rpcCall(t, handler, `{"jsonrpc":"2.0","id":1,"method":"agent.get","params":`+params+`}`), &agent)
	if agent.Agent.Model != "cheap" {
		t.Fatalf("current configuration was overwritten by history: %+v", agent)
	}
	var page operatorread.OperatorConversationTurnListResult
	decodeCompletion2270RPC(t, rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"conversation.list_turns","params":{"session_id":%q,"limit":1}}`, sessionID)), &page)
	if len(page.Turns) != 1 || !reflect.DeepEqual(page.Turns[0].CompletionEvidence, want) || page.Turns[0].ResolvedModel != "reported-B" {
		t.Fatalf("page reconstructed history from current configuration: %+v", page)
	}
	var detail operatorread.OperatorPublicConversationTurnDetail
	decodeCompletion2270RPC(t, rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":3,"method":"conversation.get_turn","params":{"session_id":%q,"turn_id":%q}}`, sessionID, turnID)), &detail)
	if !reflect.DeepEqual(detail.Turn.CompletionEvidence, want) || detail.Frame.ContentHash != frameHash {
		t.Fatalf("detail lost the admitted frame/evidence: %+v", detail)
	}
	var usage operatorread.OperatorAgentUsage
	decodeCompletion2270RPC(t, rpcCall(t, handler, `{"jsonrpc":"2.0","id":4,"method":"agent.usage","params":`+params+`}`), &usage)
	if len(usage.Breakdown) != 1 || usage.Breakdown[0].CompletionEvidence == nil || !reflect.DeepEqual(*usage.Breakdown[0].CompletionEvidence, want) || usage.Usage.Exact.LedgerEntries != 1 || usage.Usage.Exact.ProviderReportedCostUSD != charged || usage.Usage.Exact.CostUSD != charged {
		t.Fatalf("usage reconstructed history or double-charged contributors: %+v", usage)
	}
}
