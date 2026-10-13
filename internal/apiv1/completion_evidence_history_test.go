package apiv1

import (
	"context"
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
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestCompletion2270PublicHistorySurvivesReopenBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, memory := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/memory_%t", backend, memory), func(t *testing.T) {
				selected, reopen := completion2270HistoryStore(t, backend)
				ctx := context.Background()
				runID, sessionID, turnID := uuid.NewString(), uuid.NewString(), uuid.NewString()
				identity := sqliteAgentUsageIdentityForRun(t, runID, "completion-history-2270")
				storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin()})
				actor := withAPITestIntent(t, actors.AgentConfig{Identity: identity, ID: identity.AgentID(), Role: "researcher", Type: "managed", Model: "regular",
					ExecutionMode: "live", ResolvedLLMBackend: "claude_cli", FlowPath: identity.FlowInstance(), Memory: agentmemory.Plan{Enabled: memory}})
				storetest.RequireStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{Config: actor, Status: "active", StartedAt: time.Now().UTC()})
				if memory {
					sessionID = completion2270HistorySession(t, selected.(sessions.Registry), identity)
				}
				want, frameHash := persistCompletion2270History(t, selected, identity, sessionID, turnID, memory)
				actor.Model = "cheap"
				storetest.RequireStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{Config: actor, Status: "active", StartedAt: time.Now().UTC()})
				before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil {
					t.Fatal(err)
				}
				for _, owner := range []completion2270PublicStore{selected, reopen()} {
					proveCompletion2270FrozenPublicHistory(t, owner, identity, sessionID, turnID, frameHash, want)
				}
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("history reads mutated native application rows: %v", err)
				}
			})
		}
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
	lease, err := selected.Acquire(ctx, agentmemory.Identity(identity), "completion-history-seed")
	if err != nil {
		t.Fatal(err)
	}
	if result, err := selected.ReleaseOutcome(ctx, lease); err != nil || !result.Acknowledged {
		t.Fatalf("release seeded memory session: %+v, %v", result, err)
	}
	return lease.SessionID
}

func persistCompletion2270History(t *testing.T, selected completion2270PublicStore, identity agentidentity.Identity, sessionID, turnID string, memory bool) (effects.CompletionEvidence, string) {
	t.Helper()
	ctx, at := context.Background(), time.Now().UTC()
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "completion.history", "completion-2270", "", []byte(`{}`), 0, identity.RunID, events.EventEnvelope{Scope: events.EventScopeGlobal}, at)
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, nil, "subscribed")
	input, output, total := int64(12), int64(3), 0.125
	request := effects.CompletionRequest{ModelAlias: "regular", RequestedModel: "frozen-request-A", BackendProfile: "claude_cli", Provider: "claude", Transport: "cli"}
	evidence := effects.CompletionEvidence{Request: request, Models: []effects.CompletionModelObservation{{Model: "reported-B", Source: effects.CompletionModelCLITerminal},
		{Model: "contributor-C", Source: effects.CompletionModelCLIUsage, InputTokens: &input, ProviderReportedCostUSD: &total}}, CostProvenance: effects.CompletionCostReported, ProviderReportedCostUSD: &total}
	usage := effects.CompletionUsage{Evidence: evidence.Canonical(), ResolvedModel: "reported-B", Exactness: effects.CompletionUsageExact, InputTokens: &input, OutputTokens: &output, ProviderReportedCostUSD: &total}
	result := storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{Store: selected, Selected: selected, Identity: identity, RunID: identity.RunID, SessionID: sessionID,
		TurnID: turnID, Memory: agentmemory.Plan{Enabled: memory}, Event: storetest.LoadCanonicalEventRecord(t, ctx, selected, event.ID()), Adapter: "claude_cli", RequestedModel: request.RequestedModel,
		Usage: &usage, CostUSD: total, ParseOK: true, CreatedAt: at})
	return evidence.Canonical(), result.Frame.ContentHash
}

func proveCompletion2270FrozenPublicHistory(t *testing.T, selected completion2270PublicStore, identity agentidentity.Identity, sessionID, turnID, frameHash string, want effects.CompletionEvidence) {
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
	if len(usage.Breakdown) != 1 || usage.Breakdown[0].CompletionEvidence == nil || !reflect.DeepEqual(*usage.Breakdown[0].CompletionEvidence, want) || usage.Usage.Exact.LedgerEntries != 1 || usage.Usage.Exact.ProviderReportedCostUSD != 0.125 {
		t.Fatalf("usage reconstructed history or double-charged contributors: %+v", usage)
	}
}
