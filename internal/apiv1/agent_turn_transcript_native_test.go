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
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type transcriptNativeStore interface {
	storetest.AgentFixtureStore
	storetest.ManagedAgentTurnFixtureStore
	storetest.RunFixtureStore
	sessions.Registry
	operatorread.ConversationReader
	operatorread.AgentReader
}

func TestAgentTurnsOwnPublicConversationTranscriptBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, memory := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/memory_%t", backend, memory), func(t *testing.T) {
				ctx := context.Background()
				var selected transcriptNativeStore
				var reopen func() transcriptNativeStore
				if backend == "sqlite" {
					location := filepath.Join(t.TempDir(), "transcript.db")
					selected, _ = storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx, location)
					reopen = func() transcriptNativeStore {
						owner, _ := storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx, location)
						return owner
					}
				} else {
					location := testutil.StartPostgresDSN(t)
					selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t, location)
					reopen = func() transcriptNativeStore {
						owner, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, location)
						return owner
					}
				}
				runID := uuid.NewString()
				const agentID = "transcript-agent"
				identity := sqliteAgentUsageIdentityForRun(t, runID, agentID)
				at := time.Now().UTC().Truncate(time.Second)
				storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: at.Add(-time.Hour)})
				storetest.RequireStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{
					Config: apiExecutionFixtureConfig(t, "anthropic", runtimeactors.AgentConfig{
						ID: agentID, Identity: identity, Role: "worker", Type: "managed", Model: "cheap",
						ExecutionMode: "live", ResolvedLLMBackend: "anthropic", FlowPath: "flow/a",
						Intent: apiTestResolvedIntent(t, agentID, "Report the persisted transcript."), Memory: agentmemory.Plan{Enabled: memory},
					}), Status: "active", StartedAt: at.Add(-time.Minute),
				})
				ctx = admitAPITestAgentExecution(t, ctx, selected, authorActivityTestSourceArtifactFact)
				sessionID := uuid.NewString()
				if memory {
					seedCtx := effects.WithDifferentOwner(ctx, effects.OwnerBuildTestInfrastructure)
					lease, err := selected.Acquire(seedCtx, agentmemory.Identity(identity), "transcript-seed")
					if err != nil {
						t.Fatal(err)
					}
					sessionID = lease.SessionID
					if result, err := selected.ReleaseOutcome(seedCtx, lease); err != nil || !result.Acknowledged {
						t.Fatalf("release initial session: %+v err=%v", result, err)
					}
				}
				turnIDs := []string{"00000000-0000-4000-8000-000000000011", "00000000-0000-4000-8000-000000000012", "00000000-0000-4000-8000-000000000013"}
				for i, turnID := range turnIDs {
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "task.in", "transcript-proof", "", nil, 0, runID, events.EventEnvelope{}, at)
					route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: identity}
					storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, pipelineobligation.ScopeSubscribed)
					storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{
						Store: selected, Selected: selected, Identity: identity, RunID: runID, SessionID: sessionID,
						TurnID: turnID, Memory: agentmemory.Plan{Enabled: memory}, Event: event, DeliveryCommitted: true,
						ParseOK: true, Latency: time.Millisecond, CreatedAt: at,
						TurnBlocks: []llm.TurnBlock{{Kind: "reasoning", Text: "private transcript detail"}, {Kind: "assistant_text", Text: fmt.Sprintf("message-%d", i+1)}},
					})
				}
				before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil {
					t.Fatal(err)
				}
				for _, owner := range []transcriptNativeStore{selected, reopen()} {
					provePublicTranscriptReadback(t, owner, runID, agentID, sessionID, turnIDs)
				}
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("public transcript readback changed application rows: err=%v", err)
				}
			})
		}
	}
}

func provePublicTranscriptReadback(t *testing.T, selected transcriptNativeStore, runID, agentID, sessionID string, turnIDs []string) {
	t.Helper()
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorAgentConversationHandlers(AgentConversationHandlerOptions{Agents: selected, Conversations: selected})})
	call := func(method string, params any, result any) {
		t.Helper()
		body, err := json.Marshal(params)
		if err != nil {
			t.Fatal(err)
		}
		response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"transcript","method":%q,"params":%s}`, method, body))
		if response.Error != nil {
			t.Fatalf("%s: %+v", method, response.Error)
		}
		data, err := json.Marshal(response.Result)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, result); err != nil {
			t.Fatal(err)
		}
	}
	var list operatorread.OperatorConversationListResult
	call("conversation.list", map[string]any{"run_id": runID, "agent_id": agentID}, &list)
	if len(list.Conversations) != 1 || list.Conversations[0].SessionID != sessionID || list.Conversations[0].MessageCount != len(turnIDs) {
		t.Fatalf("public list did not count the whole transcript: %+v", list)
	}
	var first operatorread.OperatorConversationTurnListResult
	call("conversation.list_turns", map[string]any{"session_id": sessionID, "limit": 2}, &first)
	if len(first.Turns) != 2 || first.NextCursor == "" || first.Conversation.MessageCount != len(turnIDs) || first.Turns[0].TurnID != turnIDs[2] || first.Turns[0].Ordinal != 3 || first.Turns[1].TurnID != turnIDs[1] {
		t.Fatalf("public first page lost complete count or tied ordering: %+v", first)
	}
	var last operatorread.OperatorConversationTurnListResult
	call("conversation.list_turns", map[string]any{"session_id": sessionID, "limit": 2, "cursor": first.NextCursor}, &last)
	if len(last.Turns) != 1 || last.Turns[0].TurnID != turnIDs[0] || last.Turns[0].Ordinal != 1 || last.Conversation.MessageCount != len(turnIDs) {
		t.Fatalf("public second page changed transcript identity: %+v", last)
	}
	var exact operatorread.OperatorPublicConversationTurnDetail
	call("conversation.get_turn", map[string]any{"session_id": sessionID, "turn_id": turnIDs[2]}, &exact)
	if exact.Turn.Ordinal != 3 || exact.Turn.AssistantVisibleOutput != "message-3" || exact.Session.MessageCount != len(turnIDs) {
		t.Fatalf("public exact turn disagrees with transcript pages: %+v", exact)
	}
}
