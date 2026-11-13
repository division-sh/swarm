package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/google/uuid"
)

type transcriptNativeStore interface {
	storetest.AgentFixtureStore
	storetest.ManagedAgentTurnFixtureStore
	storetest.RunFixtureStore
	sessions.Registry
	operatorread.ConversationReader
	operatorread.AgentReader
	ListAuthorActivity(context.Context, authoractivity.ListOptions) (authoractivity.ListResult, error)
}

func TestAgentTurnsOwnPublicConversationTranscriptBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, adapter := range []string{"openai_compatible", "openai_responses", "mock_python"} {
			t.Run(backend+"/"+adapter, func(t *testing.T) { proveProviderTranscriptSettlement(t, backend, adapter) })
		}
	}
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
				ctx = originalAPIFixtureContext(t, ctx, selected)
				storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: at.Add(-time.Hour)})
				storetest.RequireStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{
					Config: runtimeactors.AgentConfig{
						ID: agentID, Identity: identity, Role: "worker", Type: "managed", Model: "cheap",
						ExecutionMode: "live", ResolvedLLMBackend: "anthropic", FlowPath: "flow/a",
						Intent: apiTestResolvedIntent(t, agentID, "Report the persisted transcript."), Memory: agentmemory.Plan{Enabled: memory},
					}, Status: "active", StartedAt: at.Add(-time.Minute),
				})
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

func proveProviderTranscriptSettlement(t *testing.T, backend, adapter string) {
	t.Helper()
	opened, reopen := completion2270HistoryStore(t, backend)
	selected := opened.(transcriptNativeStore)
	ctx := originalAPIFixtureContext(t, context.Background(), selected)
	mode, configuredBackend := effects.ExecutionModeLive, adapter
	if adapter == "mock_python" {
		mode, configuredBackend = effects.ExecutionModeMock, "mock"
	}
	ctx = effects.WithExecutionMode(ctx, mode)
	runID, sessionID := uuid.NewString(), uuid.NewString()
	identity := sqliteAgentUsageIdentityForRun(t, runID, "chat-transcript")
	at := time.Now().UTC().Truncate(time.Second)
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: at.Add(-time.Hour)})
	storetest.RequireStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{Config: runtimeactors.AgentConfig{
		ID: identity.AgentID(), Identity: identity, Role: "worker", Type: "managed", Model: "regular", ExecutionMode: mode,
		ResolvedLLMBackend: configuredBackend, FlowPath: identity.FlowInstance(), Intent: apiTestResolvedIntent(t, identity.AgentID(), "Show the selected reply."),
	}, Status: "active", StartedAt: at.Add(-time.Minute)})
	turnIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	// Tied timestamps retain the public ordinal contract by sorting the IDs.
	sort.Strings(turnIDs)
	var rawReplies []json.RawMessage
	legacy := ""
	for i, turnID := range turnIDs {
		raw := json.RawMessage(fmt.Sprintf(`{"choices":[{"message":{"content":"message-%d","reasoning_content":"private reasoning"}},{"message":{"content":"unselected"}}]}`, i+1))
		if adapter == "openai_responses" {
			raw = json.RawMessage(fmt.Sprintf(`{"output":[{"type":"reasoning","summary":[{"text":"private reasoning"}]},{"type":"message","content":[{"text":"message-%d"}]},{"type":"function_call","name":"read_flow_data","arguments":"private arguments"}]}`, i+1))
		} else if adapter == "mock_python" {
			raw = json.RawMessage(fmt.Sprintf(`{"text":"message-%d","calls":[{"name":"read_flow_data","arguments":{}}]}`, i+1))
		}
		rawReplies = append(rawReplies, raw)
		prefix := []llm.TurnBlock{{Kind: "tool_use", ToolName: "read_flow_data", Data: []byte(`{"tool_use_id":"call-1"}`)},
			{Kind: "tool_result", ToolName: "read_flow_data", Data: []byte(`{"tool_use_id":"call-1"}`)},
			{Kind: "tool_use", ToolName: "read_flow_data", Data: []byte(`{"tool_use_id":"call-2"}`)},
			{Kind: "tool_result", ToolName: "read_flow_data", Data: []byte(`{"tool_use_id":"call-2"}`)}}
		if i == 0 && adapter == "openai_responses" {
			var object map[string]any
			if err := json.Unmarshal(raw, &object); err != nil {
				t.Fatal(err)
			}
			legacy = fmt.Sprint(object["output"])
			prefix = append(prefix, llm.TurnBlock{Kind: "assistant_text", Text: legacy}, llm.TurnBlock{Kind: "outcome", Text: legacy})
		} else if i != 0 { // First row retains the previously recorded canonical blocks.
			prefix = append(prefix, llm.BuildTurnBlocks(llm.AgentTurnRecord{ResponseRaw: raw})...)
		}
		blocks := llm.BuildTurnBlocks(llm.AgentTurnRecord{TurnBlocks: prefix})
		event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "task.in", "chat-proof", "", nil, 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at, mode)
		storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, nil, pipelineobligation.ScopeSubscribed)
		storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{Store: selected, Selected: selected, Identity: identity,
			RunID: runID, SessionID: sessionID, TurnID: turnID, Event: event, TurnBlocks: blocks, ResponsePayload: raw,
			Adapter: adapter, ParseOK: true, CreatedAt: at, Latency: time.Millisecond})
	}
	page, err := selected.ListAuthorActivity(ctx, authoractivity.ListOptions{RunID: runID, Limit: 500})
	if err != nil || len(page.Occurrences) >= 500 {
		t.Fatalf("settlement activity read: %+v %v", page, err)
	}
	var reference []authoractivity.Draft
	for _, turnID := range turnIDs {
		var drafts []authoractivity.Draft
		for _, occurrence := range page.Occurrences {
			if occurrence.Projection.TurnID != turnID {
				continue
			}
			suffix, key := ":completed", "turn:"
			if occurrence.Kind == authoractivity.KindTurnToolCompleted {
				suffix, key = ":block:1", "turn-tool:"
				if occurrence.Projection.ToolUseID == "call-2" {
					suffix = ":block:3"
				}
			}
			if occurrence.SourceIdentity != turnID+suffix || occurrence.DedupKey != key+turnID+suffix {
				t.Fatalf("settlement tool ordinal/draft identity changed: %+v", occurrence)
			}
			projection := occurrence.Projection
			projection.TurnID = "same-turn"
			drafts = append(drafts, authoractivity.Draft{Kind: occurrence.Kind, Version: occurrence.Version, Transition: occurrence.Transition,
				SourceOwner: occurrence.SourceOwner, SourceIdentity: suffix, DedupKey: key + suffix, OccurredAt: occurrence.OccurredAt,
				RunID: occurrence.RunID, EntityID: occurrence.EntityID, AgentID: occurrence.AgentID, FlowID: occurrence.FlowID, Projection: projection, Failure: occurrence.Failure})
		}
		if len(drafts) != 3 || drafts[0].Kind != authoractivity.KindTurnLifecycle || drafts[1].SourceIdentity != ":block:1" || drafts[2].SourceIdentity != ":block:3" {
			t.Fatalf("settlement drafts=%+v", drafts)
		}
		if reference == nil {
			reference = drafts
		} else if !reflect.DeepEqual(reference, drafts) {
			t.Fatalf("reply altered settlement drafts: before=%+v after=%+v", reference, drafts)
		}
	}
	before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(before["agent_turns"])
	if err != nil || len(rows) != 3 {
		t.Fatalf("raw turn snapshot: rows=%d err=%v", len(rows), err)
	}
	for _, row := range rows {
		id, err := row["turn_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		for i, turnID := range turnIDs {
			if id != turnID {
				continue
			}
			response, err := row["response_payload"].Text()
			var actual, expected any
			if err != nil || json.Unmarshal([]byte(response), &actual) != nil || json.Unmarshal(rawReplies[i], &expected) != nil || !reflect.DeepEqual(actual, expected) {
				t.Fatalf("raw reply changed: %s %v", response, err)
			}
		}
	}
	for _, owner := range []transcriptNativeStore{selected, reopen().(transcriptNativeStore)} {
		provePublicTranscriptReadback(t, owner, runID, identity.AgentID(), sessionID, turnIDs)
		for i, id := range turnIDs {
			turn, err := owner.LoadOperatorPublicConversationTurn(ctx, sessionID, id)
			want := legacy
			if i != 0 {
				want = fmt.Sprintf("message-%d", i+1)
			}
			if err != nil || turn.Turn.AssistantVisibleOutput != want || turn.Turn.Outcome != want {
				t.Fatalf("fresh/legacy reply: %+v %v", turn, err)
			}
			if i != 0 && (strings.Contains(fmt.Sprint(turn.Turn.Activity), "private") || strings.Contains(fmt.Sprint(turn.Turn.Activity), "unselected")) {
				t.Fatal("private/unselected content escaped")
			}
		}
	}
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("reading legacy/fresh turns rewrote stored history: %v", err)
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
