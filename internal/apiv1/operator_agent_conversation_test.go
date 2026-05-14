package apiv1

import (
	"context"
	"testing"
	"time"

	"swarm/internal/store"
)

type fakeAgentConversationReadStore struct {
	listAgentsResult              store.OperatorAgentListResult
	listAgentsErr                 error
	agentResult                   store.OperatorAgentDetail
	agentErr                      error
	listConversationsResult       store.OperatorConversationListResult
	listConversationsErr          error
	conversationResult            store.OperatorConversationDetail
	conversationErr               error
	conversationTurnResult        store.OperatorConversationTurnDetail
	conversationTurnErr           error
	currentConversationResult     *store.OperatorConversationDetail
	currentConversationErr        error
	lastAgentList                 store.OperatorAgentListOptions
	lastConversationList          store.OperatorConversationListOptions
	lastAgentID                   string
	lastConversationSessionID     string
	lastConversationTurnSessionID string
	lastConversationTurnIndex     int
	lastCurrentConversationFor    string
}

func (s *fakeAgentConversationReadStore) ListOperatorAgents(_ context.Context, opts store.OperatorAgentListOptions) (store.OperatorAgentListResult, error) {
	s.lastAgentList = opts
	return s.listAgentsResult, s.listAgentsErr
}

func (s *fakeAgentConversationReadStore) LoadOperatorAgent(_ context.Context, agentID string) (store.OperatorAgentDetail, error) {
	s.lastAgentID = agentID
	return s.agentResult, s.agentErr
}

func (s *fakeAgentConversationReadStore) ListOperatorConversations(_ context.Context, opts store.OperatorConversationListOptions) (store.OperatorConversationListResult, error) {
	s.lastConversationList = opts
	return s.listConversationsResult, s.listConversationsErr
}

func (s *fakeAgentConversationReadStore) LoadOperatorConversation(_ context.Context, sessionID string) (store.OperatorConversationDetail, error) {
	s.lastConversationSessionID = sessionID
	return s.conversationResult, s.conversationErr
}

func (s *fakeAgentConversationReadStore) LoadOperatorConversationTurn(_ context.Context, sessionID string, turnIndex int) (store.OperatorConversationTurnDetail, error) {
	s.lastConversationTurnSessionID = sessionID
	s.lastConversationTurnIndex = turnIndex
	return s.conversationTurnResult, s.conversationTurnErr
}

func (s *fakeAgentConversationReadStore) LoadCurrentOperatorConversationForAgent(_ context.Context, agentID string) (*store.OperatorConversationDetail, error) {
	s.lastCurrentConversationFor = agentID
	return s.currentConversationResult, s.currentConversationErr
}

func TestOperatorAgentConversationHandlersExposeReadOwner(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	reads := &fakeAgentConversationReadStore{
		listAgentsResult: store.OperatorAgentListResult{Agents: []store.OperatorAgentSummary{{
			AgentID:          "agent-1",
			Role:             "researcher",
			Type:             "managed",
			ModelTier:        "haiku",
			ConversationMode: "session",
			SessionScope:     "global",
			Status:           "running",
		}}},
		agentResult: store.OperatorAgentDetail{Agent: store.OperatorAgentSummary{AgentID: "agent-1", Role: "researcher"}},
		listConversationsResult: store.OperatorConversationListResult{
			Conversations: []store.OperatorConversationSummary{{
				SessionID:    "sess-1",
				AgentID:      "agent-1",
				RunID:        "run-1",
				StartedAt:    now,
				TurnCount:    1,
				MessageCount: 2,
				Status:       "active",
			}},
			NextCursor: "next",
		},
		conversationResult: store.OperatorConversationDetail{
			Conversation: store.OperatorConversationSummary{SessionID: "sess-1", AgentID: "agent-1", StartedAt: now, Status: "active"},
			Turns:        []store.OperatorConversationTurn{{TurnID: "turn-1", TriggerEventID: "evt-1", TriggerEventType: "task.started", ParseOK: true}},
		},
		conversationTurnResult: store.OperatorConversationTurnDetail{
			Session: store.OperatorConversationSummary{SessionID: "sess-1", AgentID: "agent-1", StartedAt: now, Status: "active"},
			Turn: store.OperatorConversationDeepTurn{
				TurnIndex:                   1,
				TurnID:                      "turn-1",
				StartedAt:                   now,
				CompletedAt:                 now.Add(time.Second),
				ParseOK:                     true,
				AdvertisedTools:             []string{},
				RuntimeLogEntries:           []store.OperatorRuntimeLogEntry{},
				FullPromptContextV2Reserved: true,
				RawLLMResponseV2Reserved:    true,
			},
			RuntimeLogWindowStart: now,
			RuntimeLogWindowEnd:   ptrTime(now.Add(time.Second)),
		},
		currentConversationResult: &store.OperatorConversationDetail{
			Conversation: store.OperatorConversationSummary{SessionID: "sess-current", AgentID: "agent-1", StartedAt: now, Status: "active"},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			AgentConversations: reads,
		}),
	})

	listAgents := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agents","method":"agent.list","params":{"flow":"flow/a","role":"researcher"}}`)
	if listAgents.Error != nil {
		t.Fatalf("agent.list error = %#v", listAgents.Error)
	}
	if reads.lastAgentList.Flow != "flow/a" || reads.lastAgentList.Role != "researcher" {
		t.Fatalf("agent.list options = %#v", reads.lastAgentList)
	}

	getAgent := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent","method":"agent.get","params":{"agent_id":"agent-1"}}`)
	if getAgent.Error != nil {
		t.Fatalf("agent.get error = %#v", getAgent.Error)
	}
	if reads.lastAgentID != "agent-1" {
		t.Fatalf("agent.get id = %q", reads.lastAgentID)
	}

	listConversations := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"convs","method":"conversation.list","params":{"agent_id":"agent-1","run_id":"11111111-1111-1111-1111-111111111111","limit":10,"cursor":"abc"}}`)
	if listConversations.Error != nil {
		t.Fatalf("conversation.list error = %#v", listConversations.Error)
	}
	if reads.lastConversationList.AgentID != "agent-1" || reads.lastConversationList.RunID == "" || reads.lastConversationList.Limit != 10 || reads.lastConversationList.Cursor != "abc" {
		t.Fatalf("conversation.list options = %#v", reads.lastConversationList)
	}

	getConversation := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"conv","method":"conversation.get","params":{"session_id":"sess-1"}}`)
	if getConversation.Error != nil {
		t.Fatalf("conversation.get error = %#v", getConversation.Error)
	}
	if reads.lastConversationSessionID != "sess-1" {
		t.Fatalf("conversation.get session = %q", reads.lastConversationSessionID)
	}

	getTurn := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"turn","method":"conversation.get_turn","params":{"session_id":"sess-1","turn_index":1,"include_logs":false}}`)
	if getTurn.Error != nil {
		t.Fatalf("conversation.get_turn error = %#v", getTurn.Error)
	}
	if reads.lastConversationTurnSessionID != "sess-1" || reads.lastConversationTurnIndex != 1 {
		t.Fatalf("conversation.get_turn owner args = %q/%d", reads.lastConversationTurnSessionID, reads.lastConversationTurnIndex)
	}

	current := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"current","method":"conversation.current_for_agent","params":{"agent_id":"agent-1"}}`)
	if current.Error != nil {
		t.Fatalf("conversation.current_for_agent error = %#v", current.Error)
	}
	if reads.lastCurrentConversationFor != "agent-1" {
		t.Fatalf("current_for_agent id = %q", reads.lastCurrentConversationFor)
	}
}

func TestOperatorConversationGetTurnComposesRuntimeLogOwner(t *testing.T) {
	start := time.Unix(1700000100, 0).UTC()
	end := start.Add(2 * time.Second)
	reads := &fakeAgentConversationReadStore{
		conversationTurnResult: store.OperatorConversationTurnDetail{
			Session: store.OperatorConversationSummary{SessionID: "sess-1", AgentID: "agent-1", RunID: "run-1", StartedAt: start, Status: "terminated"},
			Turn: store.OperatorConversationDeepTurn{
				TurnIndex:                   2,
				TurnID:                      "turn-2",
				StartedAt:                   start,
				CompletedAt:                 end,
				ParseOK:                     true,
				AdvertisedTools:             []string{"emit_done"},
				RuntimeLogEntries:           []store.OperatorRuntimeLogEntry{},
				FullPromptContextV2Reserved: true,
				RawLLMResponseV2Reserved:    true,
			},
			RuntimeLogWindowStart: start,
			RuntimeLogWindowEnd:   &end,
		},
	}
	observability := &fakeObservabilityReadStore{logs: []store.OperatorRuntimeLogEntry{{
		LogID:     "log-1",
		TS:        start.Add(time.Second),
		Level:     "error",
		Component: "agent",
		Source:    "agent-1",
		SessionID: "sess-1",
		Message:   "turn failed",
	}}}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			AgentConversations: reads,
			Observability:      observability,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"turn","method":"conversation.get_turn","params":{"session_id":"sess-1","turn_index":2}}`)
	if resp.Error != nil {
		t.Fatalf("conversation.get_turn error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	turn := asMap(t, result["turn"])
	logs, _ := turn["runtime_log_entries"].([]any)
	if len(logs) != 1 || asMap(t, logs[0])["log_id"] != "log-1" {
		t.Fatalf("runtime_log_entries = %#v", turn["runtime_log_entries"])
	}
	if observability.lastRuntimeLogs.SessionID != "sess-1" {
		t.Fatalf("runtime log session filter = %#v", observability.lastRuntimeLogs)
	}
	if observability.lastRuntimeLogs.Since == nil || !observability.lastRuntimeLogs.Since.Equal(start) {
		t.Fatalf("runtime log since = %#v, want %s", observability.lastRuntimeLogs.Since, start)
	}
	if observability.lastRuntimeLogs.Until == nil || !observability.lastRuntimeLogs.Until.Equal(end) {
		t.Fatalf("runtime log until = %#v, want %s", observability.lastRuntimeLogs.Until, end)
	}
	if observability.lastRuntimeLogs.Order != "asc" || observability.lastRuntimeLogs.Limit != 1000 {
		t.Fatalf("runtime log options = %#v", observability.lastRuntimeLogs)
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func TestOperatorAgentConversationHandlersTypedErrors(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		body    string
		reads   *fakeAgentConversationReadStore
		wantApp string
	}{
		{
			name:    "agent missing",
			method:  "agent.get",
			body:    `{"jsonrpc":"2.0","id":"agent","method":"agent.get","params":{"agent_id":"missing"}}`,
			reads:   &fakeAgentConversationReadStore{agentErr: store.ErrAgentNotFound},
			wantApp: AgentNotFoundCode,
		},
		{
			name:    "conversation missing",
			method:  "conversation.get",
			body:    `{"jsonrpc":"2.0","id":"conv","method":"conversation.get","params":{"session_id":"missing"}}`,
			reads:   &fakeAgentConversationReadStore{conversationErr: store.ErrSessionNotFound},
			wantApp: SessionNotFoundCode,
		},
		{
			name:    "conversation turn missing session",
			method:  "conversation.get_turn",
			body:    `{"jsonrpc":"2.0","id":"turn","method":"conversation.get_turn","params":{"session_id":"missing","turn_index":1,"include_logs":false}}`,
			reads:   &fakeAgentConversationReadStore{conversationTurnErr: store.ErrSessionNotFound},
			wantApp: SessionNotFoundCode,
		},
		{
			name:    "conversation turn missing turn",
			method:  "conversation.get_turn",
			body:    `{"jsonrpc":"2.0","id":"turn","method":"conversation.get_turn","params":{"session_id":"sess-1","turn_index":99,"include_logs":false}}`,
			reads:   &fakeAgentConversationReadStore{conversationTurnErr: store.ErrTurnNotFound},
			wantApp: TurnNotFoundCode,
		},
		{
			name:    "current unknown agent",
			method:  "conversation.current_for_agent",
			body:    `{"jsonrpc":"2.0","id":"current","method":"conversation.current_for_agent","params":{"agent_id":"missing"}}`,
			reads:   &fakeAgentConversationReadStore{currentConversationErr: store.ErrAgentNotFound},
			wantApp: AgentNotFoundCode,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					AgentConversations: tc.reads,
				}),
			})
			resp := rpcCall(t, handler, tc.body)
			if resp.Error == nil {
				t.Fatalf("%s returned no error", tc.method)
			}
			data := asMap(t, resp.Error.Data)
			if data["code"] != tc.wantApp {
				t.Fatalf("error code = %#v, want %s", data["code"], tc.wantApp)
			}
		})
	}
}
