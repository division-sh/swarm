package apiv1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"swarm/internal/store"
)

type fakeAgentConversationReadStore struct {
	listAgentsResult              store.OperatorAgentListResult
	listAgentsErr                 error
	agentResult                   store.OperatorAgentDetail
	agentErr                      error
	agentDiagnosisResult          store.OperatorAgentDiagnosis
	agentDiagnosisErr             error
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
	lastAgentDiagnosisID          string
	lastAgentDiagnosisOptions     store.OperatorAgentDiagnosisOptions
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

func (s *fakeAgentConversationReadStore) LoadOperatorAgentDiagnosis(_ context.Context, agentID string, opts store.OperatorAgentDiagnosisOptions) (store.OperatorAgentDiagnosis, error) {
	s.lastAgentDiagnosisID = agentID
	s.lastAgentDiagnosisOptions = opts
	return s.agentDiagnosisResult, s.agentDiagnosisErr
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

type apiConversationCapabilitySource struct {
	caps store.StoreSchemaCapabilities
}

func (s apiConversationCapabilitySource) ResolveSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error) {
	return s.caps, nil
}

func apiConversationReadCaps(sessionRunID, auditRunID bool) store.StoreSchemaCapabilities {
	return store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions:     store.SchemaFlavorCanonical,
			Audits:       store.SchemaFlavorCanonical,
			Turns:        store.SchemaFlavorCanonical,
			SessionRunID: sessionRunID,
			AuditRunID:   auditRunID,
		},
	}
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
		agentDiagnosisResult: store.OperatorAgentDiagnosis{
			AgentID: "agent-1",
			Status:  "running",
			Queue: store.OperatorAgentDiagnosisQueue{
				PendingCount:            2,
				OldestPendingAgeSeconds: 45,
				PendingDeliveries: []store.OperatorAgentPendingDelivery{{
					EventID:    "event-1",
					EventName:  "task.ready",
					EnqueuedAt: time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC),
					Attempts:   1,
				}},
				NextCursor: "cursor-2",
			},
			DeliveryLifecycle: &store.OperatorAgentDeliveryLifecycle{
				State:         "active",
				BlockingLayer: "session_execution",
			},
			RuntimeState: &store.OperatorAgentDiagnosisRuntimeState{
				Watchdog: &store.OperatorAgentDiagnosisWatchdog{
					State:         "no_output",
					BlockingLayer: "session_execution",
					Action:        "session_no_output",
					Outcome:       "warning_emitted",
					RecordedAt:    "2026-05-21T10:01:00Z",
				},
			},
		},
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
			Turns:        []store.OperatorConversationTurn{{TurnIndex: 1, TurnID: "turn-1", TriggerEventID: "evt-1", TriggerEventType: "task.started", ParseOK: true}},
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
			Turns:        []store.OperatorConversationTurn{{TurnIndex: 1, TurnID: "turn-current-1", TriggerEventID: "evt-current-1", TriggerEventType: "task.started", ParseOK: true}},
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
	listResult := asMap(t, listAgents.Result)
	agents, ok := listResult["agents"].([]any)
	if !ok || len(agents) != 1 {
		t.Fatalf("agent.list result = %#v", listResult)
	}
	assertUnsupportedAgentMetricStubsAbsent(t, asMap(t, agents[0]))

	getAgent := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent","method":"agent.get","params":{"agent_id":"agent-1"}}`)
	if getAgent.Error != nil {
		t.Fatalf("agent.get error = %#v", getAgent.Error)
	}
	if reads.lastAgentID != "agent-1" {
		t.Fatalf("agent.get id = %q", reads.lastAgentID)
	}
	agentResult := asMap(t, getAgent.Result)
	agent := asMap(t, agentResult["agent"])
	assertUnsupportedAgentMetricStubsAbsent(t, agent)
	for _, splitField := range []string{"queue", "delivery_lifecycle", "runtime_state", "last_tool_outcome"} {
		if _, ok := agent[splitField]; ok {
			t.Fatalf("agent.get unexpectedly exposed diagnosis field %q: %#v", splitField, agent)
		}
	}

	diagnoseAgent := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"agent_id":"agent-1","queue_limit":1,"queue_cursor":"cursor-1"}}`)
	if diagnoseAgent.Error != nil {
		t.Fatalf("agent.diagnose error = %#v", diagnoseAgent.Error)
	}
	if reads.lastAgentDiagnosisID != "agent-1" {
		t.Fatalf("agent.diagnose id = %q", reads.lastAgentDiagnosisID)
	}
	if reads.lastAgentDiagnosisOptions.QueueLimit != 1 || reads.lastAgentDiagnosisOptions.QueueCursor != "cursor-1" {
		t.Fatalf("agent.diagnose options = %#v", reads.lastAgentDiagnosisOptions)
	}
	diagnosis := asMap(t, diagnoseAgent.Result)
	if diagnosis["agent_id"] != "agent-1" || diagnosis["status"] != "running" {
		t.Fatalf("agent.diagnose identity/status = %#v", diagnosis)
	}
	queue := asMap(t, diagnosis["queue"])
	if queue["pending_count"] != float64(2) || queue["oldest_pending_age_seconds"] != float64(45) {
		t.Fatalf("agent.diagnose queue = %#v", queue)
	}
	deliveries, ok := queue["pending_deliveries"].([]any)
	if !ok || len(deliveries) != 1 {
		t.Fatalf("agent.diagnose pending_deliveries = %#v", queue["pending_deliveries"])
	}
	delivery := asMap(t, deliveries[0])
	if delivery["event_id"] != "event-1" || delivery["event_name"] != "task.ready" || delivery["attempts"] != float64(1) {
		t.Fatalf("agent.diagnose pending delivery = %#v", delivery)
	}
	if queue["next_cursor"] != "cursor-2" {
		t.Fatalf("agent.diagnose next_cursor = %#v", queue["next_cursor"])
	}
	lifecycle := asMap(t, diagnosis["delivery_lifecycle"])
	if lifecycle["state"] != "active" || lifecycle["blocking_layer"] != "session_execution" {
		t.Fatalf("agent.diagnose lifecycle = %#v", lifecycle)
	}
	runtimeState := asMap(t, diagnosis["runtime_state"])
	watchdog := asMap(t, runtimeState["watchdog"])
	if watchdog["state"] != "no_output" || watchdog["blocking_layer"] != "session_execution" || watchdog["action"] != "session_no_output" || watchdog["outcome"] != "warning_emitted" {
		t.Fatalf("agent.diagnose runtime_state.watchdog = %#v", watchdog)
	}
	if watchdog["recorded_at"] != "2026-05-21T10:01:00Z" {
		t.Fatalf("agent.diagnose runtime_state.watchdog.recorded_at = %#v", watchdog["recorded_at"])
	}
	for _, splitField := range []string{"bundle_version", "active", "watchdog", "last_tool_outcome", "token_usage", "failures_recent", "dead_letters_recent"} {
		if _, ok := diagnosis[splitField]; ok {
			t.Fatalf("agent.diagnose exposed split field %q: %#v", splitField, diagnosis)
		}
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
	conversationTurns, _ := asMap(t, getConversation.Result)["turns"].([]any)
	if len(conversationTurns) != 1 || asMap(t, conversationTurns[0])["turn_index"] != float64(1) {
		t.Fatalf("conversation.get turns = %#v, want turn_index 1", asMap(t, getConversation.Result)["turns"])
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
	currentTurns, _ := asMap(t, current.Result)["turns"].([]any)
	if len(currentTurns) != 1 || asMap(t, currentTurns[0])["turn_index"] != float64(1) {
		t.Fatalf("conversation.current_for_agent turns = %#v, want turn_index 1", asMap(t, current.Result)["turns"])
	}
}

func assertUnsupportedAgentMetricStubsAbsent(t *testing.T, payload map[string]any) {
	t.Helper()
	for _, key := range []string{"turns_24h", "in_flight_seconds"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("agent read exposed unsupported metric stub %q: %#v", key, payload)
		}
	}
}

func TestOperatorAgentConversationHandlersSanitizeRunIDProjectionFailures(t *testing.T) {
	rawRunIDColumnErr := errors.New(`pq: column "run_id" does not exist at position 46:14 (42703)`)
	tests := []struct {
		name   string
		body   string
		caps   store.StoreSchemaCapabilities
		expect func(sqlmock.Sqlmock)
	}{
		{
			name: "conversation list raw projection error",
			body: `{"jsonrpc":"2.0","id":"convs","method":"conversation.list","params":{"limit":20}}`,
			caps: apiConversationReadCaps(true, true),
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("(?s)SELECT\\s+conversations\\.session_id,\\s+conversations\\.agent_id,\\s+conversations\\.run_id,.*FROM \\(").
					WithArgs(21).
					WillReturnError(rawRunIDColumnErr)
			},
		},
		{
			name: "conversation get raw projection error",
			body: `{"jsonrpc":"2.0","id":"conv","method":"conversation.get","params":{"session_id":"sess-1"}}`,
			caps: apiConversationReadCaps(true, true),
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("(?s)SELECT\\s+session_id,\\s+agent_id,\\s+run_id,.*FROM \\(.*\\) conversations\\s+WHERE session_id = \\$1").
					WithArgs("sess-1").
					WillReturnError(rawRunIDColumnErr)
			},
		},
		{
			name: "conversation get_turn raw projection error",
			body: `{"jsonrpc":"2.0","id":"turn","method":"conversation.get_turn","params":{"session_id":"sess-1","turn_index":1,"include_logs":true}}`,
			caps: apiConversationReadCaps(true, true),
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("(?s)SELECT\\s+session_id,\\s+agent_id,\\s+run_id,.*FROM \\(.*\\) conversations\\s+WHERE session_id = \\$1").
					WithArgs("sess-1").
					WillReturnError(rawRunIDColumnErr)
			},
		},
		{
			name: "conversation list run_id filter without any run_id capability",
			body: `{"jsonrpc":"2.0","id":"convs","method":"conversation.list","params":{"run_id":"11111111-1111-1111-1111-111111111111"}}`,
			caps: apiConversationReadCaps(false, false),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			if tc.expect != nil {
				tc.expect(mock)
			}

			reader := store.NewOperatorConversationReadSurface(db, apiConversationCapabilitySource{caps: tc.caps})
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					AgentConversations: reader,
				}),
			})

			resp := rpcCall(t, handler, tc.body)
			assertConversationRunIDErrorSanitized(t, resp)
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("expectations: %v", err)
			}
		})
	}
}

func assertConversationRunIDErrorSanitized(t *testing.T, resp rpcResponse) {
	t.Helper()
	if resp.Error == nil {
		t.Fatal("response error = nil, want sanitized run_id capability error")
	}
	if resp.Error.Code != codeInternalError {
		t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
	}
	text := strings.ToLower(resp.Error.Message + " " + fmt.Sprint(resp.Error.Data))
	if !strings.Contains(text, "operator conversation read surface run_id capability unavailable") {
		t.Fatalf("error data = %s, want stable run_id capability error", text)
	}
	for _, forbidden := range []string{"pq:", `column "run_id"`, "42703", "position"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("error data leaked %q: %s", forbidden, text)
		}
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
			name:    "agent diagnosis missing",
			method:  "agent.diagnose",
			body:    `{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"agent_id":"missing"}}`,
			reads:   &fakeAgentConversationReadStore{agentDiagnosisErr: store.ErrAgentNotFound},
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

func TestOperatorAgentDiagnoseFailsClosedOnMalformedOwnerData(t *testing.T) {
	reads := &fakeAgentConversationReadStore{
		agentDiagnosisResult: store.OperatorAgentDiagnosis{
			AgentID: "agent-1",
			Status:  "running",
			Queue: store.OperatorAgentDiagnosisQueue{
				PendingDeliveries: []store.OperatorAgentPendingDelivery{{
					EventName:  "task.ready",
					EnqueuedAt: time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC),
				}},
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			AgentConversations: reads,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"agent_id":"agent-1"}}`)
	if resp.Error == nil {
		t.Fatal("agent.diagnose returned success for malformed owner result")
	}
	if resp.Error.Code != codeInternalError {
		t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
	}
	if !strings.Contains(fmt.Sprint(resp.Error.Message, resp.Error.Data), "agent.diagnose owner returned malformed result") {
		t.Fatalf("error = %#v, want malformed owner result", resp.Error)
	}
}

func TestOperatorAgentDiagnoseFailsClosedOnMalformedWatchdogOwnerData(t *testing.T) {
	reads := &fakeAgentConversationReadStore{
		agentDiagnosisResult: store.OperatorAgentDiagnosis{
			AgentID: "agent-1",
			Status:  "running",
			Queue: store.OperatorAgentDiagnosisQueue{
				PendingDeliveries: []store.OperatorAgentPendingDelivery{},
			},
			RuntimeState: &store.OperatorAgentDiagnosisRuntimeState{
				Watchdog: &store.OperatorAgentDiagnosisWatchdog{
					State:         "healthy_long_running",
					BlockingLayer: "session_execution",
					Action:        "turn_long_running",
					Outcome:       "observed",
					RecordedAt:    "2026-05-21T10:01:00Z",
				},
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			AgentConversations: reads,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"agent_id":"agent-1"}}`)
	if resp.Error == nil {
		t.Fatal("agent.diagnose returned success for malformed watchdog owner result")
	}
	if resp.Error.Code != codeInternalError {
		t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
	}
	errorText := fmt.Sprint(resp.Error.Message, resp.Error.Data)
	if !strings.Contains(errorText, "agent.diagnose owner returned malformed result") || !strings.Contains(errorText, "runtime_state.watchdog") {
		t.Fatalf("error = %#v, want malformed runtime_state.watchdog owner result", resp.Error)
	}
}

func TestOperatorAgentDiagnoseRejectsQueueLimit(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			AgentConversations: &fakeAgentConversationReadStore{},
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-diagnose","method":"agent.diagnose","params":{"agent_id":"agent-1","queue_limit":0}}`)
	if resp.Error == nil {
		t.Fatal("agent.diagnose returned success for invalid queue_limit")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.diagnose", resp, "queue_limit")
}

func TestOperatorAgentDiagnoseRejectsBadQueueCursor(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			AgentConversations: &fakeAgentConversationReadStore{agentDiagnosisErr: store.ErrInvalidPendingAgentDeliveryCursor},
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-diagnose","method":"agent.diagnose","params":{"agent_id":"agent-1","queue_cursor":"bad"}}`)
	if resp.Error == nil {
		t.Fatal("agent.diagnose returned success for invalid queue_cursor")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.diagnose", resp, "queue_cursor")
}
