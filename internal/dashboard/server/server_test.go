package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	builderpkg "swarm/internal/builder"
	"swarm/internal/events"
	runtimepkg "swarm/internal/runtime"
	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimesessions "swarm/internal/runtime/sessions"
	runtimetools "swarm/internal/runtime/tools"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

type builderRPCResponse = builderpkg.RPCResponse
type builderWSEventFrame = builderpkg.WSEventFrame

const testBuilderAuthToken = "builder-test-token"
const testOperatorAuthToken = "operator-secret"

func setOperatorAuth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+testOperatorAuthToken)
}

type stubAgents struct {
	rows []runtimemanager.PersistedAgent
}

func (s stubAgents) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.rows, nil
}

type stubMailbox struct {
	items map[string]runtimetools.MailboxItem
	list  []runtimetools.MailboxItem
}

func (s stubMailbox) ListMailboxItems(context.Context, string, int) ([]runtimetools.MailboxItem, error) {
	return s.list, nil
}

func (s stubMailbox) GetMailboxItem(_ context.Context, id string) (runtimetools.MailboxItem, error) {
	return s.items[id], nil
}

type stubInstances struct {
	rows []runtimepipeline.WorkflowInstance
	byID map[string]runtimepipeline.WorkflowInstance
}

func (s stubInstances) List(context.Context) ([]runtimepipeline.WorkflowInstance, error) {
	return s.rows, nil
}

func (s stubInstances) Load(_ context.Context, instanceID string) (runtimepipeline.WorkflowInstance, bool, error) {
	item, ok := s.byID[instanceID]
	return item, ok, nil
}

type stubConversations struct {
	list      []ConversationSummary
	bySession map[string]ConversationDetail
}

func (s stubConversations) List(context.Context, int) ([]ConversationSummary, error) {
	return s.list, nil
}

func (s stubConversations) Get(_ context.Context, sessionID string) (ConversationDetail, bool, error) {
	item, ok := s.bySession[sessionID]
	return item, ok, nil
}

type stubObservability struct {
	events      []eventRecord
	eventDetail map[string]eventRecord
	runtimeLogs []runtimeLogRecord
	incidents   []incidentRecord
}

type stubBuilderRunStore struct{}

func (stubBuilderRunStore) AppendEvent(context.Context, events.Event) error { return nil }
func (stubBuilderRunStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (stubBuilderRunStore) MarkRunTerminal(context.Context, string, string, string, time.Time) error {
	return nil
}

type stubConversationCaps struct {
	caps store.StoreSchemaCapabilities
	err  error
}

func (s stubConversationCaps) ResolveSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error) {
	return s.caps, s.err
}

type stubSQLAgents struct {
	rows []runtimemanager.PersistedAgent
	caps store.StoreSchemaCapabilities
	err  error
}

func (s stubSQLAgents) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.rows, nil
}

func (s stubSQLAgents) ResolveSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error) {
	return s.caps, s.err
}

func (s stubObservability) ListEvents(context.Context, EventFilter, int) ([]eventRecord, error) {
	return s.events, nil
}

func (s stubObservability) GetEvent(_ context.Context, id string) (eventRecord, bool, error) {
	item, ok := s.eventDetail[id]
	return item, ok, nil
}

func (s stubObservability) ListRuntimeLogs(context.Context, RuntimeLogFilter, int) ([]runtimeLogRecord, error) {
	return s.runtimeLogs, nil
}

func (s stubObservability) ListIncidents(context.Context, IncidentFilter) ([]incidentRecord, error) {
	return s.incidents, nil
}

type stubAgentControl struct{ lastKillPrevious bool }

func (stubAgentControl) RestartAgent(string) error                        { return nil }
func (stubAgentControl) ReplayAgentBacklog(context.Context, string) error { return nil }
func (s *stubAgentControl) ChatWithAgent(_ context.Context, _, _ string, killPrevious bool) (string, error) {
	s.lastKillPrevious = killPrevious
	return "ok", nil
}

type stubRuntimeControl struct {
	resetCalls  int
	pauseCalls  int
	resumeCalls int
}

func (s *stubRuntimeControl) PauseIngress()  { s.pauseCalls++ }
func (s *stubRuntimeControl) ResumeIngress() { s.resumeCalls++ }
func (s *stubRuntimeControl) ResetState() error {
	s.resetCalls++
	return nil
}

type stubProjectControl struct {
	current builderpkg.ProjectStatus
}

func (s *stubProjectControl) OpenProject(_ context.Context, projectDir string) (builderpkg.ProjectStatus, error) {
	s.current = builderpkg.ProjectStatus{
		ProjectDir:      strings.TrimSpace(projectDir),
		Loaded:          true,
		WorkflowName:    "sample",
		WorkflowVersion: "v1",
	}
	return s.current, nil
}

func (s *stubProjectControl) ReloadProject(_ context.Context, projectDir string) (builderpkg.ProjectStatus, error) {
	if strings.TrimSpace(projectDir) != "" {
		s.current.ProjectDir = strings.TrimSpace(projectDir)
	}
	s.current.Loaded = true
	return s.current, nil
}

func (s *stubProjectControl) CloseProject(context.Context) (builderpkg.ProjectStatus, error) {
	s.current = builderpkg.ProjectStatus{}
	return s.current, nil
}

func (s *stubProjectControl) CurrentProject() builderpkg.ProjectStatus {
	return s.current
}

func newBuilderHandlerForTest(
	health HealthChecker,
	instances InstanceReader,
	version string,
	runtimeCtl RuntimeController,
	rt *runtimepkg.Runtime,
	projectCtl builderpkg.ProjectController,
) http.Handler {
	var runtimeProvider builderpkg.RuntimeProvider
	if rt != nil {
		runtimeProvider = func() *runtimepkg.Runtime { return rt }
	}
	return builderpkg.NewHandler(builderpkg.Options{
		Health:         builderpkg.HealthChecker(health),
		Instances:      instances,
		Runtime:        runtimeCtl,
		AuthToken:      testBuilderAuthToken,
		Version:        version,
		CurrentRuntime: runtimeProvider,
		ProjectControl: projectCtl,
	})
}

func builderAuthRequest(method, path, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testBuilderAuthToken)
	return req
}

func builderAuthHeader() http.Header {
	return http.Header{"Authorization": []string{"Bearer " + testBuilderAuthToken}}
}

func TestHandler_ConversationsAndAggregates(t *testing.T) {
	now := time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC)
	agentCtl := &stubAgentControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"database": map[string]any{"ok": true}}, nil
		},
		AuthToken: testOperatorAuthToken,
		Agents: stubAgents{rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ID:            "agent-1",
				Role:          "worker",
				Type:          "stub",
				Mode:          "operating",
				EntityID:      "entity-1",
				Subscriptions: []string{"task.completed"},
				Permissions:   []string{"read"},
			},
			Status:    "active",
			HiredBy:   "test",
			StartedAt: now,
		}}},
		AgentControl: agentCtl,
		Conversations: stubConversations{
			list: []ConversationSummary{{
				AgentID:   "agent-1",
				Summary:   "summarized",
				UpdatedAt: now.Format(time.RFC3339),
			}},
			bySession: map[string]ConversationDetail{
				"sess-1": {
					AgentID:   "agent-1",
					SessionID: "sess-1",
					UpdatedAt: now.Format(time.RFC3339),
					Messages:  []any{map[string]any{"role": "assistant", "content": "hi"}},
					Turns: []ConversationTurn{{
						TurnID:                 "turn-1",
						AssistantVisibleOutput: "done",
					}},
					RuntimeState: map[string]any{
						"summary":   "summarized",
						"last_turn": map[string]any{"parse_ok": true},
					},
				},
			},
		},
		Observability: stubObservability{
			events: []eventRecord{{
				ID:          "evt-1",
				EventID:     "evt-1",
				Type:        "task.completed",
				CreatedAt:   now.Format(time.RFC3339),
				SourceAgent: "agent-1",
			}},
			eventDetail: map[string]eventRecord{
				"evt-1": {
					ID:      "evt-1",
					EventID: "evt-1",
					Type:    "task.completed",
					Deliveries: []eventDeliveryRecord{{
						AgentID: "agent-1",
						Status:  "success",
					}},
				},
			},
			runtimeLogs: []runtimeLogRecord{{
				ID:        "log-1",
				TS:        now.Format(time.RFC3339),
				Level:     "error",
				Component: "runtime",
				Action:    "dispatch",
			}},
			incidents: []incidentRecord{{
				Code:  "MCP_TIMEOUT",
				Count: 1,
			}},
		},
		Instances: stubInstances{
			rows: []runtimepipeline.WorkflowInstance{
				{InstanceID: "wf-1", SubjectID: "subj-1", WorkflowName: "order", CurrentState: "active", UpdatedAt: now},
				{InstanceID: "wf-2", SubjectID: "subj-1", WorkflowName: "order", CurrentState: "done", UpdatedAt: now.Add(-time.Minute)},
			},
			byID: map[string]runtimepipeline.WorkflowInstance{
				"wf-1": {InstanceID: "wf-1", SubjectID: "subj-1", WorkflowName: "order", CurrentState: "active", UpdatedAt: now},
			},
		},
		Runtime: &stubRuntimeControl{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/conversations", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conversations status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/conversations/sess-1", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conversation detail status=%d body=%s", rec.Code, rec.Body.String())
	}

	var detail ConversationDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("unmarshal conversation detail: %v", err)
	}
	if detail.AgentID != "agent-1" || len(detail.Messages) != 1 || len(detail.Turns) != 1 {
		t.Fatalf("unexpected conversation detail: %+v", detail)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/events", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("events status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/events/evt-1", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("event detail status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/runtime/logs", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("runtime logs status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/runtime/incidents", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("runtime incidents status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/agents/agent-1", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("agent detail status=%d body=%s", rec.Code, rec.Body.String())
	}
	var agent genericAgent
	if err := json.Unmarshal(rec.Body.Bytes(), &agent); err != nil {
		t.Fatalf("unmarshal agent detail: %v", err)
	}
	if agent.ID != "agent-1" || agent.Role != "worker" || agent.EntityID != "entity-1" {
		t.Fatalf("unexpected agent detail: %+v", agent)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/instances/aggregate?group_by=current_state", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("instance aggregate status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/instances/aggregate?group_by=subject_id&subject_id=subj-1", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("instance aggregate by subject status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/subjects/subj-1/status", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("subject status=%d body=%s", rec.Code, rec.Body.String())
	}
	var subject subjectStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &subject); err != nil {
		t.Fatalf("unmarshal subject status: %v", err)
	}
	if subject.SubjectID != "subj-1" || subject.LatestState != "active" || subject.EntityCount != 2 {
		t.Fatalf("unexpected subject status: %+v", subject)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/health", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/agent-1/actions/restart", strings.NewReader(`{}`))
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("agent restart status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/agent-1/actions/directive", strings.NewReader(`{"message":"hello","kill_previous":true}`))
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("agent directive status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !agentCtl.lastKillPrevious {
		t.Fatal("expected kill_previous to be forwarded to agent control")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/runtime/actions", strings.NewReader(`{"action":"pause"}`))
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("runtime action status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandler_DashboardRoutesRequireAuthentication(t *testing.T) {
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		AuthToken: testOperatorAuthToken,
		Agents: stubAgents{rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{ID: "agent-1"},
		}}},
		Runtime: &stubRuntimeControl{},
	})

	for _, tc := range []struct {
		name       string
		method     string
		path       string
		body       string
		authHeader string
		wantStatus int
		wantError  string
	}{
		{
			name:       "dashboard get missing bearer",
			method:     http.MethodGet,
			path:       "/api/agents",
			wantStatus: http.StatusUnauthorized,
			wantError:  errDashboardAuthMissingBearer.Error(),
		},
		{
			name:       "dashboard get invalid bearer",
			method:     http.MethodGet,
			path:       "/api/runtime/logs",
			authHeader: "Bearer wrong-token",
			wantStatus: http.StatusUnauthorized,
			wantError:  errDashboardAuthInvalidToken.Error(),
		},
		{
			name:       "runtime control missing bearer",
			method:     http.MethodPost,
			path:       "/api/runtime/actions",
			body:       `{"action":"pause"}`,
			wantStatus: http.StatusUnauthorized,
			wantError:  errDashboardAuthMissingBearer.Error(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("WWW-Authenticate"); got != `Bearer realm="swarm-operator"` {
				t.Fatalf("WWW-Authenticate=%q", got)
			}
			var payload map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatalf("unmarshal denial payload: %v", err)
			}
			if payload["error"] != tc.wantError {
				t.Fatalf("error=%#v, want %q", payload["error"], tc.wantError)
			}
		})
	}
}

func TestHandler_DashboardRoutesFailClosedWhenAuthIsNotConfigured(t *testing.T) {
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal denial payload: %v", err)
	}
	if payload["error"] != errDashboardAuthNotConfigured.Error() {
		t.Fatalf("error=%#v, want %q", payload["error"], errDashboardAuthNotConfigured.Error())
	}
}

func TestSQLConversationReader_ListAndGet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions:   store.SchemaFlavorCanonical,
			Turns:      store.SchemaFlavorCanonical,
			TurnBlocks: false,
		},
	}})
	now := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	summaryState := `{"summary":"brief","last_turn":{"parse_ok":true},"provider_session_id":"sess-1","retry_reason":"session not found","retries_from_session_id":"sess-0"}`
	messagePayload := `[{"role":"assistant","content":"hello"}]`
	toolCallsPayload := `[{"name":"schedule","arguments":{"delay_seconds":1209600}}]`
	responsePayload := `{"result":"14-day review scheduled.","raw":"{\"type\":\"user\",\"message\":{\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu_1\",\"content\":[{\"type\":\"text\",\"text\":\"{\\\"status\\\":\\\"scheduled\\\"}\"}]}]}}\n{\"type\":\"result\",\"result\":\"14-day review scheduled.\"}"}`

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*FROM \\(").
		WithArgs(25).
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "updated_at",
		}).AddRow("sess-1", "agent-1", "live_session", "global", "global", "session", "active", 3, []byte(summaryState), now))

	items, err := reader.List(context.Background(), 25)
	if err != nil {
		t.Fatalf("list conversations: %v", err)
	}
	if len(items) != 1 || items[0].AgentID != "agent-1" || items[0].Summary != "brief" {
		t.Fatalf("unexpected summaries: %+v", items)
	}
	if items[0].SessionID != "sess-1" || items[0].Kind != "live_session" {
		t.Fatalf("unexpected summary identity: %+v", items[0])
	}
	if items[0].Metadata["provider_session_id"] != "sess-1" {
		t.Fatalf("expected metadata to retain provider_session_id: %+v", items[0].Metadata)
	}
	if items[0].Metadata["retry_reason"] != "session not found" || items[0].Metadata["retries_from_session_id"] != "sess-0" {
		t.Fatalf("expected retry lineage metadata, got %+v", items[0].Metadata)
	}

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*COALESCE\\(conversation, '\\[\\]'::jsonb\\).*FROM \\(").
		WithArgs("sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "conversation", "updated_at",
		}).AddRow("sess-1", "agent-1", "live_session", "global", "global", "session", "active", 3, []byte(summaryState), []byte(messagePayload), now))

	mock.ExpectQuery("SELECT\\s+turn_id::text,.*FROM agent_turns").
		WithArgs("agent-1", "sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"turn_id", "agent_id", "session_id", "runtime_mode", "scope_key", "entity_id", "trigger_event_id", "trigger_event_type", "task_id",
			"available_tools", "tool_calls", "emitted_events", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible",
			"request_payload", "response_payload", "turn_blocks", "parse_ok", "latency_ms", "retry_count", "error", "created_at",
		}).AddRow(
			"turn-1", "agent-1", "sess-1", "task", "global", "entity-1", "evt-1", "scoring/vertical.marginal", "",
			[]byte(`["schedule"]`), []byte(toolCallsPayload), []byte(`["vertical.marginal_review_due"]`), []byte(`{"runtime-tools":"ok"}`), []byte(`["mcp__runtime-tools__schedule"]`), []byte(`["mcp__runtime-tools__schedule"]`),
			[]byte(`{"message":{"content":"dispatch"}}`), []byte(responsePayload), []byte(`[]`), true, 92282, 0, "", now,
		))

	item, ok, err := reader.Get(context.Background(), "sess-1")
	if err != nil {
		t.Fatalf("get conversation: %v", err)
	}
	if !ok {
		t.Fatalf("expected conversation to exist")
	}
	if item.AgentID != "agent-1" || item.SessionID != "sess-1" || len(item.Messages) != 1 || len(item.Turns) != 1 {
		t.Fatalf("unexpected detail: %+v", item)
	}
	if item.Kind != "live_session" {
		t.Fatalf("expected live_session detail kind, got %+v", item)
	}
	lastTurn, ok := item.RuntimeState["last_turn"].(map[string]any)
	if !ok || lastTurn["parse_ok"] != true {
		t.Fatalf("expected runtime_state.last_turn parse_ok=true, got %+v", item.RuntimeState)
	}
	if item.RuntimeState["retry_reason"] != "session not found" || item.RuntimeState["retries_from_session_id"] != "sess-0" {
		t.Fatalf("expected retry lineage in runtime_state, got %+v", item.RuntimeState)
	}
	if item.Turns[0].AssistantVisibleOutput != "" || item.Turns[0].Outcome != "" || len(item.Turns[0].ToolResults) != 0 {
		t.Fatalf("expected missing canonical summary to fail closed, got %+v", item.Turns[0])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_UsesCanonicalTurnSummary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ID:   "agent-1",
				Role: "researcher",
				Mode: "global",
				Type: "managed",
			},
			Status: "active",
		}},
		caps: store.StoreSchemaCapabilities{
			Conversations: store.ConversationSchemaCapabilities{
				Sessions:   store.SchemaFlavorCanonical,
				Turns:      store.SchemaFlavorCanonical,
				TurnBlocks: true,
			},
		},
	}, 12)

	turnBlocksPayload := `[
		{"kind":"tool_result","tool_name":"schedule","output":{"status":"scheduled"},"data":{"tool_use_id":"toolu_1"}},
		{"kind":"turn_summary","data":{"tool_results":[{"tool_name":"schedule","tool_use_id":"toolu_1","output":{"status":"scheduled"}}]}}
	]`
	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows([]string{
			"agent_id", "status", "turn_count", "lease_holder", "lease_expires_at", "pending_count", "oldest_pending_age_sec",
			"failures_24h", "dead_letters_24h", "turns_24h", "task_id", "parse_ok", "turn_blocks",
		}).AddRow("agent-1", "active", 3, "", nil, 0, 0, 0, 0, 0, "task-1", true, []byte(turnBlocksPayload)))

	items, err := reader.ListGenericAgents(context.Background())
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one agent, got %+v", items)
	}
	if items[0].CurrentTaskID != "task-1" {
		t.Fatalf("current_task_id = %q", items[0].CurrentTaskID)
	}
	if items[0].LastTool["name"] != "schedule" {
		t.Fatalf("last_tool = %#v", items[0].LastTool)
	}
	if items[0].LastTool["ok"] != true {
		t.Fatalf("last_tool.ok = %#v", items[0].LastTool["ok"])
	}
	result, _ := items[0].LastTool["result"].(map[string]any)
	if result["status"] != "scheduled" {
		t.Fatalf("last_tool.result = %#v", items[0].LastTool["result"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_FailsClosedWithoutCanonicalTurnSummary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ID:   "agent-1",
				Role: "researcher",
				Mode: "global",
				Type: "managed",
			},
			Status: "active",
		}},
		caps: store.StoreSchemaCapabilities{
			Conversations: store.ConversationSchemaCapabilities{
				Sessions:   store.SchemaFlavorCanonical,
				Turns:      store.SchemaFlavorCanonical,
				TurnBlocks: true,
			},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows([]string{
			"agent_id", "status", "turn_count", "lease_holder", "lease_expires_at", "pending_count", "oldest_pending_age_sec",
			"failures_24h", "dead_letters_24h", "turns_24h", "task_id", "parse_ok", "turn_blocks",
		}).AddRow("agent-1", "active", 3, "", nil, 0, 0, 0, 0, 0, "task-1", true, []byte(`[{"kind":"assistant_text","text":"stale fallback text"}]`)))

	items, err := reader.ListGenericAgents(context.Background())
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one agent, got %+v", items)
	}
	if items[0].CurrentTaskID != "task-1" {
		t.Fatalf("current_task_id = %q", items[0].CurrentTaskID)
	}
	if items[0].LastTool != nil {
		t.Fatalf("expected missing canonical summary to fail closed, got last_tool=%#v", items[0].LastTool)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_FailsOnMalformedCanonicalTurnSummary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ID:   "agent-1",
				Role: "researcher",
				Mode: "global",
				Type: "managed",
			},
			Status: "active",
		}},
		caps: store.StoreSchemaCapabilities{
			Conversations: store.ConversationSchemaCapabilities{
				Sessions:   store.SchemaFlavorCanonical,
				Turns:      store.SchemaFlavorCanonical,
				TurnBlocks: true,
			},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows([]string{
			"agent_id", "status", "turn_count", "lease_holder", "lease_expires_at", "pending_count", "oldest_pending_age_sec",
			"failures_24h", "dead_letters_24h", "turns_24h", "task_id", "parse_ok", "turn_blocks",
		}).AddRow("agent-1", "active", 3, "", nil, 0, 0, 0, 0, 0, "task-1", true, []byte(`[{"kind":"turn_summary","data":{"tool_results":"bad"}}]`)))

	if _, err := reader.ListGenericAgents(context.Background()); err == nil {
		t.Fatal("expected malformed canonical turn summary to fail")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_UsesOperatorProjectionAsCanonicalOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ID:   "agent-1",
				Role: "researcher",
				Mode: "global",
				Type: "managed",
			},
			Status: "terminated",
		}},
		caps: store.StoreSchemaCapabilities{
			Conversations: store.ConversationSchemaCapabilities{
				Sessions:   store.SchemaFlavorCanonical,
				Turns:      store.SchemaFlavorCanonical,
				TurnBlocks: true,
			},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows([]string{
			"agent_id", "status", "turn_count", "lease_holder", "lease_expires_at", "pending_count", "oldest_pending_age_sec",
			"failures_24h", "dead_letters_24h", "turns_24h", "task_id", "parse_ok", "turn_blocks",
		}).AddRow("agent-1", "active", 7, "lease-owner", time.Now().Add(time.Minute), 2, 45, 0, 0, 0, "task-7", false, []byte(`[]`)))

	items, err := reader.ListGenericAgents(context.Background())
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one agent, got %+v", items)
	}
	if items[0].Status != "active" {
		t.Fatalf("status = %q, want active from operator projection", items[0].Status)
	}
	if items[0].State != "running" {
		t.Fatalf("state = %q, want running from operator projection", items[0].State)
	}
	if items[0].PendingEvents != 2 {
		t.Fatalf("pending_events = %d, want 2", items[0].PendingEvents)
	}
	if items[0].CurrentTaskID != "task-7" {
		t.Fatalf("current_task_id = %q, want task-7", items[0].CurrentTaskID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_FailsClosedWithoutOperatorProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ID:   "agent-1",
				Role: "researcher",
				Mode: "global",
				Type: "managed",
			},
			Status: "active",
		}},
		caps: store.StoreSchemaCapabilities{
			Conversations: store.ConversationSchemaCapabilities{
				Sessions:   store.SchemaFlavorCanonical,
				Turns:      store.SchemaFlavorCanonical,
				TurnBlocks: true,
			},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows([]string{
			"agent_id", "status", "turn_count", "lease_holder", "lease_expires_at", "pending_count", "oldest_pending_age_sec",
			"failures_24h", "dead_letters_24h", "turns_24h", "task_id", "parse_ok", "turn_blocks",
		}))

	if _, err := reader.ListGenericAgents(context.Background()); err == nil || !strings.Contains(err.Error(), "missing agent operator projection") {
		t.Fatalf("expected missing agent operator projection error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_AlignsBacklogWithCanonicalPendingSelector(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:     "agent-1",
			Role:   "researcher",
			Mode:   "global",
			Type:   "managed",
			Config: json.RawMessage(`{"system_prompt":"You are an operator agent."}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	pendingEventID := uuid.NewString()
	failedEventID := uuid.NewString()
	inProgressNoReceiptEventID := uuid.NewString()
	deadEventID := uuid.NewString()
	for _, eventID := range []string{pendingEventID, failedEventID, inProgressNoReceiptEventID, deadEventID} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (
				event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
			) VALUES (
				$1::uuid, $2::uuid, 'task.completed', 'global', '{}'::jsonb, 'runtime', 'agent', now() - interval '5 minutes'
			)
		`, eventID, runID); err != nil {
			t.Fatalf("seed event %s: %v", eventID, err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, last_error, delivered_at, created_at
		) VALUES
			($1::uuid, $2::uuid, 'agent', 'agent-1', 'pending', 0, '', NULL, now() - interval '7 minutes'),
			($1::uuid, $3::uuid, 'agent', 'agent-1', 'failed', 1, 'retryable-failure', now() - interval '2 minutes', now() - interval '5 minutes'),
			($1::uuid, $4::uuid, 'agent', 'agent-1', 'in_progress', 0, '', NULL, now() - interval '6 minutes'),
			($1::uuid, $5::uuid, 'agent', 'agent-1', 'dead_letter', 2, 'terminal-dead-letter', now() - interval '1 minute', now() - interval '8 minutes')
	`, runID, pendingEventID, failedEventID, inProgressNoReceiptEventID, deadEventID); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, side_effects, processed_at
		) VALUES
			($1::uuid, 'agent', 'agent-1', 'dead_letter', '{"manager_status":"error","retry_count":1,"error":"retryable-failure"}'::jsonb, now() - interval '2 minutes'),
			($2::uuid, 'agent', 'agent-1', 'success', '{"manager_status":"dead_letter","retry_count":2,"error":"terminal-dead-letter"}'::jsonb, now())
	`, failedEventID, deadEventID); err != nil {
		t.Fatalf("seed conflicting receipts: %v", err)
	}

	pending, err := pg.ListPendingEventsForAgent(ctx, "agent-1", time.Now().Add(-time.Hour), 20)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	gotPendingIDs := make([]string, 0, len(pending))
	for _, evt := range pending {
		gotPendingIDs = append(gotPendingIDs, evt.ID)
	}
	slices.Sort(gotPendingIDs)
	wantPendingIDs := []string{failedEventID, inProgressNoReceiptEventID, pendingEventID}
	slices.Sort(wantPendingIDs)
	if !slices.Equal(gotPendingIDs, wantPendingIDs) {
		t.Fatalf("pending event ids = %#v, want %#v", gotPendingIDs, wantPendingIDs)
	}

	reader := NewSQLAgentReader(db, pg, 12)
	items, err := reader.ListGenericAgents(ctx)
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one agent, got %+v", items)
	}
	if items[0].PendingEvents != len(pending) {
		t.Fatalf("pending_events = %d, want %d canonical pending deliveries", items[0].PendingEvents, len(pending))
	}
	if items[0].Failures24h != 1 {
		t.Fatalf("failures_24h = %d, want 1 failed delivery", items[0].Failures24h)
	}
	if items[0].DeadLetters24h != 1 {
		t.Fatalf("dead_letters_24h = %d, want 1 dead-letter delivery", items[0].DeadLetters24h)
	}
	if items[0].State != "stuck" {
		t.Fatalf("state = %q, want stuck", items[0].State)
	}
}

func TestSQLConversationReader_GetPrefersCanonicalTurnBlocks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions:   store.SchemaFlavorCanonical,
			Turns:      store.SchemaFlavorCanonical,
			TurnBlocks: true,
		},
	}})
	now := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	summaryState := `{"summary":"brief"}`
	messagePayload := `[{"role":"assistant","content":"hello"}]`
	turnBlocksPayload := `[
		{"kind":"dispatch","title":"scoring/vertical.marginal","data":{"trigger_event_id":"evt-1"}},
		{"kind":"tool_use","tool_name":"schedule","input":{"delay_seconds":1209600},"data":{"tool_use_id":"toolu_1"}},
		{"kind":"tool_result","tool_name":"schedule","output":{"status":"scheduled"},"data":{"tool_use_id":"toolu_1"}},
		{"kind":"assistant_text","text":"Parking for manual review."},
		{"kind":"outcome","text":"14-day review scheduled."},
		{"kind":"turn_summary","data":{"assistant_visible_output":"Parking for manual review.","outcome":"14-day review scheduled.","tool_results":[{"tool_name":"schedule","tool_use_id":"toolu_1","output":{"status":"scheduled"}}]}}
	]`

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*COALESCE\\(conversation, '\\[\\]'::jsonb\\).*FROM \\(").
		WithArgs("sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "conversation", "updated_at",
		}).AddRow("sess-1", "agent-1", "live_session", "global", "global", "session", "active", 3, []byte(summaryState), []byte(messagePayload), now))

	mock.ExpectQuery("SELECT\\s+turn_id::text,.*FROM agent_turns").
		WithArgs("agent-1", "sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"turn_id", "agent_id", "session_id", "runtime_mode", "scope_key", "entity_id", "trigger_event_id", "trigger_event_type", "task_id",
			"available_tools", "tool_calls", "emitted_events", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible",
			"request_payload", "response_payload", "turn_blocks", "parse_ok", "latency_ms", "retry_count", "error", "created_at",
		}).AddRow(
			"turn-1", "agent-1", "sess-1", "task", "global", "entity-1", "evt-1", "scoring/vertical.marginal", "",
			[]byte(`["schedule"]`), []byte(`[]`), []byte(`[]`), []byte(`{"runtime-tools":"ok"}`), []byte(`["mcp__runtime-tools__schedule"]`), []byte(`["mcp__runtime-tools__schedule"]`),
			[]byte(`{"message":{"content":"dispatch"}}`), []byte(`{"result":"stale fallback text"}`), []byte(turnBlocksPayload), true, 92282, 0, "", now,
		))

	item, ok, err := reader.Get(context.Background(), "sess-1")
	if err != nil {
		t.Fatalf("get conversation: %v", err)
	}
	if !ok || len(item.Turns) != 1 {
		t.Fatalf("unexpected detail: %+v", item)
	}
	if item.Turns[0].AssistantVisibleOutput != "Parking for manual review." {
		t.Fatalf("assistant_visible_output = %q", item.Turns[0].AssistantVisibleOutput)
	}
	if item.Turns[0].Outcome != "14-day review scheduled." {
		t.Fatalf("outcome = %q", item.Turns[0].Outcome)
	}
	if len(item.Turns[0].ToolResults) != 1 {
		t.Fatalf("tool_results = %#v", item.Turns[0].ToolResults)
	}
	result, _ := item.Turns[0].ToolResults[0].(map[string]any)
	if result["tool_name"] != "schedule" {
		t.Fatalf("tool_result = %#v", result)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLConversationReader_GetDoesNotInferOutcomeWithoutCanonicalField(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions:   store.SchemaFlavorCanonical,
			Turns:      store.SchemaFlavorCanonical,
			TurnBlocks: true,
		},
	}})
	now := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	summaryState := `{"summary":"brief"}`
	messagePayload := `[{"role":"assistant","content":"hello"}]`
	turnBlocksPayload := `[
		{"kind":"assistant_text","text":"stale assistant text"},
		{"kind":"outcome","text":"stale raw outcome"},
		{"kind":"turn_summary","data":{"assistant_visible_output":"Parking for manual review."}}
	]`

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*COALESCE\\(conversation, '\\[\\]'::jsonb\\).*FROM \\(").
		WithArgs("sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "conversation", "updated_at",
		}).AddRow("sess-1", "agent-1", "live_session", "global", "global", "session", "active", 3, []byte(summaryState), []byte(messagePayload), now))

	mock.ExpectQuery("SELECT\\s+turn_id::text,.*FROM agent_turns").
		WithArgs("agent-1", "sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"turn_id", "agent_id", "session_id", "runtime_mode", "scope_key", "entity_id", "trigger_event_id", "trigger_event_type", "task_id",
			"available_tools", "tool_calls", "emitted_events", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible",
			"request_payload", "response_payload", "turn_blocks", "parse_ok", "latency_ms", "retry_count", "error", "created_at",
		}).AddRow(
			"turn-1", "agent-1", "sess-1", "task", "global", "entity-1", "evt-1", "scoring/vertical.marginal", "",
			[]byte(`[]`), []byte(`[]`), []byte(`[]`), []byte(`{}`), []byte(`[]`), []byte(`[]`),
			[]byte(`{"message":{"content":"dispatch"}}`), []byte(`{"result":"14-day review scheduled."}`), []byte(turnBlocksPayload), true, 92282, 0, "", now,
		))

	item, ok, err := reader.Get(context.Background(), "sess-1")
	if err != nil {
		t.Fatalf("get conversation: %v", err)
	}
	if !ok || len(item.Turns) != 1 {
		t.Fatalf("unexpected detail: %+v", item)
	}
	if item.Turns[0].AssistantVisibleOutput != "Parking for manual review." {
		t.Fatalf("assistant_visible_output = %q", item.Turns[0].AssistantVisibleOutput)
	}
	if item.Turns[0].Outcome != "" {
		t.Fatalf("expected missing canonical outcome to fail closed, got %q", item.Turns[0].Outcome)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLConversationReader_ListAndGet_TaskAudit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Audits:     store.SchemaFlavorCanonical,
			Turns:      store.SchemaFlavorCanonical,
			TurnBlocks: true,
		},
	}})
	now := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	summaryState := `{"summary":"one-shot","last_turn":{"parse_ok":true}}`
	messagePayload := `[{"role":"assistant","content":"done"}]`

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*FROM \\(").
		WithArgs(10).
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "updated_at",
		}).AddRow("audit-1", "agent-1", "turn_audit", "", "global", "task", "active", 1, []byte(summaryState), now))

	items, err := reader.List(context.Background(), 10)
	if err != nil {
		t.Fatalf("list conversations: %v", err)
	}
	if len(items) != 1 || items[0].Kind != "turn_audit" || items[0].RuntimeMode != "task" || items[0].SessionID != "audit-1" {
		t.Fatalf("unexpected audit summaries: %+v", items)
	}

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*COALESCE\\(conversation, '\\[\\]'::jsonb\\).*FROM \\(").
		WithArgs("audit-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "conversation", "updated_at",
		}).AddRow("audit-1", "agent-1", "turn_audit", "", "global", "task", "active", 1, []byte(summaryState), []byte(messagePayload), now))

	mock.ExpectQuery("SELECT\\s+turn_id::text,.*FROM agent_turns").
		WithArgs("agent-1", "audit-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"turn_id", "agent_id", "session_id", "runtime_mode", "scope_key", "entity_id", "trigger_event_id", "trigger_event_type", "task_id",
			"available_tools", "tool_calls", "emitted_events", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible",
			"request_payload", "response_payload", "turn_blocks", "parse_ok", "latency_ms", "retry_count", "error", "created_at",
		}).AddRow(
			"turn-1", "agent-1", "audit-1", "task", "", "", "evt-1", "task.run", "task-1",
			[]byte(`[]`), []byte(`[]`), []byte(`[]`), []byte(`{}`), []byte(`[]`), []byte(`[]`),
			[]byte(`{"message":{"content":"dispatch"}}`), []byte(`{"result":"done"}`), []byte(`[{"kind":"turn_summary","data":{"assistant_visible_output":"done","outcome":"done"}}]`), true, 25, 0, "", now,
		))

	item, ok, err := reader.Get(context.Background(), "audit-1")
	if err != nil {
		t.Fatalf("get conversation: %v", err)
	}
	if !ok || item.Kind != "turn_audit" || item.RuntimeMode != "task" || item.SessionID != "audit-1" {
		t.Fatalf("unexpected task audit detail: %+v", item)
	}
	if len(item.Messages) != 1 || len(item.Turns) != 1 || item.Turns[0].Outcome != "done" {
		t.Fatalf("unexpected task audit payload: %+v", item)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLConversationReader_GetUsesCanonicalTurnSummaryProgress(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions:   store.SchemaFlavorCanonical,
			Turns:      store.SchemaFlavorCanonical,
			TurnBlocks: true,
		},
	}})
	now := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	summaryState := `{"summary":"brief"}`
	messagePayload := `[{"role":"assistant","content":"hello"}]`
	turnBlocksPayload := `[
		{"kind":"turn_summary","data":{"assistant_visible_output":"Parking for manual review.","outcome":"14-day review scheduled.","progress_updates":["Scheduling the follow-up review."],"reasoning_blocks":["Need a manual checkpoint."]}}
	]`

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*COALESCE\\(conversation, '\\[\\]'::jsonb\\).*FROM \\(").
		WithArgs("sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "conversation", "updated_at",
		}).AddRow("sess-1", "agent-1", "live_session", "global", "global", "session", "active", 3, []byte(summaryState), []byte(messagePayload), now))

	mock.ExpectQuery("SELECT\\s+turn_id::text,.*FROM agent_turns").
		WithArgs("agent-1", "sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"turn_id", "agent_id", "session_id", "runtime_mode", "scope_key", "entity_id", "trigger_event_id", "trigger_event_type", "task_id",
			"available_tools", "tool_calls", "emitted_events", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible",
			"request_payload", "response_payload", "turn_blocks", "parse_ok", "latency_ms", "retry_count", "error", "created_at",
		}).AddRow(
			"turn-1", "agent-1", "sess-1", "task", "global", "entity-1", "evt-1", "scoring/vertical.marginal", "",
			[]byte(`[]`), []byte(`[]`), []byte(`[]`), []byte(`{}`), []byte(`[]`), []byte(`[]`),
			[]byte(`{"message":{"content":"dispatch"}}`), []byte(`{"result":"stale fallback text"}`), []byte(turnBlocksPayload), true, 92282, 0, "", now,
		))

	item, ok, err := reader.Get(context.Background(), "sess-1")
	if err != nil {
		t.Fatalf("get conversation: %v", err)
	}
	if !ok || len(item.Turns) != 1 {
		t.Fatalf("unexpected detail: %+v", item)
	}
	if len(item.Turns[0].ProgressUpdates) != 1 || item.Turns[0].ProgressUpdates[0] != "Scheduling the follow-up review." {
		t.Fatalf("progress_updates = %#v", item.Turns[0].ProgressUpdates)
	}
	if len(item.Turns[0].ReasoningBlocks) != 1 || item.Turns[0].ReasoningBlocks[0] != "Need a manual checkpoint." {
		t.Fatalf("reasoning_blocks = %#v", item.Turns[0].ReasoningBlocks)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSummarizeConversationTurnBlocks_FailsClosedOnEmptyCanonicalSummary(t *testing.T) {
	blocks := []any{
		map[string]any{"kind": "assistant_text", "text": "stale fallback text"},
		map[string]any{"kind": "outcome", "text": "stale outcome"},
		map[string]any{"kind": "turn_summary", "data": map[string]any{}},
	}

	assistantText, outcome, reasoning, progress, toolResults := summarizeConversationTurnBlocks(blocks)
	if assistantText != "" || outcome != "" {
		t.Fatalf("expected empty canonical summary strings, got assistant=%q outcome=%q", assistantText, outcome)
	}
	if reasoning != nil {
		t.Fatalf("expected nil reasoning blocks, got %#v", reasoning)
	}
	if progress != nil {
		t.Fatalf("expected nil progress updates, got %#v", progress)
	}
	if toolResults != nil {
		t.Fatalf("expected nil tool results, got %#v", toolResults)
	}
}

func TestSummarizeConversationTurnBlocks_DoesNotInferOutcomeWithoutCanonicalField(t *testing.T) {
	blocks := []any{
		map[string]any{"kind": "assistant_text", "text": "stale fallback text"},
		map[string]any{"kind": "outcome", "text": "stale outcome"},
		map[string]any{"kind": "turn_summary", "data": map[string]any{
			"assistant_visible_output": "Parking for manual review.",
		}},
	}

	assistantText, outcome, reasoning, progress, toolResults := summarizeConversationTurnBlocks(blocks)
	if assistantText != "Parking for manual review." {
		t.Fatalf("assistant_visible_output = %q", assistantText)
	}
	if outcome != "" {
		t.Fatalf("expected missing canonical outcome to stay empty, got %q", outcome)
	}
	if reasoning != nil {
		t.Fatalf("expected nil reasoning blocks, got %#v", reasoning)
	}
	if progress != nil {
		t.Fatalf("expected nil progress updates, got %#v", progress)
	}
	if toolResults != nil {
		t.Fatalf("expected nil tool results, got %#v", toolResults)
	}
}

func TestSQLConversationReader_ListFailsOnMalformedCanonicalRuntimeState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions: store.SchemaFlavorCanonical,
		},
	}})

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*FROM \\(").
		WithArgs(10).
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "updated_at",
		}).AddRow("sess-1", "agent-1", "live_session", "global", "global", "session", "active", 3, []byte(`{"summary":123}`), time.Now().UTC()))

	if _, err := reader.List(context.Background(), 10); err == nil {
		t.Fatal("expected malformed canonical runtime_state to fail")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLConversationReader_GetFailsOnMalformedCanonicalTurnSummary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions:   store.SchemaFlavorCanonical,
			Turns:      store.SchemaFlavorCanonical,
			TurnBlocks: true,
		},
	}})
	now := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	summaryState := `{"summary":"brief"}`
	messagePayload := `[{"role":"assistant","content":"hello"}]`

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*COALESCE\\(conversation, '\\[\\]'::jsonb\\).*FROM \\(").
		WithArgs("sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "conversation", "updated_at",
		}).AddRow("sess-1", "agent-1", "live_session", "global", "global", "session", "active", 3, []byte(summaryState), []byte(messagePayload), now))

	mock.ExpectQuery("SELECT\\s+turn_id::text,.*FROM agent_turns").
		WithArgs("agent-1", "sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"turn_id", "agent_id", "session_id", "runtime_mode", "scope_key", "entity_id", "trigger_event_id", "trigger_event_type", "task_id",
			"available_tools", "tool_calls", "emitted_events", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible",
			"request_payload", "response_payload", "turn_blocks", "parse_ok", "latency_ms", "retry_count", "error", "created_at",
		}).AddRow(
			"turn-1", "agent-1", "sess-1", "task", "global", "entity-1", "evt-1", "scoring/vertical.marginal", "",
			[]byte(`[]`), []byte(`[]`), []byte(`[]`), []byte(`{}`), []byte(`[]`), []byte(`[]`),
			[]byte(`{"message":{"content":"dispatch"}}`), []byte(`{"result":"stale fallback text"}`), []byte(`[{"kind":"turn_summary","data":{"tool_results":"bad"}}]`), true, 92282, 0, "", now,
		))

	if _, _, err := reader.Get(context.Background(), "sess-1"); err == nil {
		t.Fatal("expected malformed canonical turn summary to fail")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLConversationReader_GetUsesSessionIDNotAgentID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Audits: store.SchemaFlavorCanonical,
			Turns:  store.SchemaFlavorCanonical,
		},
	}})
	now := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	summaryState := `{"summary":"older audit"}`
	messagePayload := `[{"role":"assistant","content":"older"}]`

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*COALESCE\\(conversation, '\\[\\]'::jsonb\\).*FROM \\(").
		WithArgs("audit-older").
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "conversation", "updated_at",
		}).AddRow("audit-older", "agent-1", "turn_audit", "", "global", "task", "active", 1, []byte(summaryState), []byte(messagePayload), now))

	mock.ExpectQuery("SELECT\\s+turn_id::text,.*FROM agent_turns").
		WithArgs("agent-1", "audit-older").
		WillReturnRows(sqlmock.NewRows([]string{
			"turn_id", "agent_id", "session_id", "runtime_mode", "scope_key", "entity_id", "trigger_event_id", "trigger_event_type", "task_id",
			"available_tools", "tool_calls", "emitted_events", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible",
			"request_payload", "response_payload", "turn_blocks", "parse_ok", "latency_ms", "retry_count", "error", "created_at",
		}).AddRow(
			"turn-1", "agent-1", "audit-older", "task", "", "", "evt-1", "task.run", "task-1",
			[]byte(`[]`), []byte(`[]`), []byte(`[]`), []byte(`{}`), []byte(`[]`), []byte(`[]`),
			[]byte(`{"message":{"content":"dispatch"}}`), []byte(`{"result":"older"}`), []byte(`[]`), true, 25, 0, "", now,
		))

	item, ok, err := reader.Get(context.Background(), "audit-older")
	if err != nil {
		t.Fatalf("get conversation: %v", err)
	}
	if !ok || item.SessionID != "audit-older" || item.Summary != "older audit" {
		t.Fatalf("unexpected detail: %+v", item)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLConversationReader_GetMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions: store.SchemaFlavorCanonical,
			Audits:   store.SchemaFlavorCanonical,
		},
	}})
	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*FROM \\(").
		WithArgs("missing-session").
		WillReturnError(sql.ErrNoRows)

	_, ok, err := reader.Get(context.Background(), "missing-session")
	if err != nil {
		t.Fatalf("expected nil error for missing conversation, got %v", err)
	}
	if ok {
		t.Fatalf("expected missing conversation")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLConversationReader_GetSkipsTurnsWithoutCapability(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions: store.SchemaFlavorCanonical,
		},
	}})
	now := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	summaryState := `{"summary":"brief"}`
	messagePayload := `[{"role":"assistant","content":"hello"}]`

	mock.ExpectQuery("SELECT\\s+session_id,\\s+agent_id,\\s+kind,.*COALESCE\\(conversation, '\\[\\]'::jsonb\\).*FROM \\(").
		WithArgs("sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "conversation", "updated_at",
		}).AddRow("sess-1", "agent-1", "live_session", "global", "global", "session", "active", 1, []byte(summaryState), []byte(messagePayload), now))

	item, ok, err := reader.Get(context.Background(), "sess-1")
	if err != nil {
		t.Fatalf("get conversation: %v", err)
	}
	if !ok {
		t.Fatalf("expected conversation to exist")
	}
	if len(item.Turns) != 0 {
		t.Fatalf("turns = %#v", item.Turns)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestHandler_BuilderRPC(t *testing.T) {
	projectCtl := &stubProjectControl{}
	instances := stubInstances{
		rows: []runtimepipeline.WorkflowInstance{
			{InstanceID: "wf-1", WorkflowName: "order", CurrentState: "active"},
		},
		byID: map[string]runtimepipeline.WorkflowInstance{
			"wf-1": {
				InstanceID:   "wf-1",
				WorkflowName: "order",
				CurrentState: "active",
				Metadata: map[string]any{
					"score": 3.7,
					"gates": map[string]any{"review_gate": true},
					"slug":  "order-1",
				},
				StateBuckets: map[string]any{
					"accumulator": map[string]any{"count": 2},
				},
			},
		},
	}
	health := func(context.Context) (map[string]any, error) {
		return map[string]any{"runtime": map[string]any{"ready": true}}, nil
	}
	handler := NewHandler(Options{
		Health:    health,
		Instances: instances,
		Version:   "swarm-test",
		Builder:   newBuilderHandlerForTest(health, instances, "swarm-test", nil, nil, projectCtl),
	})

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("engine.ping status=%d body=%s", rec.Code, rec.Body.String())
	}
	var pingResp builderpkg.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &pingResp); err != nil {
		t.Fatalf("unmarshal ping response: %v", err)
	}
	result, ok := pingResp.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected ping result: %#v", pingResp.Result)
	}
	if result["status"] != "ok" || result["version"] != "swarm-test" {
		t.Fatalf("unexpected ping result: %#v", result)
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"2","method":"state.list_instances"}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("state.list_instances status=%d body=%s", rec.Code, rec.Body.String())
	}
	var instancesResp builderpkg.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &instancesResp); err != nil {
		t.Fatalf("unmarshal instances response: %v", err)
	}
	result, ok = instancesResp.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected instances result: %#v", instancesResp.Result)
	}
	instanceRows, ok := result["instances"].([]any)
	if !ok || len(instanceRows) != 1 {
		t.Fatalf("unexpected instances payload: %#v", result)
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"3","method":"state.get_instances"}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("state.get_instances status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"4","method":"state.get_entity","params":{"instance_id":"wf-1"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("state.get_entity status=%d body=%s", rec.Code, rec.Body.String())
	}
	var entityResp builderpkg.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &entityResp); err != nil {
		t.Fatalf("unmarshal entity response: %v", err)
	}
	result, ok = entityResp.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected entity result: %#v", entityResp.Result)
	}
	entity, ok := result["entity"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected entity payload: %#v", result)
	}
	if entity["state"] != "active" || entity["score"] != 3.7 {
		t.Fatalf("unexpected entity payload: %#v", entity)
	}
	gates, ok := result["gates"].(map[string]any)
	if !ok || gates["review_gate"] != true {
		t.Fatalf("unexpected gates payload: %#v", result["gates"])
	}
	accumulated, ok := result["accumulated"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected accumulated payload: %#v", result["accumulated"])
	}
	accBucket, ok := accumulated["accumulator"].(map[string]any)
	if !ok || accBucket["count"] != float64(2) {
		t.Fatalf("unexpected accumulated payload: %#v", accumulated)
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"5","method":"project.open","params":{"project_dir":"/tmp/builder-project"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("project.open status=%d body=%s", rec.Code, rec.Body.String())
	}
	var projectResp builderpkg.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &projectResp); err != nil {
		t.Fatalf("unmarshal project.open response: %v", err)
	}
	result, ok = projectResp.Result.(map[string]any)
	if !ok || result["project_dir"] != "/tmp/builder-project" || result["loaded"] != true {
		t.Fatalf("unexpected project.open payload: %#v", projectResp.Result)
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"6","method":"engine.ping"}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/rpc engine.ping status=%d body=%s", rec.Code, rec.Body.String())
	}
	var apiPingResp builderpkg.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &apiPingResp); err != nil {
		t.Fatalf("unmarshal /api/rpc response: %v", err)
	}
	result, ok = apiPingResp.Result.(map[string]any)
	if !ok || result["status"] != "ok" || result["version"] != "swarm-test" {
		t.Fatalf("unexpected /api/rpc result: %#v", apiPingResp.Result)
	}
}

func TestHandler_BuilderWSHealthHeartbeat(t *testing.T) {
	restore := builderpkg.SetHealthHeartbeatIntervalForTest(20 * time.Millisecond)
	defer restore()
	health := func(context.Context) (map[string]any, error) {
		return map[string]any{"runtime": map[string]any{"ready": true}}, nil
	}
	ts := httptest.NewServer(NewHandler(Options{
		Health:  health,
		Version: "swarm-test",
		Builder: newBuilderHandlerForTest(health, nil, "swarm-test", nil, nil, nil),
	}))
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "engine:health"}); err != nil {
		t.Fatalf("subscribe write: %v", err)
	}

	var frame builderpkg.WSEventFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read first event: %v", err)
	}
	if frame.Channel != "engine:health" {
		t.Fatalf("unexpected channel: %#v", frame.Channel)
	}
	data, ok := frame.Data.(map[string]any)
	if !ok {
		t.Fatalf("unexpected event payload: %#v", frame.Data)
	}
	if data["status"] != "ok" || data["version"] != "swarm-test" {
		t.Fatalf("unexpected health payload: %#v", data)
	}
}

func TestHandler_BuilderWSHealthHeartbeat_APIAlias(t *testing.T) {
	restore := builderpkg.SetHealthHeartbeatIntervalForTest(20 * time.Millisecond)
	defer restore()
	health := func(context.Context) (map[string]any, error) {
		return map[string]any{"runtime": map[string]any{"ready": true}}, nil
	}
	ts := httptest.NewServer(NewHandler(Options{
		Health:  health,
		Version: "swarm-test",
		Builder: newBuilderHandlerForTest(health, nil, "swarm-test", nil, nil, nil),
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial builder ws alias: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "engine:health",
	}); err != nil {
		t.Fatalf("subscribe health alias: %v", err)
	}

	var frame builderpkg.WSEventFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read health alias frame: %v", err)
	}
	if frame.Channel != "engine:health" {
		t.Fatalf("unexpected alias channel: %#v", frame.Channel)
	}
	payload, ok := frame.Data.(map[string]any)
	if !ok {
		t.Fatalf("unexpected alias payload: %#v", frame.Data)
	}
	if payload["version"] != "swarm-test" {
		t.Fatalf("unexpected alias payload: %#v", payload)
	}
}

func TestHandler_HealthzAliases(t *testing.T) {
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		AuthToken: testOperatorAuthToken,
		Version:   "swarm-test",
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal /healthz: %v", err)
	}
	if payload["ok"] != true {
		t.Fatalf("unexpected /healthz payload: %#v", payload)
	}

	for _, path := range []string{"/api/healthz", "/api/health"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		setOperatorAuth(req)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, rec.Code, rec.Body.String())
		}
		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("unmarshal %s: %v", path, err)
		}
		if payload["ok"] != true {
			t.Fatalf("unexpected %s payload: %#v", path, payload)
		}
	}
}

func TestHandler_RunStartStreamsRunEvents(t *testing.T) {
	bus, err := runtimebus.NewEventBus(stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	runID := "run_test_001"
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	receivedTypes := map[string]struct{}{}
	done := make(chan map[string]struct{}, 1)
	go func() {
		defer close(done)
		for {
			var frame builderWSEventFrame
			if err := conn.ReadJSON(&frame); err != nil {
				done <- receivedTypes
				return
			}
			if frame.Channel != "run:events:"+runID {
				continue
			}
			payload, ok := frame.Data.(map[string]any)
			if !ok {
				continue
			}
			eventType, _ := payload["type"].(string)
			if eventType != "" {
				receivedTypes[eventType] = struct{}{}
			}
			if _, ok := receivedTypes["run.started"]; ok {
				if _, ok := receivedTypes["event.fired"]; ok {
					if _, ok := receivedTypes["run.completed"]; ok {
						done <- receivedTypes
						return
					}
				}
			}
		}
	}()

	select {
	case got := <-done:
		if _, ok := got["run.started"]; !ok {
			t.Fatalf("expected run.started, got %#v", got)
		}
		if _, ok := got["event.fired"]; !ok {
			t.Fatalf("expected event.fired, got %#v", got)
		}
		if _, ok := got["run.completed"]; !ok {
			t.Fatalf("expected run.completed, got %#v", got)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for run events")
	}
}

func TestHandler_RunStopResetsRuntimeAndStreamsStopped(t *testing.T) {
	bus, err := runtimebus.NewEventBus(stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	runID := "run_test_stop_001"
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_stop_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.stop","params":{"run_id":"run_test_stop_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.stop status=%d body=%s", rec.Code, rec.Body.String())
	}

	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for run.stopped")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		eventType, _ := payload["type"].(string)
		if eventType != "run.stopped" {
			continue
		}
		if runtimeCtl.resetCalls != 1 {
			t.Fatalf("expected runtime reset once, got %d", runtimeCtl.resetCalls)
		}
		return
	}
}

func TestHandler_RunPauseAndContinueStreamStateChanges(t *testing.T) {
	bus, err := runtimebus.NewEventBus(stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	runID := "run_test_pause_001"
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_pause_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.pause","params":{"run_id":"run_test_pause_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.pause status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"12","method":"run.continue","params":{"run_id":"run_test_pause_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.continue status=%d body=%s", rec.Code, rec.Body.String())
	}

	received := map[string]struct{}{}
	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for pause/resume events: %#v", received)
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		eventType, _ := payload["type"].(string)
		if eventType == "" {
			continue
		}
		received[eventType] = struct{}{}
		if _, ok := received["run.paused"]; ok {
			if _, ok := received["run.resumed"]; ok {
				break
			}
		}
	}

	if runtimeCtl.pauseCalls != 1 {
		t.Fatalf("expected runtime pause once, got %d", runtimeCtl.pauseCalls)
	}
	if runtimeCtl.resumeCalls != 1 {
		t.Fatalf("expected runtime resume once, got %d", runtimeCtl.resumeCalls)
	}
}

func TestHandler_RunLifecycleOverAPIAliases(t *testing.T) {
	bus, err := runtimebus.NewEventBus(stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_api_alias_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_api_alias_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.pause","params":{"run_id":"run_test_api_alias_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.pause alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"12","method":"run.continue","params":{"run_id":"run_test_api_alias_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.continue alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	received := map[string]struct{}{}
	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for alias run events: %#v", received)
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read alias run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		eventType, _ := payload["type"].(string)
		if eventType == "" {
			continue
		}
		received[eventType] = struct{}{}
		if _, ok := received["run.started"]; ok {
			if _, ok := received["event.fired"]; ok {
				if _, ok := received["run.paused"]; ok {
					if _, ok := received["run.resumed"]; ok {
						if _, ok := received["run.completed"]; ok {
							break
						}
					}
				}
			}
		}
	}

	if runtimeCtl.pauseCalls != 1 {
		t.Fatalf("expected alias runtime pause once, got %d", runtimeCtl.pauseCalls)
	}
	if runtimeCtl.resumeCalls != 1 {
		t.Fatalf("expected alias runtime resume once, got %d", runtimeCtl.resumeCalls)
	}
}

func TestHandler_RunBreakpointHitPausesRuntime(t *testing.T) {
	bus, err := runtimebus.NewEventBus(stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_breakpoint_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_breakpoint_001","inputs":{"intake.requested":{"topic":"sample"}},"breakpoints":["agent-source"]}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	typedHandler, ok := handler.(*Handler)
	if !ok || !builderpkg.HandleRuntimeLogForTest(typedHandler.builder, runtimepkg.RuntimeLogEntry{
		Level:     "info",
		Component: "pipeline",
		Action:    "handled",
		AgentID:   "agent-source",
		EntityID:  runID,
		EventID:   "evt-breakpoint",
	}) {
		t.Fatalf("expected typed handler with builder runtime-log hook")
	}

	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for breakpoint event")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		if payload["type"] != "run.breakpoint_hit" {
			continue
		}
		if payload["node_id"] != "agent-source" {
			t.Fatalf("unexpected node_id: %#v", payload)
		}
		if payload["instance_id"] != runID {
			t.Fatalf("unexpected instance_id: %#v", payload)
		}
		break
	}

	if runtimeCtl.pauseCalls != 1 {
		t.Fatalf("expected runtime pause once, got %d", runtimeCtl.pauseCalls)
	}
}

func TestHandler_HumanTaskWaitingAndDecisionResume(t *testing.T) {
	bus, err := runtimebus.NewEventBus(stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_human_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_human_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	typedHandler, ok := handler.(*Handler)
	if !ok || !builderpkg.HandleRuntimeLogForTest(typedHandler.builder, runtimepkg.RuntimeLogEntry{
		Level:     "info",
		Component: "eventbus",
		Action:    "published",
		AgentID:   "agent-source",
		EntityID:  runID,
		EventType: "human_task.requested",
		EventID:   "evt-human",
		Detail: map[string]any{
			"type":   "human_task.requested",
			"source": "agent-source",
		},
	}) {
		t.Fatalf("expected typed handler with builder runtime-log hook")
	}

	receivedWaiting := false
	deadline := time.After(1 * time.Second)
	for !receivedWaiting {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for human.task_waiting")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		switch payload["type"] {
		case "human.task_waiting":
			receivedWaiting = true
		}
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.continue","params":{"run_id":"run_test_human_001","decision":"approved","instance_ids":["run_test_human_001"]}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.continue alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	receivedSubmitted := false
	receivedResumed := false
	deadline = time.After(1 * time.Second)
	for !(receivedSubmitted && receivedResumed) {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for human submit/resume events")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		switch payload["type"] {
		case "human.task_submitted":
			receivedSubmitted = true
		case "run.resumed":
			receivedResumed = true
		}
	}

	if runtimeCtl.pauseCalls != 1 {
		t.Fatalf("expected runtime pause once, got %d", runtimeCtl.pauseCalls)
	}
	if runtimeCtl.resumeCalls != 1 {
		t.Fatalf("expected runtime resume once, got %d", runtimeCtl.resumeCalls)
	}
}

func TestHandler_RunStepPausesAfterNextRuntimeEvent(t *testing.T) {
	bus, err := runtimebus.NewEventBus(stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_step_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_step_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.step","params":{"run_id":"run_test_step_001","node_id":"agent-source","instance_id":"run_test_step_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.step alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	typedHandler, ok := handler.(*Handler)
	if !ok || !builderpkg.HandleRuntimeLogForTest(typedHandler.builder, runtimepkg.RuntimeLogEntry{
		Level:     "info",
		Component: "pipeline",
		Action:    "handled",
		AgentID:   "agent-source",
		EntityID:  runID,
		EventID:   "evt-step",
	}) {
		t.Fatalf("expected typed handler with builder runtime-log hook")
	}

	receivedResumed := false
	receivedPaused := false
	deadline := time.After(1 * time.Second)
	for !(receivedResumed && receivedPaused) {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for step events")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		switch payload["type"] {
		case "run.resumed":
			if payload["node_id"] == "agent-source" {
				receivedResumed = true
			}
		case "run.paused":
			stepPayload, _ := payload["payload"].(map[string]any)
			if stepPayload["reason"] == "step_complete" {
				receivedPaused = true
			}
		}
	}

	if runtimeCtl.resumeCalls != 1 {
		t.Fatalf("expected runtime resume once, got %d", runtimeCtl.resumeCalls)
	}
	if runtimeCtl.pauseCalls != 1 {
		t.Fatalf("expected runtime pause once from step completion, got %d", runtimeCtl.pauseCalls)
	}
}

func TestHandler_RunRetryEmitsRetriedAndResumed(t *testing.T) {
	bus, err := runtimebus.NewEventBus(stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_retry_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_retry_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.retry","params":{"run_id":"run_test_retry_001","node_id":"agent-source","instance_id":"run_test_retry_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.retry alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	receivedRetried := false
	receivedResumed := false
	deadline := time.After(1 * time.Second)
	for !(receivedRetried && receivedResumed) {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for retry events")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		switch payload["type"] {
		case "handler.retried":
			receivedRetried = true
		case "run.resumed":
			modePayload, _ := payload["payload"].(map[string]any)
			if modePayload["mode"] == "retry" {
				receivedResumed = true
			}
		}
	}

	if runtimeCtl.resumeCalls != 1 {
		t.Fatalf("expected runtime resume once, got %d", runtimeCtl.resumeCalls)
	}
}

func TestHandler_RunSkipEmitsSkippedAndResumed(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_skip_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_skip_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.skip","params":{"run_id":"run_test_skip_001","node_id":"agent-source","instance_id":"run_test_skip_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.skip alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	receivedSkipped := false
	receivedResumed := false
	deadline := time.After(1 * time.Second)
	for !(receivedSkipped && receivedResumed) {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for skip events")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		switch payload["type"] {
		case "handler.skipped":
			receivedSkipped = true
		case "run.resumed":
			modePayload, _ := payload["payload"].(map[string]any)
			if modePayload["mode"] == "skip" {
				receivedResumed = true
			}
		}
	}

	if runtimeCtl.resumeCalls != 1 {
		t.Fatalf("expected runtime resume once, got %d", runtimeCtl.resumeCalls)
	}
}
