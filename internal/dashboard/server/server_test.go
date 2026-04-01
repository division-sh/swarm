package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gorilla/websocket"
	builderpkg "swarm/internal/builder"
	runtimepkg "swarm/internal/runtime"
	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimetools "swarm/internal/runtime/tools"
)

type builderRPCResponse = builderpkg.RPCResponse
type builderWSEventFrame = builderpkg.WSEventFrame

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
	list    []ConversationSummary
	byAgent map[string]ConversationDetail
}

func (s stubConversations) List(context.Context, int) ([]ConversationSummary, error) {
	return s.list, nil
}

func (s stubConversations) Get(_ context.Context, agentID string) (ConversationDetail, bool, error) {
	item, ok := s.byAgent[agentID]
	return item, ok, nil
}

type stubObservability struct {
	events      []eventRecord
	eventDetail map[string]eventRecord
	runtimeLogs []runtimeLogRecord
	incidents   []incidentRecord
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
		Version:        version,
		CurrentRuntime: runtimeProvider,
		ProjectControl: projectCtl,
	})
}

func TestHandler_ConversationsAndAggregates(t *testing.T) {
	now := time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC)
	agentCtl := &stubAgentControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"database": map[string]any{"ok": true}}, nil
		},
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
			byAgent: map[string]ConversationDetail{
				"agent-1": {
					AgentID:   "agent-1",
					UpdatedAt: now.Format(time.RFC3339),
					Messages:  []any{map[string]any{"role": "assistant", "content": "hi"}},
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
				{InstanceID: "wf-1", WorkflowName: "order", CurrentState: "active", UpdatedAt: now},
				{InstanceID: "wf-2", WorkflowName: "order", CurrentState: "done", UpdatedAt: now.Add(-time.Minute)},
			},
			byID: map[string]runtimepipeline.WorkflowInstance{
				"wf-1": {InstanceID: "wf-1", WorkflowName: "order", CurrentState: "active", UpdatedAt: now},
			},
		},
		Runtime: &stubRuntimeControl{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/conversations", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conversations status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/conversations/agent-1", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conversation detail status=%d body=%s", rec.Code, rec.Body.String())
	}

	var detail ConversationDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("unmarshal conversation detail: %v", err)
	}
	if detail.AgentID != "agent-1" || len(detail.Messages) != 1 {
		t.Fatalf("unexpected conversation detail: %+v", detail)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/events", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("events status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/events/evt-1", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("event detail status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/runtime/logs", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("runtime logs status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/runtime/incidents", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("runtime incidents status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/agents/agent-1", nil)
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
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("instance aggregate status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/health", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/agent-1/actions/restart", strings.NewReader(`{}`))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("agent restart status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/agents/agent-1/actions/directive", strings.NewReader(`{"message":"hello","kill_previous":true}`))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("agent directive status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !agentCtl.lastKillPrevious {
		t.Fatal("expected kill_previous to be forwarded to agent control")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/runtime/actions", strings.NewReader(`{"action":"pause"}`))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("runtime action status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestSQLConversationReader_ListAndGet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db)
	now := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	summaryState := `{"summary":"brief","last_turn":{"parse_ok":true},"provider_session_id":"sess-1"}`
	messagePayload := `[{"role":"assistant","content":"hello"}]`

	mock.ExpectQuery("SELECT\\s+agent_id,.*FROM agent_sessions").
		WithArgs(25).
		WillReturnRows(sqlmock.NewRows([]string{
			"agent_id", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "updated_at",
		}).AddRow("agent-1", "global", "global", "session", "active", 3, []byte(summaryState), now))

	items, err := reader.List(context.Background(), 25)
	if err != nil {
		t.Fatalf("list conversations: %v", err)
	}
	if len(items) != 1 || items[0].AgentID != "agent-1" || items[0].Summary != "brief" {
		t.Fatalf("unexpected summaries: %+v", items)
	}
	if items[0].Metadata["provider_session_id"] != "sess-1" {
		t.Fatalf("expected metadata to retain provider_session_id: %+v", items[0].Metadata)
	}

	mock.ExpectQuery("SELECT\\s+agent_id,.*COALESCE\\(conversation, '\\[\\]'::jsonb\\).*FROM agent_sessions").
		WithArgs("agent-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"agent_id", "scope_key", "scope", "runtime_mode", "status", "turn_count", "runtime_state", "conversation", "updated_at",
		}).AddRow("agent-1", "global", "global", "session", "active", 3, []byte(summaryState), []byte(messagePayload), now))

	item, ok, err := reader.Get(context.Background(), "agent-1")
	if err != nil {
		t.Fatalf("get conversation: %v", err)
	}
	if !ok {
		t.Fatalf("expected conversation to exist")
	}
	if item.AgentID != "agent-1" || len(item.Messages) != 1 {
		t.Fatalf("unexpected detail: %+v", item)
	}
	lastTurn, ok := item.RuntimeState["last_turn"].(map[string]any)
	if !ok || lastTurn["parse_ok"] != true {
		t.Fatalf("expected runtime_state.last_turn parse_ok=true, got %+v", item.RuntimeState)
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

	reader := NewSQLConversationReader(db)
	mock.ExpectQuery("SELECT\\s+agent_id,.*FROM agent_sessions").
		WithArgs("missing-agent").
		WillReturnError(sql.ErrNoRows)

	_, ok, err := reader.Get(context.Background(), "missing-agent")
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
	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`))
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
	req = httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"2","method":"state.list_instances"}`))
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
	req = httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"3","method":"state.get_instances"}`))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("state.get_instances status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"4","method":"state.get_entity","params":{"instance_id":"wf-1"}}`))
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
	req = httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"5","method":"project.open","params":{"project_dir":"/tmp/builder-project"}}`))
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
	req = httptest.NewRequest(http.MethodPost, "/api/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"6","method":"engine.ping"}`))
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
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
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
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
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
		Version: "swarm-test",
	})

	for _, path := range []string{"/healthz", "/api/healthz"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
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

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	runID := "run_test_001"
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_001","inputs":{"intake.requested":{"topic":"sample"}}}}`))
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

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	runID := "run_test_stop_001"
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_stop_001","inputs":{"intake.requested":{"topic":"sample"}}}}`))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"11","method":"run.stop","params":{"run_id":"run_test_stop_001"}}`))
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

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	runID := "run_test_pause_001"
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_pause_001","inputs":{"intake.requested":{"topic":"sample"}}}}`))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"11","method":"run.pause","params":{"run_id":"run_test_pause_001"}}`))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.pause status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"12","method":"run.continue","params":{"run_id":"run_test_pause_001"}}`))
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
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
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
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_api_alias_001","inputs":{"intake.requested":{"topic":"sample"}}}}`),
	)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"11","method":"run.pause","params":{"run_id":"run_test_api_alias_001"}}`),
	)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.pause alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"12","method":"run.continue","params":{"run_id":"run_test_api_alias_001"}}`),
	)
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
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
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
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_breakpoint_001","inputs":{"intake.requested":{"topic":"sample"}},"breakpoints":["agent-source"]}}`),
	)
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
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
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
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_human_001","inputs":{"intake.requested":{"topic":"sample"}}}}`),
	)
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
	req = httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"11","method":"run.continue","params":{"run_id":"run_test_human_001","decision":"approved","instance_ids":["run_test_human_001"]}}`),
	)
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
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
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
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_step_001","inputs":{"intake.requested":{"topic":"sample"}}}}`),
	)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"11","method":"run.step","params":{"run_id":"run_test_step_001","node_id":"agent-source","instance_id":"run_test_step_001"}}`),
	)
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
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
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
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_retry_001","inputs":{"intake.requested":{"topic":"sample"}}}}`),
	)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"11","method":"run.retry","params":{"run_id":"run_test_retry_001","node_id":"agent-source","instance_id":"run_test_retry_001"}}`),
	)
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
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
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
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_skip_001","inputs":{"intake.requested":{"topic":"sample"}}}}`),
	)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(
		http.MethodPost,
		"/api/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":"11","method":"run.skip","params":{"run_id":"run_test_skip_001","node_id":"agent-source","instance_id":"run_test_skip_001"}}`),
	)
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
