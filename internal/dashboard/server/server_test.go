package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	runtimeactors "empireai/internal/runtime/core/actors"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/DATA-DOG/go-sqlmock"
)

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

func TestHandler_ConversationsAndAggregates(t *testing.T) {
	now := time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC)
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
		Instances: stubInstances{
			rows: []runtimepipeline.WorkflowInstance{
				{InstanceID: "wf-1", WorkflowName: "order", CurrentState: "active", UpdatedAt: now},
				{InstanceID: "wf-2", WorkflowName: "order", CurrentState: "done", UpdatedAt: now.Add(-time.Minute)},
			},
			byID: map[string]runtimepipeline.WorkflowInstance{
				"wf-1": {InstanceID: "wf-1", WorkflowName: "order", CurrentState: "active", UpdatedAt: now},
			},
		},
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
