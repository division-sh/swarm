package store

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	runtimesessions "swarm/internal/runtime/sessions"
)

type fakeConversationCapabilitySource struct {
	caps StoreSchemaCapabilities
	err  error
}

func (s fakeConversationCapabilitySource) ResolveSchemaCapabilities(context.Context) (StoreSchemaCapabilities, error) {
	return s.caps, s.err
}

type fakeAgentConversationReadSource struct {
	caps      StoreSchemaCapabilities
	agents    []runtimemanager.PersistedAgent
	pending   map[string]PendingAgentDeliveryFacts
	lifecycle map[string]AgentLifecycleFacts
	err       error
}

func (s fakeAgentConversationReadSource) ResolveSchemaCapabilities(context.Context) (StoreSchemaCapabilities, error) {
	return s.caps, s.err
}

func (s fakeAgentConversationReadSource) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.agents, s.err
}

func (s fakeAgentConversationReadSource) ListPendingAgentDeliveryFacts(_ context.Context, agentIDs []string, _ time.Time) (map[string]PendingAgentDeliveryFacts, error) {
	out := make(map[string]PendingAgentDeliveryFacts, len(agentIDs))
	for _, agentID := range agentIDs {
		out[agentID] = s.pending[agentID]
	}
	return out, s.err
}

func (s fakeAgentConversationReadSource) ListAgentLifecycleFacts(_ context.Context, agentIDs []string) (map[string]AgentLifecycleFacts, error) {
	out := make(map[string]AgentLifecycleFacts, len(agentIDs))
	for _, agentID := range agentIDs {
		out[agentID] = s.lifecycle[agentID]
	}
	return out, s.err
}

func canonicalAgentConversationReadCaps() StoreSchemaCapabilities {
	return StoreSchemaCapabilities{
		Events: EventSchemaCapabilities{
			Log:        SchemaFlavorCanonical,
			Deliveries: SchemaFlavorCanonical,
			Receipts:   SchemaFlavorCanonical,
		},
		Conversations: ConversationSchemaCapabilities{
			Sessions:     SchemaFlavorCanonical,
			Turns:        SchemaFlavorCanonical,
			TurnBlocks:   true,
			SessionRunID: true,
		},
	}
}

func operatorAgentProjectionColumns() []string {
	return []string{
		"agent_id", "status", "session_id", "session_started_at", "turn_count", "lease_holder", "lease_expires_at", "runtime_state", "pending_count", "oldest_pending_age_sec",
		"failures_24h", "dead_letters_24h", "turns_24h", "turn_id", "task_id", "parse_ok", "error", "turn_created_at", "turn_blocks",
	}
}

func operatorConversationDetailColumns() []string {
	return []string{
		"session_id", "agent_id", "run_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "message_count", "runtime_state", "conversation", "started_at", "ended_at", "updated_at",
	}
}

func operatorConversationTurnColumns() []string {
	return []string{
		"turn_id", "agent_id", "session_id", "runtime_mode", "scope_key", "entity_id", "trigger_event_id", "trigger_event_type", "task_id", "available_tools", "tool_calls", "emitted_events", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible", "request_payload", "response_payload", "turn_blocks", "parse_ok", "latency_ms", "retry_count", "error", "created_at",
	}
}

func testOperatorAgent(agentID string) runtimemanager.PersistedAgent {
	return runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:               agentID,
			Role:             "researcher",
			Type:             "managed",
			ConversationMode: runtimesessions.RuntimeModeSession.String(),
			SessionScope:     runtimesessions.SessionScopeGlobal.String(),
		},
		Status:    "active",
		StartedAt: time.Date(2026, 5, 12, 8, 0, 0, 0, time.UTC),
	}
}

func TestOperatorConversationReadSurfaceListUsesCanonicalProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	runID := "11111111-1111-1111-1111-111111111111"
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	reader := NewOperatorConversationReadSurface(db, fakeConversationCapabilitySource{caps: StoreSchemaCapabilities{
		Conversations: ConversationSchemaCapabilities{
			Sessions:     SchemaFlavorCanonical,
			Turns:        SchemaFlavorCanonical,
			TurnBlocks:   true,
			SessionRunID: true,
		},
	}})

	mock.ExpectQuery("SELECT\\s+conversations\\.session_id,\\s+conversations\\.agent_id,\\s+conversations\\.run_id,.*FROM \\(").
		WithArgs("agent-1", runID, 3).
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "run_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "message_count", "runtime_state", "turn_id", "task_id", "parse_ok", "turn_blocks", "started_at", "ended_at", "updated_at",
		}).AddRow("sess-1", "agent-1", runID, "live_session", "global", "global", "session", "active", 2, 4, []byte(`{"summary":"brief"}`), "turn-1", "task-1", true, []byte(`[]`), now, nil, now))

	result, err := reader.ListOperatorConversations(context.Background(), OperatorConversationListOptions{
		AgentID: "agent-1",
		RunID:   runID,
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("ListOperatorConversations: %v", err)
	}
	if len(result.Conversations) != 1 {
		t.Fatalf("conversation count = %d", len(result.Conversations))
	}
	row := result.Conversations[0]
	if row.SessionID != "sess-1" || row.AgentID != "agent-1" || row.RunID != runID || row.MessageCount != 4 || row.Summary != "brief" {
		t.Fatalf("unexpected conversation row: %+v", row)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentProjectsSessionAndTurnRefs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sessionID := "11111111-1111-1111-1111-111111111111"
	turnID := "22222222-2222-2222-2222-222222222222"
	sessionStartedAt := time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC)
	turnCompletedAt := time.Date(2026, 5, 12, 9, 5, 0, 0, time.UTC)
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", sessionID, sessionStartedAt, 2, "", nil, []byte(`{"provider_session_id":"provider-sess-1"}`), 0, 0, 0, 0, 0, turnID, "task-1", false, "model error", turnCompletedAt, []byte(`[]`)))

	detail, err := reader.LoadOperatorAgent(context.Background(), "agent-1")
	if err != nil {
		t.Fatalf("LoadOperatorAgent: %v", err)
	}
	if detail.CurrentSessionRef == nil || detail.CurrentSessionRef.SessionID != sessionID || !detail.CurrentSessionRef.StartedAt.Equal(sessionStartedAt) {
		t.Fatalf("current_session_ref = %#v", detail.CurrentSessionRef)
	}
	if detail.LastTurnRef == nil || detail.LastTurnRef.TurnID != turnID || !detail.LastTurnRef.CompletedAt.Equal(turnCompletedAt) || detail.LastTurnRef.ParseOK || detail.LastTurnRef.Error != "model error" {
		t.Fatalf("last_turn_ref = %#v", detail.LastTurnRef)
	}
	if detail.Agent.CurrentSessionRef == nil || detail.Agent.LastTurnRef == nil {
		t.Fatalf("agent summary refs not populated: %+v", detail.Agent)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorConversationReadSurfaceCurrentForAgentUsesMostRecentActiveSessionOnly(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sessionID := "11111111-1111-1111-1111-111111111111"
	runID := "33333333-3333-3333-3333-333333333333"
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", sessionID, now.Add(-time.Hour), 2, "", nil, []byte(`{}`), 0, 0, 0, 0, 0, "", "", false, "", nil, []byte(`[]`)))
	mock.ExpectQuery("(?s)SELECT\\s+session_id::text,\\s+agent_id,.*FROM agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WithArgs("agent-1", runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorConversationDetailColumns()).
			AddRow(sessionID, "agent-1", runID, "live_session", "global", "global", "session", "active", 2, 1, []byte(`{"summary":"active session"}`), []byte(`[{"role":"assistant","content":"ready"}]`), now.Add(-time.Hour), nil, now))
	mock.ExpectQuery("(?s)SELECT\\s+turn_id::text,.*FROM agent_turns").
		WithArgs("agent-1", sessionID).
		WillReturnRows(sqlmock.NewRows(operatorConversationTurnColumns()))

	detail, err := reader.LoadCurrentOperatorConversationForAgent(context.Background(), "agent-1")
	if err != nil {
		t.Fatalf("LoadCurrentOperatorConversationForAgent: %v", err)
	}
	if detail == nil {
		t.Fatal("expected active conversation")
	}
	if detail.Conversation.SessionID != sessionID || detail.Conversation.Kind != "live_session" || detail.Conversation.Status != "active" {
		t.Fatalf("conversation = %+v", detail.Conversation)
	}
	if detail.Conversation.Summary != "active session" {
		t.Fatalf("summary = %q", detail.Conversation.Summary)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorConversationReadSurfaceCurrentForAgentReturnsNullWithoutActiveLiveSession(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "", nil, 0, "", nil, []byte(`{}`), 0, 0, 0, 0, 0, "", "", false, "", nil, []byte(`[]`)))
	mock.ExpectQuery("(?s)SELECT\\s+session_id::text,\\s+agent_id,.*FROM agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WithArgs("agent-1", runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorConversationDetailColumns()))

	detail, err := reader.LoadCurrentOperatorConversationForAgent(context.Background(), "agent-1")
	if err != nil {
		t.Fatalf("LoadCurrentOperatorConversationForAgent: %v", err)
	}
	if detail != nil {
		t.Fatalf("expected nil current conversation without active live session, got %+v", detail)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
