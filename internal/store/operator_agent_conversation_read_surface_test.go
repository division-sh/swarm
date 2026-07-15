package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type fakeConversationCapabilitySource struct {
	caps    StoreSchemaCapabilities
	turns   map[string][]OperatorPublicConversationTurn
	turnErr error
	err     error
}

func (s fakeConversationCapabilitySource) ResolveSchemaCapabilities(context.Context) (StoreSchemaCapabilities, error) {
	return s.caps, s.err
}

func (s fakeConversationCapabilitySource) ListOperatorConversationTurns(_ context.Context, opts OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error) {
	if s.turnErr != nil {
		return OperatorConversationTurnListResult{}, s.turnErr
	}
	publicTurns := s.turns[strings.TrimSpace(opts.SessionID)]
	page := OperatorConversationTurnListResult{Turns: []OperatorConversationTurnListItem{}}
	for _, turn := range publicTurns {
		page.Turns = append(page.Turns, operatorConversationTurnListItemFromPublic(turn))
	}
	return page, nil
}

func (s fakeConversationCapabilitySource) LoadOperatorPublicConversationTurn(_ context.Context, sessionID, turnID string) (OperatorPublicConversationTurnDetail, error) {
	for _, turn := range s.turns[strings.TrimSpace(sessionID)] {
		if turn.TurnID == strings.TrimSpace(turnID) {
			return OperatorPublicConversationTurnDetail{Turn: turn}, nil
		}
	}
	return OperatorPublicConversationTurnDetail{}, ErrTurnNotFound
}

type fakeAgentConversationReadSource struct {
	caps      StoreSchemaCapabilities
	agents    []runtimemanager.PersistedAgent
	pending   map[string]PendingAgentDeliveryFacts
	details   map[string]PendingAgentDeliveryPage
	lifecycle map[string]AgentDeliveryLifecycleFacts
	turns     map[string][]OperatorPublicConversationTurn
	turnErr   error
	err       error
	detailErr error
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

func (s fakeAgentConversationReadSource) ListPendingAgentDeliveryDetails(_ context.Context, opts PendingAgentDeliveryListOptions) (PendingAgentDeliveryPage, error) {
	if s.detailErr != nil {
		return PendingAgentDeliveryPage{}, s.detailErr
	}
	page, ok := s.details[strings.TrimSpace(opts.AgentID)]
	if !ok {
		return PendingAgentDeliveryPage{PendingDeliveries: []PendingAgentDeliveryDetail{}}, s.err
	}
	if page.PendingDeliveries == nil {
		page.PendingDeliveries = []PendingAgentDeliveryDetail{}
	}
	return page, s.err
}

func (s fakeAgentConversationReadSource) ListAgentDeliveryLifecycleFacts(_ context.Context, agentIDs []string) (map[string]AgentDeliveryLifecycleFacts, error) {
	out := make(map[string]AgentDeliveryLifecycleFacts, len(agentIDs))
	for _, agentID := range agentIDs {
		out[agentID] = s.lifecycle[agentID]
	}
	return out, s.err
}

func (s fakeAgentConversationReadSource) ListOperatorConversationTurns(_ context.Context, opts OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error) {
	if s.turnErr != nil {
		return OperatorConversationTurnListResult{}, s.turnErr
	}
	publicTurns := s.turns[strings.TrimSpace(opts.SessionID)]
	page := OperatorConversationTurnListResult{Turns: []OperatorConversationTurnListItem{}}
	for _, turn := range publicTurns {
		page.Turns = append(page.Turns, operatorConversationTurnListItemFromPublic(turn))
	}
	return page, nil
}

func (s fakeAgentConversationReadSource) LoadOperatorPublicConversationTurn(_ context.Context, sessionID, turnID string) (OperatorPublicConversationTurnDetail, error) {
	if s.turnErr != nil {
		return OperatorPublicConversationTurnDetail{}, s.turnErr
	}
	for _, turn := range s.turns[strings.TrimSpace(sessionID)] {
		if turn.TurnID == strings.TrimSpace(turnID) {
			return OperatorPublicConversationTurnDetail{Turn: turn}, nil
		}
	}
	return OperatorPublicConversationTurnDetail{}, ErrTurnNotFound
}

func TestOperatorAgentSummaryPublishesCanonicalMemoryFacts(t *testing.T) {
	memorySummary := operatorAgentSummaryFromPersisted(runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "memory-agent",
			Role:     "worker",
			Type:     "managed",
			Model:    "cheap",
			Memory:   agentmemory.Authored(true),
			FlowPath: "support/chat-1",
		},
	}, operatorAgentProjection{LifecycleState: "active"}, 0)
	if !memorySummary.Memory || memorySummary.MemorySource != string(agentmemory.SourceAuthored) {
		t.Fatalf("memory summary = enabled:%v source:%q, want authored true", memorySummary.Memory, memorySummary.MemorySource)
	}
	if memorySummary.FlowInstance != "support/chat-1" {
		t.Fatalf("FlowInstance = %q, want support/chat-1", memorySummary.FlowInstance)
	}
	raw, err := json.Marshal(memorySummary)
	if err != nil {
		t.Fatalf("marshal summary: %v", err)
	}
	text := string(raw)
	if !strings.Contains(text, `"memory":true`) || !strings.Contains(text, `"memory_source":"authored"`) {
		t.Fatalf("summary json = %s, want canonical memory facts", text)
	}
	for _, retired := range []string{"conversation_mode", "session_scope", `"mode"`} {
		if strings.Contains(text, retired) {
			t.Fatalf("summary json = %s, must not expose retired %s", text, retired)
		}
	}

	defaultSummary := operatorAgentSummaryFromPersisted(runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "stateless-agent",
			Role:   "worker",
			Type:   "managed",
			Model:  "cheap",
			Memory: agentmemory.PlatformDefault(),
		},
	}, operatorAgentProjection{}, 0)
	if defaultSummary.Memory || defaultSummary.MemorySource != string(agentmemory.SourcePlatformDefault) {
		t.Fatalf("default memory summary = enabled:%v source:%q, want platform-default false", defaultSummary.Memory, defaultSummary.MemorySource)
	}
	raw, err = json.Marshal(defaultSummary)
	if err != nil {
		t.Fatalf("marshal task summary: %v", err)
	}
	text = string(raw)
	if !strings.Contains(text, `"memory":false`) || !strings.Contains(text, `"memory_source":"platform_default"`) {
		t.Fatalf("default summary json = %s, want platform-default memory facts", text)
	}
	for _, retired := range []string{"conversation_mode", "session_scope", `"mode"`} {
		if strings.Contains(text, retired) {
			t.Fatalf("default summary json = %s, must not expose retired %s", text, retired)
		}
	}
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
	}
}

func TestCanonicalStatelessConversationVisibilitySourceProjectsRunIDByCapability(t *testing.T) {
	withRunID := CanonicalStatelessConversationVisibilitySourceSQL(ConversationSchemaCapabilities{
		Audits:     SchemaFlavorCanonical,
		AuditRunID: true,
	})
	if !strings.Contains(withRunID, "COALESCE(run_id::text, '') AS run_id") {
		t.Fatalf("audit run_id projection missing from canonical source:\n%s", withRunID)
	}

	withoutRunID := CanonicalStatelessConversationVisibilitySourceSQL(ConversationSchemaCapabilities{
		Audits: SchemaFlavorCanonical,
	})
	if !strings.Contains(withoutRunID, "'' AS run_id") {
		t.Fatalf("audit no-run_id projection missing stable blank run_id:\n%s", withoutRunID)
	}
	if strings.Contains(withoutRunID, "COALESCE(run_id::text") {
		t.Fatalf("audit no-run_id projection referenced missing run_id column:\n%s", withoutRunID)
	}
}

func TestOperatorConversationQuerySourcesRunIDCapabilityMatrix(t *testing.T) {
	tests := []struct {
		name         string
		sessionRunID bool
		auditRunID   bool
	}{
		{name: "both", sessionRunID: true, auditRunID: true},
		{name: "session only", sessionRunID: true},
		{name: "audit only", auditRunID: true},
		{name: "neither"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sources := operatorConversationQuerySources(StoreSchemaCapabilities{
				Conversations: ConversationSchemaCapabilities{
					Sessions:     SchemaFlavorCanonical,
					Audits:       SchemaFlavorCanonical,
					Turns:        SchemaFlavorCanonical,
					SessionRunID: tc.sessionRunID,
					AuditRunID:   tc.auditRunID,
				},
			})
			if len(sources) != 2 {
				t.Fatalf("source count = %d, want 2", len(sources))
			}
			for _, source := range sources {
				switch {
				case strings.Contains(source, "'live_session' AS kind"):
					assertRunIDSourceProjection(t, source, tc.sessionRunID)
				case strings.Contains(source, "'turn_audit' AS kind"):
					assertRunIDSourceProjection(t, source, tc.auditRunID)
				default:
					t.Fatalf("unknown conversation source:\n%s", source)
				}
			}
		})
	}
}

func assertRunIDSourceProjection(t *testing.T, source string, hasRunID bool) {
	t.Helper()
	if hasRunID {
		if !strings.Contains(source, "COALESCE(run_id::text, '') AS run_id") {
			t.Fatalf("run_id-capable source did not project run_id:\n%s", source)
		}
		return
	}
	if !strings.Contains(source, "'' AS run_id") {
		t.Fatalf("non-run_id source did not project stable blank run_id:\n%s", source)
	}
}

func TestOperatorConversationReadSurfaceListRejectsRunIDFilterWithoutRunIDCapability(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewOperatorConversationReadSurface(db, fakeConversationCapabilitySource{caps: StoreSchemaCapabilities{
		Conversations: ConversationSchemaCapabilities{
			Sessions:   SchemaFlavorCanonical,
			Audits:     SchemaFlavorCanonical,
			Turns:      SchemaFlavorCanonical,
			TurnBlocks: true,
		},
	}})

	_, err = reader.ListOperatorConversations(context.Background(), OperatorConversationListOptions{
		RunID: "11111111-1111-1111-1111-111111111111",
	})
	if !errors.Is(err, ErrOperatorConversationRunIDCapability) {
		t.Fatalf("ListOperatorConversations err = %v, want ErrOperatorConversationRunIDCapability", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "pq:") || strings.Contains(strings.ToLower(err.Error()), `column "run_id"`) {
		t.Fatalf("capability error leaked driver detail: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorConversationReadSurfaceSanitizesRunIDProjectionDriverError(t *testing.T) {
	err := operatorConversationReadQueryError("load operator conversation", errors.New(`pq: column "run_id" does not exist at position 46:14 (42703)`))
	if !errors.Is(err, ErrOperatorConversationRunIDCapability) {
		t.Fatalf("sanitized err = %v, want ErrOperatorConversationRunIDCapability", err)
	}
	lower := strings.ToLower(err.Error())
	for _, forbidden := range []string{"pq:", `column "run_id"`, "42703", "position"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("sanitized err leaked %q: %v", forbidden, err)
		}
	}
}

func testOperatorAgent(agentID string) runtimemanager.PersistedAgent {
	return runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            agentID,
			Role:          "researcher",
			Type:          "managed",
			ExecutionMode: "live",
			Memory:        agentmemory.Authored(true),
			FlowPath:      "global",
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
	reader := NewOperatorConversationReadSurface(db, fakeConversationCapabilitySource{
		caps: StoreSchemaCapabilities{Conversations: ConversationSchemaCapabilities{
			Sessions:     SchemaFlavorCanonical,
			Turns:        SchemaFlavorCanonical,
			TurnBlocks:   true,
			SessionRunID: true,
		}},
		turns: map[string][]OperatorPublicConversationTurn{
			"sess-1": {{TurnID: "turn-1", TaskID: "task-1", ParseOK: true, Activity: []OperatorConversationActivity{}}},
		},
	})

	mock.ExpectQuery("SELECT\\s+conversations\\.session_id,\\s+conversations\\.agent_id,\\s+conversations\\.run_id,.*FROM \\(").
		WithArgs("agent-1", runID, 3).
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "run_id", "kind", "flow_instance", "memory_enabled", "memory_source", "status", "turn_count", "message_count", "runtime_state", "started_at", "ended_at", "updated_at",
		}).AddRow("sess-1", "agent-1", runID, "live_session", "global", true, "authored", "active", 2, 4, []byte(`{"summary":"brief"}`), now, nil, now))

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
	if row.Metadata.LiveTurn == nil || row.Metadata.LiveTurn.TurnID != "turn-1" || row.Metadata.LiveTurn.TaskID != "task-1" {
		t.Fatalf("latest public turn = %#v", row.Metadata.LiveTurn)
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
	turnFailure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassConnectorFailure, "model_error", "llm-runtime", "turn", nil), "llm-runtime", "turn")
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
		turns: map[string][]OperatorPublicConversationTurn{
			sessionID: {{
				TurnID: turnID, CompletedAt: turnCompletedAt, ParseOK: false, Failure: &turnFailure,
			}},
		},
	}, 0)
	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", sessionID, sessionStartedAt, 2, "lease-owner", time.Now().Add(time.Minute), []byte(`{"provider_session_id":"provider-sess-1"}`), 0, 0))

	detail, err := reader.LoadOperatorAgent(context.Background(), "agent-1")
	if err != nil {
		t.Fatalf("LoadOperatorAgent: %v", err)
	}
	if detail.CurrentSessionRef == nil || detail.CurrentSessionRef.SessionID != sessionID || !detail.CurrentSessionRef.StartedAt.Equal(sessionStartedAt) {
		t.Fatalf("current_session_ref = %#v", detail.CurrentSessionRef)
	}
	if detail.LastTurnRef == nil || detail.LastTurnRef.TurnID != turnID || !detail.LastTurnRef.CompletedAt.Equal(turnCompletedAt) || detail.LastTurnRef.ParseOK || detail.LastTurnRef.Failure == nil || detail.LastTurnRef.Failure.Detail.Code != "model_error" {
		t.Fatalf("last_turn_ref = %#v", detail.LastTurnRef)
	}
	if detail.Agent.Status != "idle" {
		t.Fatalf("agent.status = %q, want idle from empty canonical lifecycle facts", detail.Agent.Status)
	}
	if detail.Agent.DashboardState != "idle" {
		t.Fatalf("dashboard state = %q, want idle from empty canonical lifecycle facts", detail.Agent.DashboardState)
	}
	if detail.Agent.BlockingLayer != "" {
		t.Fatalf("blocking_layer = %q, want empty without canonical lifecycle blocker", detail.Agent.BlockingLayer)
	}
	if detail.Agent.CurrentSessionRef == nil || detail.Agent.LastTurnRef == nil {
		t.Fatalf("agent summary refs not populated: %+v", detail.Agent)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceListAgentsDoesNotDeriveStatusFromActiveLease(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
		pending: map[string]PendingAgentDeliveryFacts{
			"agent-1": {},
		},
		lifecycle: map[string]AgentDeliveryLifecycleFacts{
			"agent-1": {},
		},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 2, "lease-owner", time.Now().Add(time.Minute), []byte(`{}`), 0, 0))

	result, err := reader.ListOperatorAgents(context.Background(), OperatorAgentListOptions{})
	if err != nil {
		t.Fatalf("ListOperatorAgents: %v", err)
	}
	if len(result.Agents) != 1 {
		t.Fatalf("agent count = %d, want 1", len(result.Agents))
	}
	agent := result.Agents[0]
	if agent.Status != "idle" {
		t.Fatalf("status = %q, want idle from empty canonical lifecycle facts", agent.Status)
	}
	if agent.DashboardState != "idle" {
		t.Fatalf("dashboard state = %q, want idle from empty canonical lifecycle facts", agent.DashboardState)
	}
	if agent.BlockingLayer != "" {
		t.Fatalf("blocking_layer = %q, want empty without canonical lifecycle blocker", agent.BlockingLayer)
	}
	if agent.LockOwner != "lease-owner" || agent.LockExpiresAt.IsZero() {
		t.Fatalf("raw lease metadata not preserved as debug data: %+v", agent)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisUsesSelectedOwners(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sessionID := "11111111-1111-1111-1111-111111111111"
	turnID := "22222222-2222-2222-2222-222222222222"
	turnEntityID := "33333333-3333-3333-3333-333333333333"
	configuredEntityID := "44444444-4444-4444-4444-444444444444"
	sessionStartedAt := time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC)
	turnCompletedAt := time.Date(2026, 5, 12, 9, 5, 0, 0, time.UTC)
	eventTime := time.Date(2026, 5, 12, 8, 55, 0, 0, time.UTC)
	runtimeState := []byte(`{"provider_session_id":"provider-sess-1","watchdog":{"state":"healthy_long_running","blocking_layer":"session_execution","action":"turn_long_running","outcome":"observed","last_output_at":"2026-05-12T09:04:00Z","recorded_at":"2026-05-12T09:05:00Z"}}`)
	agent := testOperatorAgent("agent-1")
	agent.Config.EntityID = configuredEntityID
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{agent},
		pending: map[string]PendingAgentDeliveryFacts{
			"agent-1": {PendingCount: 99, OldestPendingAgeSec: 999},
		},
		details: map[string]PendingAgentDeliveryPage{
			"agent-1": {
				PendingCount:        3,
				OldestPendingAgeSec: 90,
				PendingDeliveries: []PendingAgentDeliveryDetail{{
					EventID:    "event-1",
					EventName:  "task.ready",
					EnqueuedAt: eventTime,
					Attempts:   1,
				}},
				NextCursor: "cursor-2",
			},
		},
		lifecycle: map[string]AgentDeliveryLifecycleFacts{
			"agent-1": {CurrentState: "active", BlockingLayer: "session_execution"},
		},
		turns: map[string][]OperatorPublicConversationTurn{
			sessionID: {{
				TurnID: turnID, TaskID: "task-1", EntityID: turnEntityID, CompletedAt: turnCompletedAt, ParseOK: true,
				Activity: []OperatorConversationActivity{
					{Kind: "tool_result", ToolName: "older_tool", ToolUseID: "toolu-old", OK: boolPointer(true)},
					{Kind: "tool_result", ToolName: "selected_tool", ToolUseID: "toolu-selected", OK: boolPointer(true)},
				},
			}},
		},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", sessionID, sessionStartedAt, 2, "", nil, runtimeState, 0, 0))

	diagnosis, err := reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{QueueLimit: 1, QueueCursor: "cursor-1"})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDiagnosis: %v", err)
	}
	if diagnosis.AgentID != "agent-1" || diagnosis.Status != "running" {
		t.Fatalf("identity/status = %#v", diagnosis)
	}
	if diagnosis.CurrentSessionRef == nil || diagnosis.CurrentSessionRef.SessionID != sessionID || !diagnosis.CurrentSessionRef.StartedAt.Equal(sessionStartedAt) {
		t.Fatalf("current_session_ref = %#v", diagnosis.CurrentSessionRef)
	}
	if diagnosis.LastTurnRef == nil || diagnosis.LastTurnRef.TurnID != turnID || !diagnosis.LastTurnRef.CompletedAt.Equal(turnCompletedAt) {
		t.Fatalf("last_turn_ref = %#v", diagnosis.LastTurnRef)
	}
	if diagnosis.Active == nil || diagnosis.Active.TurnID != turnID || diagnosis.Active.TaskID != "task-1" || diagnosis.Active.EntityID != turnEntityID {
		t.Fatalf("active = %#v", diagnosis.Active)
	}
	if diagnosis.Queue.PendingCount != 3 || diagnosis.Queue.OldestPendingAgeSeconds != 90 {
		t.Fatalf("queue = %#v", diagnosis.Queue)
	}
	if len(diagnosis.Queue.PendingDeliveries) != 1 {
		t.Fatalf("pending deliveries = %#v", diagnosis.Queue.PendingDeliveries)
	}
	if got := diagnosis.Queue.PendingDeliveries[0]; got.EventID != "event-1" || got.EventName != "task.ready" || !got.EnqueuedAt.Equal(eventTime) || got.Attempts != 1 {
		t.Fatalf("pending delivery = %#v", got)
	}
	if diagnosis.Queue.NextCursor != "cursor-2" {
		t.Fatalf("next cursor = %q", diagnosis.Queue.NextCursor)
	}
	if diagnosis.DeliveryLifecycle == nil || diagnosis.DeliveryLifecycle.State != "active" || diagnosis.DeliveryLifecycle.BlockingLayer != "session_execution" {
		t.Fatalf("delivery_lifecycle = %#v", diagnosis.DeliveryLifecycle)
	}
	if diagnosis.RuntimeState == nil || diagnosis.RuntimeState.Watchdog == nil {
		t.Fatalf("runtime_state.watchdog = %#v", diagnosis.RuntimeState)
	}
	watchdog := diagnosis.RuntimeState.Watchdog
	if watchdog.State != "healthy_long_running" || watchdog.BlockingLayer != "session_execution" || watchdog.Action != "turn_long_running" || watchdog.Outcome != "observed" {
		t.Fatalf("runtime_state.watchdog = %#v", watchdog)
	}
	if watchdog.LastOutputAt != "2026-05-12T09:04:00Z" || watchdog.RecordedAt != "2026-05-12T09:05:00Z" {
		t.Fatalf("runtime_state.watchdog timestamps = %#v", watchdog)
	}
	if diagnosis.LastToolOutcome == nil {
		t.Fatalf("last_tool_outcome = nil, want selected latest tool")
	}
	lastTool := diagnosis.LastToolOutcome
	if lastTool.TurnID != turnID || lastTool.ToolName != "selected_tool" || lastTool.ToolUseID != "toolu-selected" || !lastTool.OK {
		t.Fatalf("last_tool_outcome = %#v", lastTool)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDeliveryDiagnosticsPromotesCanonicalOwner(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-1",
			Role:          "researcher",
			Type:          "managed",
			Model:         "cheap",
			ExecutionMode: "live",
			Memory:        agentmemory.PlatformDefault(),
			Config:        json.RawMessage(`{"system_prompt":"diagnose"}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	now := time.Now().UTC()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	failedNewEventID := uuid.NewString()
	failedOldEventID := uuid.NewString()
	deadEventID := uuid.NewString()
	otherAgentEventID := uuid.NewString()
	for _, event := range []struct {
		id   string
		name string
	}{
		{failedNewEventID, "task.failed.new"},
		{failedOldEventID, "task.failed.old"},
		{deadEventID, "task.dead"},
		{otherAgentEventID, "task.other"},
	} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (execution_mode,
				event_id, run_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
			) VALUES ('live',
				$1::uuid, $2::uuid, $3, $4::uuid, 'global', '{}'::jsonb, 'runtime', 'agent', $5
			)
		`, event.id, runID, event.name, entityID, now.Add(-10*time.Minute)); err != nil {
			t.Fatalf("seed event %s: %v", event.name, err)
		}
	}
	failedNewDeliveryID := uuid.NewString()
	failedOldDeliveryID := uuid.NewString()
	deadDeliveryID := uuid.NewString()
	otherDeliveryID := uuid.NewString()
	newFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "new_failure", nil))
	oldFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "old_failure", nil))
	terminalFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassRetryExhausted, "terminal_failure", nil))
	otherAgentFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "other_agent_failure", nil))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, failure, delivered_at, created_at
		) VALUES
			($1::uuid, $2::uuid, $3::uuid, 'agent', 'agent-1', 'failed', 2, 'handler_error', $4::jsonb, $5, $6),
			($7::uuid, $2::uuid, $8::uuid, 'agent', 'agent-1', 'failed', 1, 'handler_error', $9::jsonb, $10, $6),
			($11::uuid, $2::uuid, $12::uuid, 'agent', 'agent-1', 'dead_letter', 3, 'retry_exhausted', $13::jsonb, $14, $6),
			($15::uuid, $2::uuid, $16::uuid, 'agent', 'agent-2', 'failed', 1, 'handler_error', $17::jsonb, $5, $6)
	`, failedNewDeliveryID, runID, failedNewEventID, newFailure, now.Add(-1*time.Minute), now.Add(-15*time.Minute),
		failedOldDeliveryID, failedOldEventID, oldFailure, now.Add(-2*time.Minute),
		deadDeliveryID, deadEventID, terminalFailure, now.Add(-3*time.Minute),
		otherDeliveryID, otherAgentEventID, otherAgentFailure); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	deadLetterID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dead_letters (
			dead_letter_id, original_event_id, original_event, original_payload, flow_instance,
			failure, retry_count, chain_depth, handler_node, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'task.dead', '{}'::jsonb, 'flow/test',
			$3::jsonb, 3, 0, 'agent-1', $4
		)
	`, deadLetterID, deadEventID, terminalFailure, now.Add(-2*time.Minute)); err != nil {
		t.Fatalf("seed dead letter: %v", err)
	}

	first, err := pg.LoadOperatorAgentDeliveryDiagnostics(ctx, "agent-1", OperatorAgentDeliveryDiagnosticsOptions{
		FailureLimit:    1,
		DeadLetterLimit: 10,
	})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDeliveryDiagnostics first page: %v", err)
	}
	if first.AgentID != "agent-1" {
		t.Fatalf("agent_id = %q", first.AgentID)
	}
	if first.Summary.Failures24h != 2 || first.Summary.DeadLetters24h != 1 {
		t.Fatalf("summary = %#v, want failures=2 dead_letters=1", first.Summary)
	}
	if len(first.Failures) != 1 || first.Failures[0].DeliveryID != failedNewDeliveryID || first.Failures[0].Status != "failed" {
		t.Fatalf("first failures page = %#v", first.Failures)
	}
	if first.Failures[0].EventName != "task.failed.new" || first.Failures[0].RunID != runID || first.Failures[0].EntityID != entityID || first.Failures[0].RetryCount != 2 {
		t.Fatalf("failure row = %#v", first.Failures[0])
	}
	if first.FailuresNextCursor == "" {
		t.Fatal("failures_next_cursor empty, want second page")
	}
	if len(first.DeadLetters) != 1 || first.DeadLetters[0].DeliveryID != deadDeliveryID || first.DeadLetters[0].Status != "dead_letter" {
		t.Fatalf("dead letters = %#v", first.DeadLetters)
	}
	if len(first.DeadLetters[0].DeadLetterRecords) != 1 || first.DeadLetters[0].DeadLetterRecords[0].DeadLetterID != deadLetterID {
		t.Fatalf("dead letter records = %#v", first.DeadLetters[0].DeadLetterRecords)
	}

	second, err := pg.LoadOperatorAgentDeliveryDiagnostics(ctx, "agent-1", OperatorAgentDeliveryDiagnosticsOptions{
		FailureLimit:  1,
		FailureCursor: first.FailuresNextCursor,
	})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDeliveryDiagnostics second page: %v", err)
	}
	if len(second.Failures) != 1 || second.Failures[0].DeliveryID != failedOldDeliveryID {
		t.Fatalf("second failures page = %#v", second.Failures)
	}
	if second.FailuresNextCursor != "" {
		t.Fatalf("second failures_next_cursor = %q, want empty", second.FailuresNextCursor)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentUsageSplitsExactAndEstimated(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-1",
			Role:          "researcher",
			Type:          "managed",
			Model:         "cheap",
			ExecutionMode: "live",
			Memory:        agentmemory.PlatformDefault(),
			Config:        json.RawMessage(`{"system_prompt":"usage"}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent agent-1: %v", err)
	}
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-2",
			Role:          "other",
			Type:          "managed",
			Model:         "cheap",
			ExecutionMode: "live",
			Memory:        agentmemory.PlatformDefault(),
			Config:        json.RawMessage(`{"system_prompt":"usage"}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent agent-2: %v", err)
	}

	since := time.Date(2026, 5, 21, 9, 0, 0, 0, time.UTC)
	until := time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC)
	rows := []struct {
		agentID         string
		model           string
		modelAlias      string
		backendProfile  string
		provider        string
		transport       string
		resolvedModel   string
		inputTokens     int
		outputTokens    int
		costUSD         string
		invocationType  string
		usageAccounting string
		createdAt       time.Time
	}{
		{"agent-1", "claude-3-5-sonnet", "regular", "anthropic", "anthropic", "api", "claude-3-5-sonnet", 100, 25, "0.000675", "anthropic", AgentUsageAccountingExact, since},
		{"agent-1", "sonnet", "regular", "claude_cli", "claude", "cli", "sonnet", 50, 10, "0.000300", "claude_cli", AgentUsageAccountingEstimated, since.Add(time.Minute)},
		{"agent-1", "claude-3-5-sonnet", "regular", "anthropic", "anthropic", "api", "claude-3-5-sonnet", 7, 3, "0.000010", "anthropic", AgentUsageAccountingExact, until},
		{"agent-2", "claude-3-5-sonnet", "regular", "anthropic", "anthropic", "api", "claude-3-5-sonnet", 999, 999, "1.000000", "anthropic", AgentUsageAccountingExact, since.Add(time.Minute)},
	}
	for _, row := range rows {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO spend_ledger (
				flow_instance, agent_id, model, model_alias, backend_profile, provider, transport, resolved_model, input_tokens, output_tokens,
				cost_usd, invocation_type, usage_accounting, execution_mode, created_at
			) VALUES (
				'flow/a', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::numeric, $11, $12, 'live', $13
			)
		`, row.agentID, row.model, row.modelAlias, row.backendProfile, row.provider, row.transport, row.resolvedModel, row.inputTokens, row.outputTokens, row.costUSD, row.invocationType, row.usageAccounting, row.createdAt); err != nil {
			t.Fatalf("seed spend row %+v: %v", row, err)
		}
	}

	result, err := pg.LoadOperatorAgentUsage(ctx, "agent-1", OperatorAgentUsageOptions{Since: &since, Until: &until})
	if err != nil {
		t.Fatalf("LoadOperatorAgentUsage: %v", err)
	}
	if result.AgentID != "agent-1" {
		t.Fatalf("agent_id = %q", result.AgentID)
	}
	if result.Window.Since == nil || !result.Window.Since.Equal(since) || result.Window.Until == nil || !result.Window.Until.Equal(until) {
		t.Fatalf("window = %#v", result.Window)
	}
	if result.Usage.Exact.LedgerEntries != 1 || result.Usage.Exact.InputTokens != 100 || result.Usage.Exact.OutputTokens != 25 {
		t.Fatalf("exact usage = %#v", result.Usage.Exact)
	}
	if result.Usage.Estimated.LedgerEntries != 1 || result.Usage.Estimated.InputTokens != 50 || result.Usage.Estimated.OutputTokens != 10 {
		t.Fatalf("estimated usage = %#v", result.Usage.Estimated)
	}
	if len(result.Breakdown) != 2 {
		t.Fatalf("breakdown = %#v, want two rows", result.Breakdown)
	}
	if got := result.Breakdown[0]; got.UsageAccounting != AgentUsageAccountingExact || got.InvocationType != "anthropic" || got.Model != "claude-3-5-sonnet" || got.ModelAlias != "regular" || got.BackendProfile != "anthropic" || got.Provider != "anthropic" || got.Transport != "api" || got.ResolvedModel != "claude-3-5-sonnet" {
		t.Fatalf("first breakdown = %#v", got)
	}
	if got := result.Breakdown[1]; got.UsageAccounting != AgentUsageAccountingEstimated || got.InvocationType != "claude_cli" || got.Model != "sonnet" || got.ModelAlias != "regular" || got.BackendProfile != "claude_cli" || got.Provider != "claude" || got.Transport != "cli" || got.ResolvedModel != "sonnet" {
		t.Fatalf("second breakdown = %#v", got)
	}
}

func TestSQLiteRuntimeStoreLoadAgentUsageSplitsExactAndEstimated(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-1", "active")
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-2", "active")

	since := time.Date(2026, 5, 21, 9, 0, 0, 0, time.UTC)
	until := time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC)
	records := []budgetspend.SpendRecord{
		{ExecutionMode: "live", FlowInstance: "flow/a", AgentID: "agent-1", Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 100, OutputTokens: 25, CostUSD: 0.000675, InvocationType: "anthropic", UsageAccounting: AgentUsageAccountingExact, RecordedAt: since},
		{ExecutionMode: "live", FlowInstance: "flow/a", AgentID: "agent-1", Model: "sonnet", ModelAlias: "regular", BackendProfile: "claude_cli", Provider: "claude", Transport: "cli", ResolvedModel: "sonnet", InputTokens: 50, OutputTokens: 10, CostUSD: 0.000300, InvocationType: "claude_cli", UsageAccounting: AgentUsageAccountingEstimated, RecordedAt: since.Add(time.Minute)},
		{ExecutionMode: "live", FlowInstance: "flow/a", AgentID: "agent-1", Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 7, OutputTokens: 3, CostUSD: 0.000010, InvocationType: "anthropic", UsageAccounting: AgentUsageAccountingExact, RecordedAt: until},
		{ExecutionMode: "live", FlowInstance: "flow/a", AgentID: "agent-2", Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 999, OutputTokens: 999, CostUSD: 1.000000, InvocationType: "anthropic", UsageAccounting: AgentUsageAccountingExact, RecordedAt: since.Add(time.Minute)},
	}
	for _, rec := range records {
		if err := sqliteStore.RecordSpend(ctx, rec); err != nil {
			t.Fatalf("RecordSpend(%s/%s): %v", rec.AgentID, rec.UsageAccounting, err)
		}
	}

	result, err := sqliteStore.LoadOperatorAgentUsage(ctx, "agent-1", OperatorAgentUsageOptions{Since: &since, Until: &until})
	if err != nil {
		t.Fatalf("LoadOperatorAgentUsage: %v", err)
	}
	if result.AgentID != "agent-1" {
		t.Fatalf("agent_id = %q", result.AgentID)
	}
	if result.Window.Since == nil || !result.Window.Since.Equal(since) || result.Window.Until == nil || !result.Window.Until.Equal(until) {
		t.Fatalf("window = %#v", result.Window)
	}
	if result.Usage.Exact.LedgerEntries != 1 || result.Usage.Exact.InputTokens != 100 || result.Usage.Exact.OutputTokens != 25 {
		t.Fatalf("exact usage = %#v", result.Usage.Exact)
	}
	if result.Usage.Estimated.LedgerEntries != 1 || result.Usage.Estimated.InputTokens != 50 || result.Usage.Estimated.OutputTokens != 10 {
		t.Fatalf("estimated usage = %#v", result.Usage.Estimated)
	}
	if len(result.Breakdown) != 2 {
		t.Fatalf("breakdown = %#v, want two rows", result.Breakdown)
	}
	if got := result.Breakdown[0]; got.UsageAccounting != AgentUsageAccountingExact || got.InvocationType != "anthropic" || got.Model != "claude-3-5-sonnet" || got.ModelAlias != "regular" || got.BackendProfile != "anthropic" || got.Provider != "anthropic" || got.Transport != "api" || got.ResolvedModel != "claude-3-5-sonnet" {
		t.Fatalf("first breakdown = %#v", got)
	}
	if got := result.Breakdown[1]; got.UsageAccounting != AgentUsageAccountingEstimated || got.InvocationType != "claude_cli" || got.Model != "sonnet" || got.ModelAlias != "regular" || got.BackendProfile != "claude_cli" || got.Provider != "claude" || got.Transport != "cli" || got.ResolvedModel != "sonnet" {
		t.Fatalf("second breakdown = %#v", got)
	}
}

func TestSQLiteRuntimeStoreLoadAgentUsageEmptyAndAgentExistence(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-empty", "active")
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-terminated", "terminated")
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-ephemeral", "ephemeral")

	result, err := sqliteStore.LoadOperatorAgentUsage(ctx, "agent-empty", OperatorAgentUsageOptions{})
	if err != nil {
		t.Fatalf("LoadOperatorAgentUsage empty: %v", err)
	}
	if result.AgentID != "agent-empty" || result.Breakdown == nil || len(result.Breakdown) != 0 {
		t.Fatalf("empty result = %#v", result)
	}
	if result.Usage.Exact.LedgerEntries != 0 || result.Usage.Estimated.LedgerEntries != 0 {
		t.Fatalf("empty usage totals = %#v", result.Usage)
	}
	for _, agentID := range []string{"missing", "agent-terminated", "agent-ephemeral"} {
		_, err := sqliteStore.LoadOperatorAgentUsage(ctx, agentID, OperatorAgentUsageOptions{})
		if !errors.Is(err, ErrAgentNotFound) {
			t.Fatalf("LoadOperatorAgentUsage(%s) error = %v, want ErrAgentNotFound", agentID, err)
		}
	}
}

func TestSQLiteRuntimeStoreLoadAgentUsageFailsClosedOnMalformedRows(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-1", "active")
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO spend_ledger (
			flow_instance, agent_id, model, invocation_type, usage_accounting, execution_mode, created_at
		) VALUES (
			'flow/a', 'agent-1', '', 'anthropic', 'exact', 'live', ?
		)
	`, time.Date(2026, 5, 21, 9, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed malformed spend row: %v", err)
	}
	_, err := sqliteStore.LoadOperatorAgentUsage(ctx, "agent-1", OperatorAgentUsageOptions{})
	if err == nil || !strings.Contains(err.Error(), "empty model") {
		t.Fatalf("LoadOperatorAgentUsage malformed error = %v, want empty model", err)
	}
}

func seedOperatorAgentUsageAgent(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, agentID string, status string) {
	t.Helper()
	if err := store.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            agentID,
			Role:          "researcher",
			Type:          "managed",
			Model:         "cheap",
			ExecutionMode: "live",
			Memory:        agentmemory.PlatformDefault(),
			Config:        json.RawMessage(`{"system_prompt":"usage"}`),
		},
		Status:    status,
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent %s: %v", agentID, err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDeliveryDiagnosticsDoesNotRequireConversationOwners(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-1",
			Role:          "researcher",
			Type:          "managed",
			Model:         "cheap",
			ExecutionMode: "live",
			Memory:        agentmemory.PlatformDefault(),
			Config:        json.RawMessage(`{"system_prompt":"diagnose"}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps: StoreSchemaCapabilities{
			Agents: SchemaFlavorCanonical,
			Events: EventSchemaCapabilities{
				Log:        SchemaFlavorCanonical,
				LogRunID:   true,
				Deliveries: SchemaFlavorCanonical,
			},
		},
	}, 0)

	result, err := reader.LoadOperatorAgentDeliveryDiagnostics(ctx, "agent-1", OperatorAgentDeliveryDiagnosticsOptions{})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDeliveryDiagnostics: %v", err)
	}
	if result.AgentID != "agent-1" {
		t.Fatalf("agent_id = %q", result.AgentID)
	}
	if result.Summary.Failures24h != 0 || result.Summary.DeadLetters24h != 0 {
		t.Fatalf("summary = %#v, want zero counts", result.Summary)
	}
	if len(result.Failures) != 0 || len(result.DeadLetters) != 0 {
		t.Fatalf("diagnostics = failures %#v dead_letters %#v, want empty", result.Failures, result.DeadLetters)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDeliveryLifecyclePostgres(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	for _, agent := range []struct {
		id   string
		role string
	}{
		{"agent-1", "researcher"},
		{"agent-2", "reviewer"},
	} {
		if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
			Config: runtimeactors.AgentConfig{
				ID:            agent.id,
				Role:          agent.role,
				Type:          "managed",
				Model:         "cheap",
				ExecutionMode: "live",
				Memory:        agentmemory.PlatformDefault(),
				Config:        json.RawMessage(`{"system_prompt":"lifecycle"}`),
			},
			Status:    "active",
			StartedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("UpsertAgent %s: %v", agent.id, err)
		}
	}

	base := time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	otherRunID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running'), ($2::uuid, 'running')`, runID, otherRunID); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	pendingEventID := uuid.NewString()
	inProgressEventID := uuid.NewString()
	deliveredEventID := uuid.NewString()
	failedEventID := uuid.NewString()
	deadLetterEventID := uuid.NewString()
	failedOtherRunEventID := uuid.NewString()
	otherAgentEventID := uuid.NewString()
	for _, event := range []struct {
		id    string
		runID string
		name  string
	}{
		{pendingEventID, runID, "task.pending"},
		{inProgressEventID, runID, "task.in_progress"},
		{deliveredEventID, runID, "task.delivered"},
		{failedEventID, runID, "task.failed"},
		{deadLetterEventID, runID, "task.dead_letter"},
		{failedOtherRunEventID, otherRunID, "task.failed"},
		{otherAgentEventID, runID, "task.other_agent"},
	} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (execution_mode,
				event_id, run_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
			) VALUES ('live',
				$1::uuid, $2::uuid, $3, $4::uuid, 'global', '{}'::jsonb, 'runtime', 'agent', $5
			)
		`, event.id, event.runID, event.name, entityID, base.Add(-10*time.Minute)); err != nil {
			t.Fatalf("seed event %s: %v", event.name, err)
		}
	}
	pendingDeliveryID := uuid.NewString()
	inProgressDeliveryID := uuid.NewString()
	deliveredDeliveryID := uuid.NewString()
	failedDeliveryID := uuid.NewString()
	deadLetterDeliveryID := uuid.NewString()
	failedOtherRunDeliveryID := uuid.NewString()
	otherAgentDeliveryID := uuid.NewString()
	temporaryFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "temporary", nil))
	boomFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "boom", nil))
	terminalFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassRetryExhausted, "terminal", nil))
	otherRunFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "other_run_boom", nil))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, failure, started_at, delivered_at, created_at
		) VALUES
			($1::uuid, $2::uuid, $3::uuid, 'agent', 'agent-1', 'pending', 1, 'retry_scheduled', $4::jsonb, NULL, NULL, $5),
			($6::uuid, $2::uuid, $7::uuid, 'agent', 'agent-1', 'in_progress', 2, 'handler_started', NULL, $8, NULL, $9),
			($10::uuid, $2::uuid, $11::uuid, 'agent', 'agent-1', 'delivered', 0, NULL, NULL, $12, $13, $14),
			($15::uuid, $2::uuid, $16::uuid, 'agent', 'agent-1', 'failed', 3, 'handler_error', $17::jsonb, $18, $19, $20),
			($21::uuid, $2::uuid, $22::uuid, 'agent', 'agent-1', 'dead_letter', 4, 'retry_exhausted', $23::jsonb, $24, $25, $26),
			($27::uuid, $28::uuid, $29::uuid, 'agent', 'agent-1', 'failed', 2, 'handler_error', $30::jsonb, $31, $32, $33),
			($34::uuid, $2::uuid, $35::uuid, 'agent', 'agent-2', 'delivered', 0, NULL, NULL, $12, $13, $14)
	`, pendingDeliveryID, runID, pendingEventID, temporaryFailure, base.Add(-4*time.Minute),
		inProgressDeliveryID, inProgressEventID, base.Add(-3*time.Minute-50*time.Second), base.Add(-3*time.Minute),
		deliveredDeliveryID, deliveredEventID, base.Add(-2*time.Minute-50*time.Second), base.Add(-2*time.Minute-40*time.Second), base.Add(-2*time.Minute),
		failedDeliveryID, failedEventID, boomFailure, base.Add(-1*time.Minute-50*time.Second), base.Add(-1*time.Minute-40*time.Second), base.Add(-1*time.Minute),
		deadLetterDeliveryID, deadLetterEventID, terminalFailure, base.Add(-50*time.Second), base.Add(-40*time.Second), base,
		failedOtherRunDeliveryID, otherRunID, failedOtherRunEventID, otherRunFailure, base.Add(-5*time.Minute-50*time.Second), base.Add(-5*time.Minute-40*time.Second), base.Add(-5*time.Minute),
		otherAgentDeliveryID, otherAgentEventID); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}

	first, err := pg.LoadOperatorAgentDeliveryLifecycle(ctx, "agent-1", OperatorAgentDeliveryLifecycleOptions{
		RunID:    runID,
		Statuses: []string{"pending", "in_progress", "delivered", "failed", "dead_letter"},
		Limit:    3,
	})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDeliveryLifecycle first page: %v", err)
	}
	if first.AgentID != "agent-1" || len(first.Deliveries) != 3 {
		t.Fatalf("first page = %#v, want three rows", first)
	}
	if first.NextCursor == "" {
		t.Fatal("next_cursor empty, want second page")
	}

	second, err := pg.LoadOperatorAgentDeliveryLifecycle(ctx, "agent-1", OperatorAgentDeliveryLifecycleOptions{
		RunID:    runID,
		Statuses: []string{"pending", "in_progress", "delivered", "failed", "dead_letter"},
		Limit:    3,
		Cursor:   first.NextCursor,
	})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDeliveryLifecycle second page: %v", err)
	}
	if len(second.Deliveries) != 2 {
		t.Fatalf("second page = %#v, want two rows", second)
	}
	if second.NextCursor != "" {
		t.Fatalf("second next_cursor = %q, want empty", second.NextCursor)
	}
	assertAgentDeliveryLifecycleRows(t, append(first.Deliveries, second.Deliveries...), []expectedAgentDeliveryLifecycleRow{
		{
			deliveryID:  deadLetterDeliveryID,
			eventID:     deadLetterEventID,
			eventName:   "task.dead_letter",
			runID:       runID,
			entityID:    entityID,
			status:      "dead_letter",
			retryCount:  4,
			reasonCode:  "retry_exhausted",
			failureCode: "terminal",
			createdAt:   base,
			wantStarted: true,
			wantDone:    true,
		},
		{
			deliveryID:  failedDeliveryID,
			eventID:     failedEventID,
			eventName:   "task.failed",
			runID:       runID,
			entityID:    entityID,
			status:      "failed",
			retryCount:  3,
			reasonCode:  "handler_error",
			failureCode: "boom",
			createdAt:   base.Add(-1 * time.Minute),
			wantStarted: true,
			wantDone:    true,
		},
		{
			deliveryID:  deliveredDeliveryID,
			eventID:     deliveredEventID,
			eventName:   "task.delivered",
			runID:       runID,
			entityID:    entityID,
			status:      "delivered",
			retryCount:  0,
			createdAt:   base.Add(-2 * time.Minute),
			wantStarted: true,
			wantDone:    true,
		},
		{
			deliveryID:  inProgressDeliveryID,
			eventID:     inProgressEventID,
			eventName:   "task.in_progress",
			runID:       runID,
			entityID:    entityID,
			status:      "in_progress",
			retryCount:  2,
			reasonCode:  "handler_started",
			createdAt:   base.Add(-3 * time.Minute),
			wantStarted: true,
		},
		{
			deliveryID:  pendingDeliveryID,
			eventID:     pendingEventID,
			eventName:   "task.pending",
			runID:       runID,
			entityID:    entityID,
			status:      "pending",
			retryCount:  1,
			reasonCode:  "retry_scheduled",
			failureCode: "temporary",
			createdAt:   base.Add(-4 * time.Minute),
		},
	})
}

func TestSQLiteRuntimeStoreLoadAgentDeliveryLifecycle(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := context.Background()
	if err := sqliteStore.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-1",
			Role:          "researcher",
			Type:          "managed",
			Model:         "cheap",
			ExecutionMode: "live",
			Memory:        agentmemory.PlatformDefault(),
			Config:        json.RawMessage(`{"system_prompt":"lifecycle"}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	if err := sqliteStore.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-2",
			Role:          "reviewer",
			Type:          "managed",
			Model:         "cheap",
			ExecutionMode: "live",
			Memory:        agentmemory.PlatformDefault(),
			Config:        json.RawMessage(`{"system_prompt":"lifecycle"}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent agent-2: %v", err)
	}

	base := time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	otherRunID := uuid.NewString()
	entityID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running'), (?, 'running')`, runID, otherRunID); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	pendingEventID := uuid.NewString()
	inProgressEventID := uuid.NewString()
	deliveredEventID := uuid.NewString()
	failedEventID := uuid.NewString()
	deadLetterEventID := uuid.NewString()
	failedOtherRunEventID := uuid.NewString()
	otherAgentEventID := uuid.NewString()
	for _, event := range []struct {
		id    string
		runID string
		name  string
	}{
		{pendingEventID, runID, "task.pending"},
		{inProgressEventID, runID, "task.in_progress"},
		{deliveredEventID, runID, "task.delivered"},
		{failedEventID, runID, "task.failed"},
		{deadLetterEventID, runID, "task.dead_letter"},
		{failedOtherRunEventID, otherRunID, "task.failed"},
		{otherAgentEventID, runID, "task.other_agent"},
	} {
		if _, err := sqliteStore.DB.ExecContext(ctx, `
			INSERT INTO events (execution_mode,
				event_id, run_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
			) VALUES ('live',
				?, ?, ?, ?, 'global', '{}', 'runtime', 'agent', ?
			)
		`, event.id, event.runID, event.name, entityID, base.Add(-10*time.Minute)); err != nil {
			t.Fatalf("seed sqlite event %s: %v", event.name, err)
		}
	}
	pendingDeliveryID := uuid.NewString()
	inProgressDeliveryID := uuid.NewString()
	deliveredDeliveryID := uuid.NewString()
	failedDeliveryID := uuid.NewString()
	deadLetterDeliveryID := uuid.NewString()
	failedOtherRunDeliveryID := uuid.NewString()
	otherAgentDeliveryID := uuid.NewString()
	temporaryFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "temporary", nil))
	boomFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "boom", nil))
	terminalFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassRetryExhausted, "terminal", nil))
	otherRunFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassConnectorFailure, "other_run_boom", nil))
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, failure, started_at, delivered_at, created_at
		) VALUES
			(?, ?, ?, 'agent', 'agent-1', 'pending', 1, 'retry_scheduled', ?, NULL, NULL, ?),
			(?, ?, ?, 'agent', 'agent-1', 'in_progress', 2, 'handler_started', NULL, ?, NULL, ?),
			(?, ?, ?, 'agent', 'agent-1', 'delivered', 0, NULL, NULL, ?, ?, ?),
			(?, ?, ?, 'agent', 'agent-1', 'failed', 3, 'handler_error', ?, ?, ?, ?),
			(?, ?, ?, 'agent', 'agent-1', 'dead_letter', 4, 'retry_exhausted', ?, ?, ?, ?),
			(?, ?, ?, 'agent', 'agent-1', 'failed', 2, 'handler_error', ?, ?, ?, ?),
			(?, ?, ?, 'agent', 'agent-2', 'delivered', 0, NULL, NULL, ?, ?, ?)
	`, pendingDeliveryID, runID, pendingEventID, temporaryFailure, base.Add(-4*time.Minute),
		inProgressDeliveryID, runID, inProgressEventID, base.Add(-3*time.Minute-50*time.Second), base.Add(-3*time.Minute),
		deliveredDeliveryID, runID, deliveredEventID, base.Add(-2*time.Minute-50*time.Second), base.Add(-2*time.Minute-40*time.Second), base.Add(-2*time.Minute),
		failedDeliveryID, runID, failedEventID, boomFailure, base.Add(-1*time.Minute-50*time.Second), base.Add(-1*time.Minute-40*time.Second), base.Add(-1*time.Minute),
		deadLetterDeliveryID, runID, deadLetterEventID, terminalFailure, base.Add(-50*time.Second), base.Add(-40*time.Second), base,
		failedOtherRunDeliveryID, otherRunID, failedOtherRunEventID, otherRunFailure, base.Add(-5*time.Minute-50*time.Second), base.Add(-5*time.Minute-40*time.Second), base.Add(-5*time.Minute),
		otherAgentDeliveryID, runID, otherAgentEventID, base.Add(-2*time.Minute-50*time.Second), base.Add(-2*time.Minute-40*time.Second), base.Add(-2*time.Minute)); err != nil {
		t.Fatalf("seed sqlite deliveries: %v", err)
	}

	first, err := sqliteStore.LoadOperatorAgentDeliveryLifecycle(ctx, "agent-1", OperatorAgentDeliveryLifecycleOptions{
		RunID:    runID,
		Statuses: []string{"pending", "in_progress", "delivered", "failed", "dead_letter"},
		Limit:    3,
	})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDeliveryLifecycle first page: %v", err)
	}
	if first.AgentID != "agent-1" || len(first.Deliveries) != 3 {
		t.Fatalf("first page = %#v, want three rows", first)
	}
	if first.NextCursor == "" {
		t.Fatal("next_cursor empty, want second page")
	}

	second, err := sqliteStore.LoadOperatorAgentDeliveryLifecycle(ctx, "agent-1", OperatorAgentDeliveryLifecycleOptions{
		RunID:    runID,
		Statuses: []string{"pending", "in_progress", "delivered", "failed", "dead_letter"},
		Limit:    3,
		Cursor:   first.NextCursor,
	})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDeliveryLifecycle second page: %v", err)
	}
	if len(second.Deliveries) != 2 {
		t.Fatalf("second page = %#v, want two rows", second)
	}
	if second.NextCursor != "" {
		t.Fatalf("second next_cursor = %q, want empty", second.NextCursor)
	}
	assertAgentDeliveryLifecycleRows(t, append(first.Deliveries, second.Deliveries...), []expectedAgentDeliveryLifecycleRow{
		{
			deliveryID:  deadLetterDeliveryID,
			eventID:     deadLetterEventID,
			eventName:   "task.dead_letter",
			runID:       runID,
			entityID:    entityID,
			status:      "dead_letter",
			retryCount:  4,
			reasonCode:  "retry_exhausted",
			failureCode: "terminal",
			createdAt:   base,
			wantStarted: true,
			wantDone:    true,
		},
		{
			deliveryID:  failedDeliveryID,
			eventID:     failedEventID,
			eventName:   "task.failed",
			runID:       runID,
			entityID:    entityID,
			status:      "failed",
			retryCount:  3,
			reasonCode:  "handler_error",
			failureCode: "boom",
			createdAt:   base.Add(-1 * time.Minute),
			wantStarted: true,
			wantDone:    true,
		},
		{
			deliveryID:  deliveredDeliveryID,
			eventID:     deliveredEventID,
			eventName:   "task.delivered",
			runID:       runID,
			entityID:    entityID,
			status:      "delivered",
			retryCount:  0,
			createdAt:   base.Add(-2 * time.Minute),
			wantStarted: true,
			wantDone:    true,
		},
		{
			deliveryID:  inProgressDeliveryID,
			eventID:     inProgressEventID,
			eventName:   "task.in_progress",
			runID:       runID,
			entityID:    entityID,
			status:      "in_progress",
			retryCount:  2,
			reasonCode:  "handler_started",
			createdAt:   base.Add(-3 * time.Minute),
			wantStarted: true,
		},
		{
			deliveryID:  pendingDeliveryID,
			eventID:     pendingEventID,
			eventName:   "task.pending",
			runID:       runID,
			entityID:    entityID,
			status:      "pending",
			retryCount:  1,
			reasonCode:  "retry_scheduled",
			failureCode: "temporary",
			createdAt:   base.Add(-4 * time.Minute),
		},
	})
}

type expectedAgentDeliveryLifecycleRow struct {
	deliveryID  string
	eventID     string
	eventName   string
	runID       string
	entityID    string
	status      string
	retryCount  int
	reasonCode  string
	failureCode string
	createdAt   time.Time
	wantStarted bool
	wantDone    bool
}

func assertAgentDeliveryLifecycleRows(t *testing.T, got []OperatorAgentDeliveryLifecycleRow, want []expectedAgentDeliveryLifecycleRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("delivery lifecycle rows = %#v, want %d rows", got, len(want))
	}
	for i, row := range got {
		expected := want[i]
		if row.DeliveryID != expected.deliveryID ||
			row.EventID != expected.eventID ||
			row.EventName != expected.eventName ||
			row.RunID != expected.runID ||
			row.EntityID != expected.entityID ||
			row.Status != expected.status ||
			row.RetryCount != expected.retryCount ||
			row.ReasonCode != expected.reasonCode ||
			failureDetailCode(row.Failure) != expected.failureCode ||
			!row.DeliveryCreatedAt.Equal(expected.createdAt) {
			t.Fatalf("delivery lifecycle row[%d] = %#v, want %#v", i, row, expected)
		}
		if expected.wantStarted && row.DeliveryStartedAt == nil {
			t.Fatalf("delivery lifecycle row[%d] missing started timestamp: %#v", i, row)
		}
		if !expected.wantStarted && row.DeliveryStartedAt != nil {
			t.Fatalf("delivery lifecycle row[%d] started timestamp = %s, want nil", i, row.DeliveryStartedAt.Format(time.RFC3339Nano))
		}
		if expected.wantDone && row.DeliveryDeliveredAt == nil {
			t.Fatalf("delivery lifecycle row[%d] missing delivered timestamp: %#v", i, row)
		}
		if !expected.wantDone && row.DeliveryDeliveredAt != nil {
			t.Fatalf("delivery lifecycle row[%d] delivered timestamp = %s, want nil", i, row.DeliveryDeliveredAt.Format(time.RFC3339Nano))
		}
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDeliveryDiagnosticsFailsClosedOnDeadLetterMismatch(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-1",
			Role:          "researcher",
			Type:          "managed",
			Model:         "cheap",
			ExecutionMode: "live",
			Memory:        agentmemory.PlatformDefault(),
			Config:        json.RawMessage(`{"system_prompt":"diagnose"}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	runID := uuid.NewString()
	eventID := uuid.NewString()
	deliveryID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode, event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ('live', $1::uuid, $2::uuid, 'task.dead', 'global', '{}'::jsonb, 'runtime', 'agent', now())
	`, eventID, runID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, retry_count, delivered_at, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'agent', 'agent-1', 'dead_letter', 1, now(), now()
		)
	`, deliveryID, runID, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	_, err = pg.LoadOperatorAgentDeliveryDiagnostics(ctx, "agent-1", OperatorAgentDeliveryDiagnosticsOptions{})
	if err == nil {
		t.Fatal("LoadOperatorAgentDeliveryDiagnostics returned success for dead_letter delivery without record")
	}
	if !strings.Contains(err.Error(), "without a dead_letters record") {
		t.Fatalf("error = %v, want dead_letters reconciliation failure", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisOmitsAbsentLifecycle(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "", nil, 0, "", nil, []byte(`{}`), 0, 0))

	diagnosis, err := reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDiagnosis: %v", err)
	}
	if diagnosis.Queue.PendingCount != 0 || diagnosis.Queue.OldestPendingAgeSeconds != 0 {
		t.Fatalf("queue = %#v, want zero facts", diagnosis.Queue)
	}
	if diagnosis.Queue.PendingDeliveries == nil || len(diagnosis.Queue.PendingDeliveries) != 0 {
		t.Fatalf("pending_deliveries = %#v, want empty array", diagnosis.Queue.PendingDeliveries)
	}
	if diagnosis.DeliveryLifecycle != nil {
		t.Fatalf("delivery_lifecycle = %#v, want nil", diagnosis.DeliveryLifecycle)
	}
	if diagnosis.RuntimeState != nil {
		t.Fatalf("runtime_state = %#v, want nil without active watchdog evidence", diagnosis.RuntimeState)
	}
	if diagnosis.Active != nil {
		t.Fatalf("active = %#v, want nil without selected active-session latest turn", diagnosis.Active)
	}
	if diagnosis.LastToolOutcome != nil {
		t.Fatalf("last_tool_outcome = %#v, want nil without selected active-session latest turn", diagnosis.LastToolOutcome)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisLastToolOutcomeUsesParseOK(t *testing.T) {
	turnID := "22222222-2222-2222-2222-222222222222"
	diagnosis, err := loadAgentDiagnosisWithLatestTurn(t, turnID, "task-1", "", false, []byte(`[{"kind":"tool_result","tool_name":"read_file","output":{"ok":true},"data":{"tool_use_id":"toolu-1"}}]`))
	if err != nil {
		t.Fatalf("LoadOperatorAgentDiagnosis: %v", err)
	}
	if diagnosis.LastToolOutcome == nil {
		t.Fatalf("last_tool_outcome = nil")
	}
	if diagnosis.LastToolOutcome.TurnID != turnID || diagnosis.LastToolOutcome.ToolName != "read_file" || diagnosis.LastToolOutcome.OK {
		t.Fatalf("last_tool_outcome = %#v, want parse_ok=false reflected as ok=false", diagnosis.LastToolOutcome)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisOmitsLastToolOutcomeWithoutToolResult(t *testing.T) {
	turnID := "22222222-2222-2222-2222-222222222222"
	for _, tc := range []struct {
		name       string
		turnBlocks []byte
	}{
		{name: "no summary", turnBlocks: []byte(`[]`)},
		{name: "summary without tool results", turnBlocks: []byte(`[{"kind":"turn_summary","data":{"assistant_visible_output":"done"}}]`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diagnosis, err := loadAgentDiagnosisWithLatestTurn(t, turnID, "task-1", "", true, tc.turnBlocks)
			if err != nil {
				t.Fatalf("LoadOperatorAgentDiagnosis: %v", err)
			}
			if diagnosis.Active == nil || diagnosis.Active.TurnID != turnID {
				t.Fatalf("active = %#v, want selected turn", diagnosis.Active)
			}
			if diagnosis.LastToolOutcome != nil {
				t.Fatalf("last_tool_outcome = %#v, want nil", diagnosis.LastToolOutcome)
			}
		})
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisFailsClosedOnMalformedPublicToolActivity(t *testing.T) {
	turnID := "22222222-2222-2222-2222-222222222222"
	for _, tc := range []struct {
		name       string
		turnBlocks []byte
		want       string
	}{
		{
			name:       "missing tool name",
			turnBlocks: []byte(`[{"kind":"tool_result","output":{"status":"ok"},"data":{"tool_use_id":"toolu-1"}}]`),
			want:       "decode public tool result activity: tool_name is required",
		},
		{
			name:       "malformed tool link",
			turnBlocks: []byte(`[{"kind":"tool_result","tool_name":"read_file","output":"private","data":{"tool_use_id":[]}}]`),
			want:       "decode public tool result activity",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadAgentDiagnosisWithLatestTurn(t, turnID, "task-1", "", true, tc.turnBlocks)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadOperatorAgentDiagnosis err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisDoesNotInterpretRawToolOutput(t *testing.T) {
	turnID := "22222222-2222-2222-2222-222222222222"
	diagnosis, err := loadAgentDiagnosisWithLatestTurn(t, turnID, "task-1", "", true, []byte(`[{"kind":"tool_result","tool_name":"read_file","output":"private-provider-payload","data":{"tool_use_id":"toolu-1"}}]`))
	if err != nil {
		t.Fatalf("LoadOperatorAgentDiagnosis: %v", err)
	}
	if diagnosis.LastToolOutcome == nil || diagnosis.LastToolOutcome.ToolName != "read_file" || !diagnosis.LastToolOutcome.OK {
		t.Fatalf("last_tool_outcome = %#v", diagnosis.LastToolOutcome)
	}
	raw, err := json.Marshal(diagnosis.LastToolOutcome)
	if err != nil {
		t.Fatalf("marshal last_tool_outcome: %v", err)
	}
	if strings.Contains(string(raw), "private-provider-payload") || strings.Contains(string(raw), "result") {
		t.Fatalf("last_tool_outcome leaked raw tool output: %s", raw)
	}
}

func TestOperatorAgentDiagnosisValidationFailsClosedOnLastToolOutcomeWithoutActive(t *testing.T) {
	err := validateOperatorAgentDiagnosis(OperatorAgentDiagnosis{
		AgentID: "agent-1",
		Status:  "running",
		Queue: OperatorAgentDiagnosisQueue{
			PendingDeliveries: []OperatorAgentPendingDelivery{},
		},
		LastToolOutcome: &OperatorAgentLastToolOutcome{
			TurnID:   "turn-1",
			ToolName: "read_file",
			OK:       true,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "last_tool_outcome requires active") {
		t.Fatalf("validateOperatorAgentDiagnosis err = %v, want last_tool_outcome active requirement", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisDoesNotDeriveStatusFromActiveLease(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
		lifecycle: map[string]AgentDeliveryLifecycleFacts{
			"agent-1": {},
		},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 0, "lease-owner", time.Now().Add(time.Minute), []byte(`{}`), 0, 0))

	diagnosis, err := reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDiagnosis: %v", err)
	}
	if diagnosis.Status != "idle" {
		t.Fatalf("status = %q, want idle from empty canonical lifecycle facts", diagnosis.Status)
	}
	if diagnosis.DeliveryLifecycle != nil {
		t.Fatalf("delivery_lifecycle = %#v, want nil", diagnosis.DeliveryLifecycle)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisOmitsActiveWithoutLatestTurn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 0, "", nil, []byte(`{}`), 0, 0))

	diagnosis, err := reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDiagnosis: %v", err)
	}
	if diagnosis.Active != nil {
		t.Fatalf("active = %#v, want nil for active session without latest turn", diagnosis.Active)
	}
	if diagnosis.LastToolOutcome != nil {
		t.Fatalf("last_tool_outcome = %#v, want nil for active session without latest turn", diagnosis.LastToolOutcome)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisOmitsEmptyActiveOptionalRefs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	turnID := "22222222-2222-2222-2222-222222222222"
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
		turns: map[string][]OperatorPublicConversationTurn{
			"sess-1": {{TurnID: turnID, CompletedAt: time.Date(2026, 5, 12, 9, 5, 0, 0, time.UTC), ParseOK: true, Activity: []OperatorConversationActivity{}}},
		},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 0, "", nil, []byte(`{}`), 0, 0))

	diagnosis, err := reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDiagnosis: %v", err)
	}
	if diagnosis.Active == nil || diagnosis.Active.TurnID != turnID {
		t.Fatalf("active = %#v, want turn ref", diagnosis.Active)
	}
	if diagnosis.Active.TaskID != "" || diagnosis.Active.EntityID != "" {
		t.Fatalf("active optional refs = task:%q entity:%q, want omitted/empty", diagnosis.Active.TaskID, diagnosis.Active.EntityID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisFailsClosedOnMalformedRuntimeState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 0, "", nil, []byte(`{"watchdog":{"state":"stale","blocking_layer":"session_execution","action":"turn_long_running","outcome":"observed","recorded_at":"2026-05-12T09:05:00Z"}}`), 0, 0))

	_, err = reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{})
	if err == nil || !strings.Contains(err.Error(), "decode latest agent session runtime_state") || !strings.Contains(err.Error(), "watchdog.state") {
		t.Fatalf("LoadOperatorAgentDiagnosis err = %v, want runtime_state watchdog validation failure", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisFailsClosedOnMalformedLifecycle(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
		lifecycle: map[string]AgentDeliveryLifecycleFacts{
			"agent-1": {CurrentState: "blocked", BlockingLayer: "session_execution"},
		},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "", nil, 0, "", nil, []byte(`{}`), 0, 0))

	_, err = reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{})
	if err == nil || !strings.Contains(err.Error(), "delivery_lifecycle.state") {
		t.Fatalf("LoadOperatorAgentDiagnosis err = %v, want delivery_lifecycle.state failure", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisPropagatesCapabilityFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	caps := canonicalAgentConversationReadCaps()
	caps.Events.Log = SchemaFlavorLegacy
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   caps,
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	_, err = reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{})
	if err == nil || !strings.Contains(err.Error(), "events schema is unsupported") {
		t.Fatalf("LoadOperatorAgentDiagnosis err = %v, want events capability failure", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisFailsClosedWithoutCanonicalTurns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	caps := canonicalAgentConversationReadCaps()
	caps.Conversations.Turns = SchemaFlavorUnavailable
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   caps,
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	_, err = reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{})
	if err == nil || !strings.Contains(err.Error(), "agent_turns schema is unavailable") {
		t.Fatalf("LoadOperatorAgentDiagnosis err = %v, want agent_turns capability failure", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func loadAgentDiagnosisWithLatestTurn(t *testing.T, turnID, taskID, entityID string, parseOK bool, turnBlocks []byte) (OperatorAgentDiagnosis, error) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	source := fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}

	turnCompletedAt := time.Date(2026, 5, 12, 9, 5, 0, 0, time.UTC)
	if strings.TrimSpace(turnID) != "" {
		turn, projectionErr := projectPublicConversationTurn(conversationTurnRecord{
			TurnID:        turnID,
			TaskID:        taskID,
			EntityID:      entityID,
			SessionID:     "11111111-1111-1111-1111-111111111111",
			ParseOK:       parseOK,
			ExecutionMode: "live",
			TurnBlocksRaw: turnBlocks,
			CreatedAt:     turnCompletedAt,
		})
		if projectionErr != nil {
			source.turnErr = projectionErr
		} else {
			source.turns = map[string][]OperatorPublicConversationTurn{
				"11111111-1111-1111-1111-111111111111": {turn},
			}
		}
	}
	reader := NewOperatorAgentConversationReadSurface(db, source, 0)
	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "11111111-1111-1111-1111-111111111111", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 1, "", nil, []byte(`{}`), 0, 0))

	diagnosis, err := reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{})
	if expectationsErr := mock.ExpectationsWereMet(); expectationsErr != nil {
		t.Fatalf("expectations: %v", expectationsErr)
	}
	return diagnosis, err
}

func boolPointer(value bool) *bool {
	return &value
}
