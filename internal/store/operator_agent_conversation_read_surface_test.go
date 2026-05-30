package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	runtimesessions "swarm/internal/runtime/sessions"
	"swarm/internal/testutil"
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
	details   map[string]PendingAgentDeliveryPage
	lifecycle map[string]AgentLifecycleFacts
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
		"turn_id", "task_id", "entity_id", "parse_ok", "error", "turn_created_at", "turn_blocks",
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

func TestCanonicalTaskConversationVisibilitySourceProjectsRunIDByCapability(t *testing.T) {
	withRunID := CanonicalTaskConversationVisibilitySourceSQL(ConversationSchemaCapabilities{
		Audits:     SchemaFlavorCanonical,
		AuditRunID: true,
	})
	if !strings.Contains(withRunID, "COALESCE(run_id::text, '') AS run_id") {
		t.Fatalf("audit run_id projection missing from canonical source:\n%s", withRunID)
	}

	withoutRunID := CanonicalTaskConversationVisibilitySourceSQL(ConversationSchemaCapabilities{
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
			Sessions: SchemaFlavorCanonical,
			Audits:   SchemaFlavorCanonical,
			Turns:    SchemaFlavorCanonical,
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

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", sessionID, sessionStartedAt, 2, "lease-owner", time.Now().Add(time.Minute), []byte(`{"provider_session_id":"provider-sess-1"}`), 0, 0, turnID, "task-1", "entity-1", false, "model error", turnCompletedAt, []byte(`[]`)))

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
		lifecycle: map[string]AgentLifecycleFacts{
			"agent-1": {},
		},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 2, "lease-owner", time.Now().Add(time.Minute), []byte(`{}`), 0, 0, "", "", "", false, "", nil, []byte(`[]`)))

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
	turnBlocks := []byte(`[{"kind":"turn_summary","data":{"assistant_visible_output":"working","tool_results":[{"tool_name":"older_tool","tool_use_id":"toolu-old","output":{"status":"old"}},{"tool_name":"selected_tool","tool_use_id":"toolu-selected","output":{"status":"selected"}}]}}]`)
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
		lifecycle: map[string]AgentLifecycleFacts{
			"agent-1": {CurrentState: "active", BlockingLayer: "session_execution"},
		},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", sessionID, sessionStartedAt, 2, "", nil, runtimeState, 0, 0, turnID, "task-1", turnEntityID, true, "", turnCompletedAt, turnBlocks))

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
	var lastToolResult map[string]any
	if err := json.Unmarshal(lastTool.Result, &lastToolResult); err != nil {
		t.Fatalf("decode last_tool_outcome.result: %v", err)
	}
	if lastToolResult["status"] != "selected" {
		t.Fatalf("last_tool_outcome.result = %#v", lastToolResult)
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
			ID:               "agent-1",
			Role:             "researcher",
			Type:             "managed",
			ModelTier:        "haiku",
			ConversationMode: runtimesessions.RuntimeModeTask.String(),
			Config:           json.RawMessage(`{"system_prompt":"diagnose"}`),
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
			INSERT INTO events (
				event_id, run_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
			) VALUES (
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
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, last_error, delivered_at, created_at
		) VALUES
			($1::uuid, $2::uuid, $3::uuid, 'agent', 'agent-1', 'failed', 2, 'handler_error', 'new failure', $4, $8),
			($5::uuid, $2::uuid, $6::uuid, 'agent', 'agent-1', 'failed', 1, 'handler_error', 'old failure', $7, $8),
			($9::uuid, $2::uuid, $10::uuid, 'agent', 'agent-1', 'dead_letter', 3, 'retry_exhausted', 'terminal failure', $11, $8),
			($12::uuid, $2::uuid, $13::uuid, 'agent', 'agent-2', 'failed', 1, 'handler_error', 'other agent', $4, $8)
	`, failedNewDeliveryID, runID, failedNewEventID, now.Add(-1*time.Minute), failedOldDeliveryID, failedOldEventID, now.Add(-2*time.Minute), now.Add(-15*time.Minute), deadDeliveryID, deadEventID, now.Add(-3*time.Minute), otherDeliveryID, otherAgentEventID); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	deadLetterID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dead_letters (
			dead_letter_id, original_event_id, original_event, original_payload, flow_instance,
			failure_type, error_message, retry_count, chain_depth, handler_node, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'task.dead', '{}'::jsonb, 'flow/test',
			'retry_exhausted', 'terminal failure', 3, 0, 'agent-1', $3
		)
	`, deadLetterID, deadEventID, now.Add(-2*time.Minute)); err != nil {
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
			ID:               "agent-1",
			Role:             "researcher",
			Type:             "managed",
			ModelTier:        "haiku",
			ConversationMode: runtimesessions.RuntimeModeTask.String(),
			Config:           json.RawMessage(`{"system_prompt":"usage"}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent agent-1: %v", err)
	}
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:               "agent-2",
			Role:             "other",
			Type:             "managed",
			ModelTier:        "haiku",
			ConversationMode: runtimesessions.RuntimeModeTask.String(),
			Config:           json.RawMessage(`{"system_prompt":"usage"}`),
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
		inputTokens     int
		outputTokens    int
		costUSD         string
		invocationType  string
		usageAccounting string
		createdAt       time.Time
	}{
		{"agent-1", "claude-3-5-sonnet", 100, 25, "0.000675", "api", AgentUsageAccountingExact, since},
		{"agent-1", "claude-cli-sonnet", 50, 10, "0.000300", "cli_test", AgentUsageAccountingEstimated, since.Add(time.Minute)},
		{"agent-1", "claude-3-5-sonnet", 7, 3, "0.000010", "api", AgentUsageAccountingExact, until},
		{"agent-2", "claude-3-5-sonnet", 999, 999, "1.000000", "api", AgentUsageAccountingExact, since.Add(time.Minute)},
	}
	for _, row := range rows {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO spend_ledger (
				flow_instance, agent_id, model, input_tokens, output_tokens,
				cost_usd, invocation_type, usage_accounting, created_at
			) VALUES (
				'flow/a', $1, $2, $3, $4, $5::numeric, $6, $7, $8
			)
		`, row.agentID, row.model, row.inputTokens, row.outputTokens, row.costUSD, row.invocationType, row.usageAccounting, row.createdAt); err != nil {
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
	if got := result.Breakdown[0]; got.UsageAccounting != AgentUsageAccountingExact || got.InvocationType != "api" || got.Model != "claude-3-5-sonnet" {
		t.Fatalf("first breakdown = %#v", got)
	}
	if got := result.Breakdown[1]; got.UsageAccounting != AgentUsageAccountingEstimated || got.InvocationType != "cli_test" || got.Model != "claude-cli-sonnet" {
		t.Fatalf("second breakdown = %#v", got)
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
			ID:               "agent-1",
			Role:             "researcher",
			Type:             "managed",
			ModelTier:        "haiku",
			ConversationMode: runtimesessions.RuntimeModeTask.String(),
			Config:           json.RawMessage(`{"system_prompt":"diagnose"}`),
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
			ID:               "agent-1",
			Role:             "researcher",
			Type:             "managed",
			ModelTier:        "haiku",
			ConversationMode: runtimesessions.RuntimeModeTask.String(),
			Config:           json.RawMessage(`{"system_prompt":"diagnose"}`),
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
		INSERT INTO events (event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2::uuid, 'task.dead', 'global', '{}'::jsonb, 'runtime', 'agent', now())
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
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "", nil, 0, "", nil, []byte(`{}`), 0, 0, "", "", "", false, "", nil, []byte(`[]`)))

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
	diagnosis, err := loadAgentDiagnosisWithLatestTurn(t, turnID, "task-1", "", false, []byte(`[{"kind":"turn_summary","data":{"tool_results":[{"tool_name":"read_file","tool_use_id":"toolu-1","output":{"ok":true}}]}}]`))
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

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisFailsClosedOnMalformedLastToolOutcome(t *testing.T) {
	turnID := "22222222-2222-2222-2222-222222222222"
	for _, tc := range []struct {
		name       string
		turnBlocks []byte
		want       string
	}{
		{
			name:       "missing tool name",
			turnBlocks: []byte(`[{"kind":"turn_summary","data":{"tool_results":[{"tool_use_id":"toolu-1","output":{"status":"ok"}}]}}]`),
			want:       "latest canonical tool_result is missing tool_name",
		},
		{
			name:       "non object result",
			turnBlocks: []byte(`[{"kind":"turn_summary","data":{"tool_results":[{"tool_name":"read_file","tool_use_id":"toolu-1","output":"ok"}]}}]`),
			want:       "last_tool_outcome.result must be a JSON object",
		},
		{
			name:       "null result",
			turnBlocks: []byte(`[{"kind":"turn_summary","data":{"tool_results":[{"tool_name":"read_file","tool_use_id":"toolu-1","output":null}]}}]`),
			want:       "last_tool_outcome.result must be a JSON object",
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
		lifecycle: map[string]AgentLifecycleFacts{
			"agent-1": {},
		},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 0, "lease-owner", time.Now().Add(time.Minute), []byte(`{}`), 0, 0, "", "", "", false, "", nil, []byte(`[]`)))

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
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 0, "", nil, []byte(`{}`), 0, 0, "", "", "", false, "", nil, []byte(`[]`)))

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
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 0, "", nil, []byte(`{}`), 0, 0, turnID, "", "", true, "", time.Date(2026, 5, 12, 9, 5, 0, 0, time.UTC), []byte(`[]`)))

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
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 0, "", nil, []byte(`{"watchdog":{"state":"stale","blocking_layer":"session_execution","action":"turn_long_running","outcome":"observed","recorded_at":"2026-05-12T09:05:00Z"}}`), 0, 0, "", "", "", false, "", nil, []byte(`[]`)))

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
		lifecycle: map[string]AgentLifecycleFacts{
			"agent-1": {CurrentState: "blocked", BlockingLayer: "session_execution"},
		},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "", nil, 0, "", nil, []byte(`{}`), 0, 0, "", "", "", false, "", nil, []byte(`[]`)))

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

	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	var turnCompletedAt any
	if strings.TrimSpace(turnID) != "" {
		turnCompletedAt = time.Date(2026, 5, 12, 9, 5, 0, 0, time.UTC)
	}
	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a.*agent_sessions.*status = 'active'.*ORDER BY updated_at DESC, created_at DESC, session_id ASC").
		WithArgs(runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity).
		WillReturnRows(sqlmock.NewRows(operatorAgentProjectionColumns()).
			AddRow("agent-1", "active", "11111111-1111-1111-1111-111111111111", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 1, "", nil, []byte(`{}`), 0, 0, turnID, taskID, entityID, parseOK, "", turnCompletedAt, turnBlocks))

	diagnosis, err := reader.LoadOperatorAgentDiagnosis(context.Background(), "agent-1", OperatorAgentDiagnosisOptions{})
	if expectationsErr := mock.ExpectationsWereMet(); expectationsErr != nil {
		t.Fatalf("expectations: %v", expectationsErr)
	}
	return diagnosis, err
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
			AddRow("agent-1", "active", sessionID, now.Add(-time.Hour), 2, "", nil, []byte(`{}`), 0, 0, "", "", "", false, "", nil, []byte(`[]`)))
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
			AddRow("agent-1", "active", "", nil, 0, "", nil, []byte(`{}`), 0, 0, "", "", "", false, "", nil, []byte(`[]`)))
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

func TestOperatorConversationReadSurfaceLoadConversationAssignsTurnIndexes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sessionID := "11111111-1111-1111-1111-111111111111"
	runID := "33333333-3333-3333-3333-333333333333"
	firstTurnID := "44444444-4444-4444-4444-444444444444"
	secondTurnID := "55555555-5555-5555-5555-555555555555"
	startedAt := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+session_id,\\s+agent_id,.*FROM \\(.*\\) conversations\\s+WHERE session_id = \\$1").
		WithArgs(sessionID).
		WillReturnRows(sqlmock.NewRows(operatorConversationDetailColumns()).
			AddRow(sessionID, "agent-1", runID, "live_session", "global", "global", "session", "active", 2, 1, []byte(`{"summary":"active session"}`), []byte(`[{"role":"assistant","content":"ready"}]`), startedAt, nil, startedAt))
	mock.ExpectQuery("(?s)SELECT\\s+turn_id::text,.*FROM agent_turns.*ORDER BY created_at ASC, turn_id ASC").
		WithArgs("agent-1", sessionID).
		WillReturnRows(sqlmock.NewRows(operatorConversationTurnColumns()).
			AddRow(firstTurnID, "agent-1", sessionID, "session", "global", "", "66666666-6666-6666-6666-666666666666", "input.received", "task-1", []byte(`["emit_done"]`), []byte(`[]`), []byte(`[]`), []byte(`{}`), []byte(`[]`), []byte(`[]`), []byte(`{"turn":1}`), []byte(`{"ok":true}`), []byte(`[]`), true, 1000, 0, "", startedAt).
			AddRow(secondTurnID, "agent-1", sessionID, "session", "global", "", "77777777-7777-7777-7777-777777777777", "input.received", "task-2", []byte(`["emit_done"]`), []byte(`[]`), []byte(`[]`), []byte(`{}`), []byte(`[]`), []byte(`[]`), []byte(`{"turn":2}`), []byte(`{"ok":true}`), []byte(`[]`), true, 1500, 0, "", startedAt.Add(time.Second)))

	detail, err := reader.LoadOperatorConversation(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("LoadOperatorConversation: %v", err)
	}
	if len(detail.Turns) != 2 {
		t.Fatalf("turn count = %d, want 2", len(detail.Turns))
	}
	if detail.Turns[0].TurnIndex != 1 || detail.Turns[0].TurnID != firstTurnID || detail.Turns[1].TurnIndex != 2 || detail.Turns[1].TurnID != secondTurnID {
		t.Fatalf("turn indexes = %#v", detail.Turns)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorConversationReadSurfaceLoadTurnComposesConversationOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sessionID := "11111111-1111-1111-1111-111111111111"
	runID := "33333333-3333-3333-3333-333333333333"
	firstTurnID := "44444444-4444-4444-4444-444444444444"
	secondTurnID := "55555555-5555-5555-5555-555555555555"
	startedAt := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	firstCompletedAt := startedAt.Add(time.Second)
	secondCompletedAt := startedAt.Add(5 * time.Second)
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+session_id,\\s+agent_id,.*FROM \\(.*\\) conversations\\s+WHERE session_id = \\$1").
		WithArgs(sessionID).
		WillReturnRows(sqlmock.NewRows(operatorConversationDetailColumns()).
			AddRow(sessionID, "agent-1", runID, "live_session", "global", "global", "session", "terminated", 2, 4, []byte(`{"summary":"active session"}`), []byte(`[{"role":"assistant","content":"ready"}]`), startedAt, secondCompletedAt, secondCompletedAt))
	mock.ExpectQuery("(?s)SELECT\\s+turn_id::text,.*FROM agent_turns.*ORDER BY created_at ASC, turn_id ASC").
		WithArgs("agent-1", sessionID).
		WillReturnRows(sqlmock.NewRows(operatorConversationTurnColumns()).
			AddRow(firstTurnID, "agent-1", sessionID, "session", "global", "", "", "", "task-1", []byte(`["emit_done"]`), []byte(`[]`), []byte(`[]`), []byte(`{}`), []byte(`[]`), []byte(`[]`), []byte(`{"first":true}`), []byte(`{"ok":true}`), []byte(`[]`), true, 1000, 0, "", firstCompletedAt).
			AddRow(secondTurnID, "agent-1", sessionID, "session", "global", "", "66666666-6666-6666-6666-666666666666", "input.received", "task-2", []byte(`["emit_done","read_state"]`), []byte(`[{"name":"read_state","arguments":{"id":"entity-1"}}]`), []byte(`["task.completed"]`), []byte(`{}`), []byte(`["read_state"]`), []byte(`["read_state"]`), []byte(`{"turn":2}`), []byte(`{"ok":true}`), []byte(`[{"kind":"assistant","text":"done"}]`), true, 1500, 1, "", secondCompletedAt))

	result, err := reader.LoadOperatorConversationTurn(context.Background(), sessionID, 2)
	if err != nil {
		t.Fatalf("LoadOperatorConversationTurn: %v", err)
	}
	if result.Session.SessionID != sessionID || result.Session.RunID != runID {
		t.Fatalf("session = %#v", result.Session)
	}
	wantStarted := secondCompletedAt.Add(-1500 * time.Millisecond)
	if result.Turn.TurnIndex != 2 || result.Turn.TurnID != secondTurnID || !result.Turn.StartedAt.Equal(wantStarted) || !result.Turn.CompletedAt.Equal(secondCompletedAt) {
		t.Fatalf("turn timing/id = %#v", result.Turn)
	}
	if !result.RuntimeLogWindowStart.Equal(wantStarted.Add(-time.Nanosecond)) || result.RuntimeLogWindowEnd == nil || !result.RuntimeLogWindowEnd.Equal(secondCompletedAt) {
		t.Fatalf("runtime log window start=%s end=%v", result.RuntimeLogWindowStart, result.RuntimeLogWindowEnd)
	}
	if result.Turn.DispatchMetadata.TaskID != "task-2" || result.Turn.DispatchMetadata.RunID != runID || result.Turn.DispatchMetadata.TriggerEventType != "input.received" {
		t.Fatalf("dispatch metadata = %#v", result.Turn.DispatchMetadata)
	}
	if len(result.Turn.AdvertisedTools) != 2 || result.Turn.AdvertisedTools[1] != "read_state" {
		t.Fatalf("advertised tools = %#v", result.Turn.AdvertisedTools)
	}
	if len(result.TurnBlocksRaw) != 1 || result.TurnBlocksRaw[0].Text != "done" {
		t.Fatalf("turn_blocks_raw = %#v", result.TurnBlocksRaw)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorConversationReadSurfaceLoadTurnOutOfRange(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sessionID := "11111111-1111-1111-1111-111111111111"
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
	}, 0)

	mock.ExpectQuery("(?s)SELECT\\s+session_id,\\s+agent_id,.*FROM \\(.*\\) conversations\\s+WHERE session_id = \\$1").
		WithArgs(sessionID).
		WillReturnRows(sqlmock.NewRows(operatorConversationDetailColumns()).
			AddRow(sessionID, "agent-1", "", "live_session", "global", "global", "session", "active", 0, 0, []byte(`{}`), []byte(`[]`), now, nil, now))
	mock.ExpectQuery("(?s)SELECT\\s+turn_id::text,.*FROM agent_turns.*ORDER BY created_at ASC, turn_id ASC").
		WithArgs("agent-1", sessionID).
		WillReturnRows(sqlmock.NewRows(operatorConversationTurnColumns()))

	_, err = reader.LoadOperatorConversationTurn(context.Background(), sessionID, 1)
	if !errors.Is(err, ErrTurnNotFound) {
		t.Fatalf("LoadOperatorConversationTurn missing turn err = %v, want ErrTurnNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
