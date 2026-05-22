package store

import (
	"context"
	"errors"
	"strings"
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

func TestOperatorAgentReadSurfaceLoadAgentDiagnosisUsesSelectedOwners(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sessionID := "11111111-1111-1111-1111-111111111111"
	turnID := "22222222-2222-2222-2222-222222222222"
	sessionStartedAt := time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC)
	turnCompletedAt := time.Date(2026, 5, 12, 9, 5, 0, 0, time.UTC)
	eventTime := time.Date(2026, 5, 12, 8, 55, 0, 0, time.UTC)
	runtimeState := []byte(`{"provider_session_id":"provider-sess-1","watchdog":{"state":"healthy_long_running","blocking_layer":"session_execution","action":"turn_long_running","outcome":"observed","last_output_at":"2026-05-12T09:04:00Z","recorded_at":"2026-05-12T09:05:00Z"}}`)
	reader := NewOperatorAgentConversationReadSurface(db, fakeAgentConversationReadSource{
		caps:   canonicalAgentConversationReadCaps(),
		agents: []runtimemanager.PersistedAgent{testOperatorAgent("agent-1")},
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
			AddRow("agent-1", "active", sessionID, sessionStartedAt, 2, "", nil, runtimeState, 0, 0, 0, 0, 0, turnID, "task-1", true, "", turnCompletedAt, []byte(`[]`)))

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
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
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
			AddRow("agent-1", "active", "", nil, 0, "", nil, []byte(`{}`), 0, 0, 0, 0, 0, "", "", false, "", nil, []byte(`[]`)))

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
			AddRow("agent-1", "active", "sess-1", time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), 0, "", nil, []byte(`{"watchdog":{"state":"stale","blocking_layer":"session_execution","action":"turn_long_running","outcome":"observed","recorded_at":"2026-05-12T09:05:00Z"}}`), 0, 0, 0, 0, 0, "", "", false, "", nil, []byte(`[]`)))

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
			AddRow("agent-1", "active", "", nil, 0, "", nil, []byte(`{}`), 0, 0, 0, 0, 0, "", "", false, "", nil, []byte(`[]`)))

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
