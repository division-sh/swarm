package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storeoperatorsurface "github.com/division-sh/swarm/internal/store/internal/operatorsurface"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type fakeConversationCapabilitySource struct {
	turns   map[string][]operatorread.OperatorPublicConversationTurn
	turnErr error
	err     error
}

func (s fakeConversationCapabilitySource) RequireCurrentSchema() error {
	return s.err
}

func (s fakeConversationCapabilitySource) ListOperatorConversationTurns(_ context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error) {
	if s.turnErr != nil {
		return operatorread.OperatorConversationTurnListResult{}, s.turnErr
	}
	publicTurns := s.turns[strings.TrimSpace(opts.SessionID)]
	page := operatorread.OperatorConversationTurnListResult{Turns: []operatorread.OperatorConversationTurnListItem{}}
	for _, turn := range publicTurns {
		page.Turns = append(page.Turns, operatorConversationTurnListItemFromPublic(turn))
	}
	return page, nil
}

func (s fakeConversationCapabilitySource) LoadOperatorPublicConversationTurn(_ context.Context, sessionID, turnID string) (operatorread.OperatorPublicConversationTurnDetail, error) {
	for _, turn := range s.turns[strings.TrimSpace(sessionID)] {
		if turn.TurnID == strings.TrimSpace(turnID) {
			return operatorread.OperatorPublicConversationTurnDetail{Turn: turn}, nil
		}
	}
	return operatorread.OperatorPublicConversationTurnDetail{}, operatorread.ErrTurnNotFound
}

func newTestOperatorConversationReadOwner(t *testing.T, db *sql.DB, source fakeConversationCapabilitySource) *storeoperatorsurface.ConversationPostgres {
	t.Helper()
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatalf("postgres backend: %v", err)
	}
	owner, err := storeoperatorsurface.NewConversationPostgres(backend, source.RequireCurrentSchema)
	if err != nil {
		t.Fatalf("conversation owner: %v", err)
	}
	return owner
}

func TestCanonicalStatelessConversationVisibilitySourceProjectsRunID(t *testing.T) {
	source := storeoperatorsurface.CanonicalStatelessConversationVisibilitySourceSQL()
	if !strings.Contains(source, "COALESCE(run_id::text, '') AS run_id") {
		t.Fatalf("audit run_id projection missing from canonical source:\n%s", source)
	}
}

func testOperatorAgentConfig(agentID, role string) runtimeactors.AgentConfig {
	return runtimePersistenceTestAgentConfig(runtimeactors.AgentConfig{
		Identity:      testOperatorAgentIdentity(agentID),
		ID:            agentID,
		Role:          role,
		Type:          "managed",
		Model:         "cheap",
		ExecutionMode: "live",
		Memory:        agentmemory.PlatformDefault(),
		FlowPath:      "global",
		Config:        json.RawMessage(`{}`),
	})
}

func testOperatorAgentIdentity(agentID string) agentidentity.Identity {
	return mustTestAgentIdentity(agentID, "global")
}

func TestOperatorConversationReadSurfaceListUsesCanonicalProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	runID := "11111111-1111-1111-1111-111111111111"
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	reader := newTestOperatorConversationReadOwner(t, db, fakeConversationCapabilitySource{
		turns: map[string][]operatorread.OperatorPublicConversationTurn{
			"sess-1": {{TurnID: "turn-1", TaskID: "task-1", ParseOK: true, Activity: []operatorread.OperatorConversationActivity{}}},
		},
	})

	mock.ExpectQuery("SELECT\\s+conversations\\.session_id,\\s+conversations\\.agent_id,\\s+conversations\\.run_id,.*FROM \\(").
		WithArgs("agent-1", runID, 3).
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "run_id", "kind", "flow_instance", "memory_enabled", "memory_source", "status", "turn_count", "message_count", "runtime_state", "started_at", "ended_at", "updated_at",
		}).AddRow("sess-1", "agent-1", runID, "live_session", "global", true, "authored", "active", 2, 4, []byte(`{"summary":"brief"}`), now, nil, now))
	mock.ExpectQuery("(?s)WITH ordered AS.*FROM agent_turns.*ORDER BY created_at DESC, turn_id DESC").
		WithArgs("sess-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"ordinal", "turn_id", "run_id", "agent_id", "session_id", "entity_id", "trigger_event_id", "trigger_event_type", "task_id", "turn_blocks", "parse_ok", "latency_ms", "retry_count", "usage_exactness", "execution_mode", "input_tokens", "output_tokens", "failure", "created_at",
		}))

	result, err := reader.ListOperatorConversations(testAuthorActivityContext(), operatorread.OperatorConversationListOptions{
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
	if row.Metadata.LiveTurn != nil {
		t.Fatalf("latest public turn = %#v, want nil for empty turn projection", row.Metadata.LiveTurn)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDeliveryDiagnosticsPromotesCanonicalOwner(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := newTestPostgresStore(t, db)

	ctx := testAuthorActivityContext()
	if err := agentfixture.UpsertStatic(t, ctx, pg, runtimemanager.PersistedAgent{
		Config:    testOperatorAgentConfig("agent-1", "researcher"),
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	now := time.Now().UTC()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	failedNewEventID := uuid.NewString()
	failedOldEventID := uuid.NewString()
	deadEventID := uuid.NewString()
	otherAgentEventID := uuid.NewString()
	eventsByID := make(map[string]events.Event, 4)
	for _, event := range []struct {
		id   string
		name string
	}{
		{failedNewEventID, "task.failed.new"},
		{failedOldEventID, "task.failed.old"},
		{deadEventID, "task.dead"},
		{otherAgentEventID, "task.other"},
	} {
		eventsByID[event.id] = seedOperatorAgentEvent(t, ctx, pg, event.id, runID, event.name, entityID, now.Add(-10*time.Minute))
	}
	agentOneRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-1"), AgentIdentity: testOperatorAgentIdentity("agent-1")}
	agentTwoRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-2"), AgentIdentity: testOperatorAgentIdentity("agent-2")}
	oldFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "old_failure", nil)
	oldSnapshot := seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[failedOldEventID], agentOneRoute, runtimedelivery.StateRetrying, &oldFailure)
	terminalFailureEnvelope := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "terminal_failure", nil)
	deadSnapshot := seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[deadEventID], agentOneRoute, runtimedelivery.StateExhausted, &terminalFailureEnvelope)
	otherAgentFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "other_agent_failure", nil)
	seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[otherAgentEventID], agentTwoRoute, runtimedelivery.StateRetrying, &otherAgentFailure)
	newFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "new_failure", nil)
	newSnapshot := seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[failedNewEventID], agentOneRoute, runtimedelivery.StateRetrying, &newFailure)
	failedNewDeliveryID := newSnapshot.DeliveryID
	failedOldDeliveryID := oldSnapshot.DeliveryID
	deadDeliveryID := deadSnapshot.DeliveryID
	var deadLetterID string
	if err := db.QueryRowContext(ctx, `
		SELECT dead_letter_id::text FROM dead_letters
		WHERE delivery_id = $1::uuid AND claim_version = $2
	`, deadDeliveryID, deadSnapshot.ClaimVersion).Scan(&deadLetterID); err != nil {
		t.Fatalf("load canonical dead letter: %v", err)
	}

	first, err := pg.LoadOperatorAgentDeliveryDiagnostics(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentDeliveryDiagnosticsOptions{
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
	if first.Failures[0].EventName != "task.failed.new" || first.Failures[0].RunID != runID || first.Failures[0].EntityID != entityID || first.Failures[0].RetryCount != 1 {
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

	second, err := pg.LoadOperatorAgentDeliveryDiagnostics(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentDeliveryDiagnosticsOptions{
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
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := newTestPostgresStore(t, db)

	ctx := testAuthorActivityContext()
	if err := agentfixture.UpsertStatic(t, ctx, pg, runtimemanager.PersistedAgent{
		Config:    testOperatorAgentConfig("agent-1", "researcher"),
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent agent-1: %v", err)
	}
	if err := agentfixture.UpsertStatic(t, ctx, pg, runtimemanager.PersistedAgent{
		Config:    testOperatorAgentConfig("agent-2", "other"),
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
		{"agent-1", "claude-3-5-sonnet", "regular", "anthropic", "anthropic", "api", "claude-3-5-sonnet", 100, 25, "0.000675", "anthropic", operatorread.AgentUsageAccountingExact, since},
		{"agent-1", "sonnet", "regular", "claude_cli", "claude", "cli", "sonnet", 50, 10, "0.000300", "claude_cli", operatorread.AgentUsageAccountingEstimated, since.Add(time.Minute)},
		{"agent-1", "claude-3-5-sonnet", "regular", "anthropic", "anthropic", "api", "claude-3-5-sonnet", 7, 3, "0.000010", "anthropic", operatorread.AgentUsageAccountingExact, until},
		{"agent-2", "claude-3-5-sonnet", "regular", "anthropic", "anthropic", "api", "claude-3-5-sonnet", 999, 999, "1.000000", "anthropic", operatorread.AgentUsageAccountingExact, since.Add(time.Minute)},
	}
	for _, row := range rows {
		fields, err := testOperatorAgentIdentity(row.agentID).StorageFields()
		if err != nil {
			t.Fatalf("seed spend identity: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO spend_ledger (
				flow_instance, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				agent_flow_scope_key, agent_flow_instance_id,
				model, model_alias, backend_profile, provider, transport, resolved_model,
				input_tokens, output_tokens, cost_usd, invocation_type, usage_accounting, execution_mode, created_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7,
				$8, $9, $10, $11, $12, $13, $14, $15, $16::numeric, $17, $18, 'live', $19
			)
		`, fields.FlowInstancePath, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
			fields.FlowScopeKey, fields.FlowInstanceID,
			row.model, row.modelAlias, row.backendProfile, row.provider, row.transport, row.resolvedModel,
			row.inputTokens, row.outputTokens, row.costUSD, row.invocationType, row.usageAccounting, row.createdAt); err != nil {
			t.Fatalf("seed spend row %+v: %v", row, err)
		}
	}

	result, err := pg.LoadOperatorAgentUsage(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentUsageOptions{Since: &since, Until: &until})
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
	if got := result.Breakdown[0]; got.UsageAccounting != operatorread.AgentUsageAccountingExact || got.InvocationType != "anthropic" || got.Model != "claude-3-5-sonnet" || got.ModelAlias != "regular" || got.BackendProfile != "anthropic" || got.Provider != "anthropic" || got.Transport != "api" || got.ResolvedModel != "claude-3-5-sonnet" {
		t.Fatalf("first breakdown = %#v", got)
	}
	if got := result.Breakdown[1]; got.UsageAccounting != operatorread.AgentUsageAccountingEstimated || got.InvocationType != "claude_cli" || got.Model != "sonnet" || got.ModelAlias != "regular" || got.BackendProfile != "claude_cli" || got.Provider != "claude" || got.Transport != "cli" || got.ResolvedModel != "sonnet" {
		t.Fatalf("second breakdown = %#v", got)
	}
}

func TestSQLiteRuntimeStoreLoadAgentUsageSplitsExactAndEstimated(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-1", "active")
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-2", "active")

	since := time.Date(2026, 5, 21, 9, 0, 0, 0, time.UTC)
	until := time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC)
	agent1 := testOperatorAgentIdentity("agent-1")
	agent2 := testOperatorAgentIdentity("agent-2")
	records := []budgetspend.SpendRecord{
		{ExecutionMode: "live", FlowInstance: "global", AgentID: "agent-1", AgentIdentity: agent1, Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 100, OutputTokens: 25, CostUSD: 0.000675, InvocationType: "anthropic", UsageAccounting: operatorread.AgentUsageAccountingExact, RecordedAt: since},
		{ExecutionMode: "live", FlowInstance: "global", AgentID: "agent-1", AgentIdentity: agent1, Model: "sonnet", ModelAlias: "regular", BackendProfile: "claude_cli", Provider: "claude", Transport: "cli", ResolvedModel: "sonnet", InputTokens: 50, OutputTokens: 10, CostUSD: 0.000300, InvocationType: "claude_cli", UsageAccounting: operatorread.AgentUsageAccountingEstimated, RecordedAt: since.Add(time.Minute)},
		{ExecutionMode: "live", FlowInstance: "global", AgentID: "agent-1", AgentIdentity: agent1, Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 7, OutputTokens: 3, CostUSD: 0.000010, InvocationType: "anthropic", UsageAccounting: operatorread.AgentUsageAccountingExact, RecordedAt: until},
		{ExecutionMode: "live", FlowInstance: "global", AgentID: "agent-2", AgentIdentity: agent2, Model: "claude-3-5-sonnet", ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api", ResolvedModel: "claude-3-5-sonnet", InputTokens: 999, OutputTokens: 999, CostUSD: 1.000000, InvocationType: "anthropic", UsageAccounting: operatorread.AgentUsageAccountingExact, RecordedAt: since.Add(time.Minute)},
	}
	for _, rec := range records {
		if err := sqliteStore.RecordSpend(ctx, rec); err != nil {
			t.Fatalf("RecordSpend(%s/%s): %v", rec.AgentID, rec.UsageAccounting, err)
		}
	}

	result, err := sqliteStore.LoadOperatorAgentUsage(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentUsageOptions{Since: &since, Until: &until})
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
	if got := result.Breakdown[0]; got.UsageAccounting != operatorread.AgentUsageAccountingExact || got.InvocationType != "anthropic" || got.Model != "claude-3-5-sonnet" || got.ModelAlias != "regular" || got.BackendProfile != "anthropic" || got.Provider != "anthropic" || got.Transport != "api" || got.ResolvedModel != "claude-3-5-sonnet" {
		t.Fatalf("first breakdown = %#v", got)
	}
	if got := result.Breakdown[1]; got.UsageAccounting != operatorread.AgentUsageAccountingEstimated || got.InvocationType != "claude_cli" || got.Model != "sonnet" || got.ModelAlias != "regular" || got.BackendProfile != "claude_cli" || got.Provider != "claude" || got.Transport != "cli" || got.ResolvedModel != "sonnet" {
		t.Fatalf("second breakdown = %#v", got)
	}
}

func TestSQLiteRuntimeStoreLoadAgentUsageEmptyAndAgentExistence(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-empty", "active")
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-terminated", "terminated")

	result, err := sqliteStore.LoadOperatorAgentUsage(ctx, testOperatorAgentIdentity("agent-empty"), operatorread.OperatorAgentUsageOptions{})
	if err != nil {
		t.Fatalf("LoadOperatorAgentUsage empty: %v", err)
	}
	if result.AgentID != "agent-empty" || result.Breakdown == nil || len(result.Breakdown) != 0 {
		t.Fatalf("empty result = %#v", result)
	}
	if result.Usage.Exact.LedgerEntries != 0 || result.Usage.Estimated.LedgerEntries != 0 {
		t.Fatalf("empty usage totals = %#v", result.Usage)
	}
	for _, agentID := range []string{"missing", "agent-terminated"} {
		_, err := sqliteStore.LoadOperatorAgentUsage(ctx, testOperatorAgentIdentity(agentID), operatorread.OperatorAgentUsageOptions{})
		if !errors.Is(err, operatorread.ErrAgentNotFound) {
			t.Fatalf("LoadOperatorAgentUsage(%s) error = %v, want operatorread.ErrAgentNotFound", agentID, err)
		}
	}
}

func TestSQLiteRuntimeStoreLoadAgentUsageFailsClosedOnMalformedRows(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	seedOperatorAgentUsageAgent(t, ctx, sqliteStore, "agent-1", "active")
	fields, err := testOperatorAgentIdentity("agent-1").StorageFields()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqliteStore.backend.ExecContext(ctx, `
		INSERT INTO spend_ledger (
			flow_instance, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			agent_flow_scope_key, agent_flow_instance_id,
			model, invocation_type, usage_accounting, execution_mode, created_at
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, '', 'anthropic', 'exact', 'live', ?
		)
	`, fields.FlowInstancePath, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID,
		time.Date(2026, 5, 21, 9, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed malformed spend row: %v", err)
	}
	_, err = sqliteStore.LoadOperatorAgentUsage(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentUsageOptions{})
	if err == nil || !strings.Contains(err.Error(), "empty model") {
		t.Fatalf("LoadOperatorAgentUsage malformed error = %v, want empty model", err)
	}
}

func seedOperatorAgentUsageAgent(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, agentID string, status string) {
	t.Helper()
	if err := agentfixture.UpsertStatic(t, ctx, store, runtimemanager.PersistedAgent{
		Config:    testOperatorAgentConfig(agentID, "researcher"),
		Status:    status,
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent %s: %v", agentID, err)
	}
}

func TestOperatorAgentReadSurfaceLoadAgentDeliveryDiagnosticsDoesNotRequireConversationOwners(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := newTestPostgresStore(t, db)

	ctx := testAuthorActivityContext()
	if err := agentfixture.UpsertStatic(t, ctx, pg, runtimemanager.PersistedAgent{
		Config:    testOperatorAgentConfig("agent-1", "researcher"),
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	result, err := pg.LoadOperatorAgentDeliveryDiagnostics(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentDeliveryDiagnosticsOptions{})
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
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := newTestPostgresStore(t, db)

	ctx := testAuthorActivityContext()
	for _, agent := range []struct {
		id   string
		role string
	}{
		{"agent-1", "researcher"},
		{"agent-2", "reviewer"},
	} {
		if err := agentfixture.UpsertStatic(t, ctx, pg, runtimemanager.PersistedAgent{
			Config:    testOperatorAgentConfig(agent.id, agent.role),
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
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: base})
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: otherRunID, StartedAt: base})
	pendingEventID := uuid.NewString()
	inProgressEventID := uuid.NewString()
	deliveredEventID := uuid.NewString()
	failedEventID := uuid.NewString()
	deadLetterEventID := uuid.NewString()
	failedOtherRunEventID := uuid.NewString()
	otherAgentEventID := uuid.NewString()
	eventsByID := make(map[string]events.Event, 7)
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
		eventsByID[event.id] = seedOperatorAgentEvent(t, ctx, pg, event.id, event.runID, event.name, entityID, base.Add(-10*time.Minute))
	}
	agentOneRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-1"), AgentIdentity: testOperatorAgentIdentity("agent-1")}
	agentTwoRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-2"), AgentIdentity: testOperatorAgentIdentity("agent-2")}
	pendingSnapshot := seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[pendingEventID], agentOneRoute, runtimedelivery.StateQueued, nil)
	inProgressSnapshot := seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[inProgressEventID], agentOneRoute, runtimedelivery.StateLaunching, nil)
	deliveredSnapshot := seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[deliveredEventID], agentOneRoute, runtimedelivery.StateDelivered, nil)
	boomFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "boom", nil)
	failedSnapshot := seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[failedEventID], agentOneRoute, runtimedelivery.StateRetrying, &boomFailure)
	terminalFailure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "terminal", nil)
	deadLetterSnapshot := seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[deadLetterEventID], agentOneRoute, runtimedelivery.StateExhausted, &terminalFailure)
	otherRunFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "other_run_boom", nil)
	seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[failedOtherRunEventID], agentOneRoute, runtimedelivery.StateRetrying, &otherRunFailure)
	seedAgentDeliveryStateFixture(t, ctx, pg, eventsByID[otherAgentEventID], agentTwoRoute, runtimedelivery.StateDelivered, nil)
	pendingDeliveryID := pendingSnapshot.DeliveryID
	inProgressDeliveryID := inProgressSnapshot.DeliveryID
	deliveredDeliveryID := deliveredSnapshot.DeliveryID
	failedDeliveryID := failedSnapshot.DeliveryID
	deadLetterDeliveryID := deadLetterSnapshot.DeliveryID

	first, err := pg.LoadOperatorAgentDeliveryLifecycle(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentDeliveryLifecycleOptions{
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

	second, err := pg.LoadOperatorAgentDeliveryLifecycle(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentDeliveryLifecycleOptions{
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
			retryCount:  0,
			reasonCode:  "terminal",
			failureCode: "terminal",
			createdAt:   deadLetterSnapshot.CreatedAt,
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
			retryCount:  1,
			failureCode: "boom",
			createdAt:   failedSnapshot.CreatedAt,
			wantStarted: true,
		},
		{
			deliveryID:  deliveredDeliveryID,
			eventID:     deliveredEventID,
			eventName:   "task.delivered",
			runID:       runID,
			entityID:    entityID,
			status:      "delivered",
			retryCount:  0,
			createdAt:   deliveredSnapshot.CreatedAt,
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
			retryCount:  0,
			createdAt:   inProgressSnapshot.CreatedAt,
			wantStarted: true,
		},
		{
			deliveryID: pendingDeliveryID,
			eventID:    pendingEventID,
			eventName:  "task.pending",
			runID:      runID,
			entityID:   entityID,
			status:     "pending",
			retryCount: 0,
			createdAt:  pendingSnapshot.CreatedAt,
		},
	})
}

func TestSQLiteRuntimeStoreLoadAgentDeliveryLifecycle(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := testAuthorActivityContext()
	if err := agentfixture.UpsertStatic(t, ctx, sqliteStore, runtimemanager.PersistedAgent{
		Config:    testOperatorAgentConfig("agent-1", "researcher"),
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	if err := agentfixture.UpsertStatic(t, ctx, sqliteStore, runtimemanager.PersistedAgent{
		Config:    testOperatorAgentConfig("agent-2", "reviewer"),
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent agent-2: %v", err)
	}

	base := time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	otherRunID := uuid.NewString()
	entityID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, sqliteStore, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: base})
	requireRunFixtureForTest(t, ctx, sqliteStore, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: otherRunID, StartedAt: base})
	pendingEventID := uuid.NewString()
	inProgressEventID := uuid.NewString()
	deliveredEventID := uuid.NewString()
	failedEventID := uuid.NewString()
	deadLetterEventID := uuid.NewString()
	failedOtherRunEventID := uuid.NewString()
	otherAgentEventID := uuid.NewString()
	eventsByID := make(map[string]events.Event, 7)
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
		fixture := eventtest.PersistedProjection(
			event.id, events.EventType(event.name), "runtime", "", json.RawMessage(`{}`), 0,
			event.runID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), base.Add(-10*time.Minute),
		)
		if err := commitSemanticEventFixture(ctx, sqliteStore, fixture); err != nil {
			t.Fatalf("seed sqlite event %s: %v", event.name, err)
		}
		eventsByID[event.id] = fixture
	}
	agentOneRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-1"), AgentIdentity: testOperatorAgentIdentity("agent-1")}
	agentTwoRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-2"), AgentIdentity: testOperatorAgentIdentity("agent-2")}
	pendingSnapshot := seedAgentDeliveryStateFixture(t, ctx, sqliteStore, eventsByID[pendingEventID], agentOneRoute, runtimedelivery.StateQueued, nil)
	inProgressSnapshot := seedAgentDeliveryStateFixture(t, ctx, sqliteStore, eventsByID[inProgressEventID], agentOneRoute, runtimedelivery.StateLaunching, nil)
	deliveredSnapshot := seedAgentDeliveryStateFixture(t, ctx, sqliteStore, eventsByID[deliveredEventID], agentOneRoute, runtimedelivery.StateDelivered, nil)
	boomFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "boom", nil)
	failedSnapshot := seedAgentDeliveryStateFixture(t, ctx, sqliteStore, eventsByID[failedEventID], agentOneRoute, runtimedelivery.StateRetrying, &boomFailure)
	terminalFailure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "terminal", nil)
	deadLetterSnapshot := seedAgentDeliveryStateFixture(t, ctx, sqliteStore, eventsByID[deadLetterEventID], agentOneRoute, runtimedelivery.StateExhausted, &terminalFailure)
	otherRunFailure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "other_run_boom", nil)
	seedAgentDeliveryStateFixture(t, ctx, sqliteStore, eventsByID[failedOtherRunEventID], agentOneRoute, runtimedelivery.StateRetrying, &otherRunFailure)
	seedAgentDeliveryStateFixture(t, ctx, sqliteStore, eventsByID[otherAgentEventID], agentTwoRoute, runtimedelivery.StateDelivered, nil)
	pendingDeliveryID := pendingSnapshot.DeliveryID
	inProgressDeliveryID := inProgressSnapshot.DeliveryID
	deliveredDeliveryID := deliveredSnapshot.DeliveryID
	failedDeliveryID := failedSnapshot.DeliveryID
	deadLetterDeliveryID := deadLetterSnapshot.DeliveryID

	first, err := sqliteStore.LoadOperatorAgentDeliveryLifecycle(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentDeliveryLifecycleOptions{
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

	second, err := sqliteStore.LoadOperatorAgentDeliveryLifecycle(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentDeliveryLifecycleOptions{
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
			retryCount:  0,
			reasonCode:  "terminal",
			failureCode: "terminal",
			createdAt:   deadLetterSnapshot.CreatedAt,
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
			retryCount:  1,
			failureCode: "boom",
			createdAt:   failedSnapshot.CreatedAt,
			wantStarted: true,
		},
		{
			deliveryID:  deliveredDeliveryID,
			eventID:     deliveredEventID,
			eventName:   "task.delivered",
			runID:       runID,
			entityID:    entityID,
			status:      "delivered",
			retryCount:  0,
			createdAt:   deliveredSnapshot.CreatedAt,
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
			retryCount:  0,
			createdAt:   inProgressSnapshot.CreatedAt,
			wantStarted: true,
		},
		{
			deliveryID: pendingDeliveryID,
			eventID:    pendingEventID,
			eventName:  "task.pending",
			runID:      runID,
			entityID:   entityID,
			status:     "pending",
			retryCount: 0,
			createdAt:  pendingSnapshot.CreatedAt,
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

func assertAgentDeliveryLifecycleRows(t *testing.T, got []operatorread.OperatorAgentDeliveryLifecycleRow, want []expectedAgentDeliveryLifecycleRow) {
	t.Helper()
	want = append([]expectedAgentDeliveryLifecycleRow(nil), want...)
	sort.Slice(want, func(i, j int) bool {
		if !want[i].createdAt.Equal(want[j].createdAt) {
			return want[i].createdAt.After(want[j].createdAt)
		}
		return want[i].deliveryID > want[j].deliveryID
	})
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

func TestOperatorAgentReadSurfaceLoadAgentDeliveryDiagnosticsUsesCanonicalLifecycleDiagnostic(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := newTestPostgresStore(t, db)

	ctx := testAuthorActivityContext()
	if err := agentfixture.UpsertStatic(t, ctx, pg, runtimemanager.PersistedAgent{
		Config:    testOperatorAgentConfig("agent-1", "researcher"),
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	runID := uuid.NewString()
	eventID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	event := seedOperatorAgentEvent(t, ctx, pg, eventID, runID, "task.dead", "", time.Now().UTC())
	failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "missing_dead_letter_record", nil)
	seedAgentDeliveryStateFixture(t, ctx, pg, event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-1"), AgentIdentity: testOperatorAgentIdentity("agent-1")}, runtimedelivery.StateExhausted, &failure)

	got, err := pg.LoadOperatorAgentDeliveryDiagnostics(ctx, testOperatorAgentIdentity("agent-1"), operatorread.OperatorAgentDeliveryDiagnosticsOptions{})
	if err != nil {
		t.Fatalf("LoadOperatorAgentDeliveryDiagnostics: %v", err)
	}
	if got.Summary.DeadLetters24h != 1 || len(got.DeadLetters) != 1 || len(got.DeadLetters[0].DeadLetterRecords) != 1 {
		t.Fatalf("delivery diagnostics = %#v, want one canonical lifecycle outcome and diagnostic", got)
	}
}

func seedOperatorAgentEvent(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, runID, eventName, entityID string, createdAt time.Time) events.Event {
	t.Helper()
	envelope := events.EventEnvelope{}
	if entityID != "" {
		envelope = events.EnvelopeForEntityID(envelope, entityID)
	}
	parentID := eventtest.UUID("operator-agent-parent:" + eventID)
	if err := commitSemanticParentFixture(ctx, pg, runID, parentID, createdAt.Add(-time.Microsecond)); err != nil {
		t.Fatalf("seed operator-agent parent %s: %v", eventName, err)
	}
	event := eventtest.PersistedChildForProducer(
		eventID, events.EventType(eventName), eventtest.Producer(events.EventProducerAgent, "runtime"), "",
		json.RawMessage(`{}`), 0, runID, parentID, envelope, createdAt,
	)
	if err := commitSemanticEventFixture(ctx, pg, event); err != nil {
		t.Fatalf("seed operator-agent event %s: %v", eventName, err)
	}
	return event
}

func TestOperatorAgentDiagnosisValidationFailsClosedOnLastToolOutcomeWithoutActive(t *testing.T) {
	err := storeoperatorsurface.ValidateOperatorAgentDiagnosis(operatorread.OperatorAgentDiagnosis{
		AgentID: "agent-1",
		Status:  "running",
		Queue: operatorread.OperatorAgentDiagnosisQueue{
			PendingDeliveries: []operatorread.OperatorAgentPendingDelivery{},
		},
		LastToolOutcome: &operatorread.OperatorAgentLastToolOutcome{
			TurnID:   "turn-1",
			ToolName: "read_file",
			OK:       true,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "last_tool_outcome requires active") {
		t.Fatalf("storeoperatorsurface.ValidateOperatorAgentDiagnosis err = %v, want last_tool_outcome active requirement", err)
	}
}
