package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
)

func TestSQLiteRunDebugTracePagePaginationWindowAndFilterParity(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	fixture := seedSQLiteRunTraceParityRows(t, ctx, sqliteStore)
	mainFilter := operatorread.RunDebugTraceFilter{
		EventNames: []string{"trace.event_only", "trace.late_delivered", "trace.failed", "trace.second_delivered"},
	}

	page1, next, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, operatorread.RunDebugTraceQueryOptions{Limit: 2, Filter: mainFilter})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage page1: %v", err)
	}
	if len(page1) != 2 || page1[0].EventID != fixture.eventOnlyID || page1[1].EventID != fixture.lateDeliveredID || next == "" {
		t.Fatalf("page1 rows=%#v next=%q, want event-only then late-delivered with next cursor", page1, next)
	}
	page2, next2, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, operatorread.RunDebugTraceQueryOptions{Limit: 2, Cursor: next, Filter: mainFilter})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage page2: %v", err)
	}
	if len(page2) != 2 || page2[0].EventID != fixture.failedID || page2[1].EventID != fixture.secondDeliveredID || next2 != "" {
		t.Fatalf("page2 rows=%#v next=%q, want failed then second-delivered and no next cursor", page2, next2)
	}
	if _, _, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, operatorread.RunDebugTraceQueryOptions{Limit: 2, Cursor: "not-a-cursor"}); !errors.Is(err, operatorread.ErrInvalidObservabilityCursor) {
		t.Fatalf("invalid cursor error = %v, want operatorread.ErrInvalidObservabilityCursor", err)
	}

	sinceRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, operatorread.RunDebugTraceQueryOptions{Limit: 10, Since: &fixture.base, Filter: mainFilter})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage since: %v", err)
	}
	if got := traceEventIDs(sinceRows); !sameStrings(got, []string{fixture.lateDeliveredID, fixture.failedID, fixture.secondDeliveredID}) {
		t.Fatalf("since rows = %#v, want late materialized rows only", got)
	}
	until := fixture.base.Add(3500 * time.Millisecond)
	untilRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, operatorread.RunDebugTraceQueryOptions{Limit: 10, Until: &until, Filter: mainFilter})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage until: %v", err)
	}
	if got := traceEventIDs(untilRows); !sameStrings(got, []string{fixture.eventOnlyID, fixture.failedID}) {
		t.Fatalf("until rows = %#v, want rows whose materialization watermark is <= until", got)
	}
	emptyWindowRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, operatorread.RunDebugTraceQueryOptions{Limit: 10, Since: &fixture.base, Until: &fixture.base, Filter: mainFilter})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage equal since/until: %v", err)
	}
	if len(emptyWindowRows) != 0 {
		t.Fatalf("equal since/until rows = %#v, want empty strict/inclusive window", emptyWindowRows)
	}

	deliveredPage1, deliveredNext, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, operatorread.RunDebugTraceQueryOptions{
		Limit: 1,
		Since: &fixture.base,
		Filter: operatorread.RunDebugTraceFilter{
			EventNames:       []string{"trace.late_delivered", "trace.second_delivered"},
			DeliveryStatuses: []string{"delivered"},
			SubscriberTypes:  []string{"agent"},
		},
	})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage delivered page1: %v", err)
	}
	if len(deliveredPage1) != 1 || deliveredPage1[0].EventID != fixture.lateDeliveredID || deliveredNext == "" {
		t.Fatalf("delivered page1 rows=%#v next=%q, want late-delivered with next cursor", deliveredPage1, deliveredNext)
	}
	deliveredPage2, deliveredNext2, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, operatorread.RunDebugTraceQueryOptions{
		Limit:  1,
		Cursor: deliveredNext,
		Since:  &fixture.base,
		Filter: operatorread.RunDebugTraceFilter{
			EventNames:       []string{"trace.late_delivered", "trace.second_delivered"},
			DeliveryStatuses: []string{"delivered"},
			SubscriberTypes:  []string{"agent"},
		},
	})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage delivered page2: %v", err)
	}
	if len(deliveredPage2) != 1 || deliveredPage2[0].EventID != fixture.secondDeliveredID || deliveredNext2 != "" {
		t.Fatalf("delivered page2 rows=%#v next=%q, want second-delivered and no next cursor", deliveredPage2, deliveredNext2)
	}
}

func TestSQLiteRunDebugTracePageDeterministicDeliveryAndTurnTiePaging(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	fixture := seedSQLiteRunTraceParityRows(t, ctx, sqliteStore)

	var got []string
	cursor := ""
	for {
		rows, next, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, operatorread.RunDebugTraceQueryOptions{
			Limit:  1,
			Cursor: cursor,
			Filter: operatorread.RunDebugTraceFilter{
				EventNames: []string{"trace.tie"},
			},
		})
		if err != nil {
			t.Fatalf("LoadRunDebugTracePage tie cursor=%q: %v", cursor, err)
		}
		for _, row := range rows {
			got = append(got, row.DeliveryID+"/"+row.TurnID)
		}
		if next == "" {
			break
		}
		if next == cursor {
			t.Fatalf("cursor did not advance: %q", next)
		}
		cursor = next
	}
	want := []string{
		fixture.tieDeliveryAID + "/" + fixture.tieTurnA1ID,
		fixture.tieDeliveryAID + "/" + fixture.tieTurnA2ID,
		fixture.tieDeliveryBID + "/" + fixture.tieTurnBID,
	}
	sort.Strings(want)
	if !sameStrings(got, want) {
		t.Fatalf("tie paging rows = %#v, want %#v", got, want)
	}
}

func TestSQLiteRunDebugTracePageExcludeRuntimeLogs(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)

	runID := "00000000-0000-0000-0000-000000001816"
	businessEvent := "00000000-0000-0000-0000-000000001817"
	runtimeLogEvent := "00000000-0000-0000-0000-000000001818"
	base := time.Unix(1700000600, 0).UTC()
	requireRunFixtureForTest(t, ctx, NewSQLiteRuntimeStoreForTest(sqliteStore.backend.ConstructionHandle()), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: base})
	if err := commitSemanticEventFixture(ctx, sqliteStore, eventtest.PersistedProjection(
		businessEvent, events.EventType("item.received"), "runtime", "", json.RawMessage(`{}`), 0,
		runID, "", events.EventEnvelope{}, base,
	)); err != nil {
		t.Fatalf("seed business trace row: %v", err)
	}
	if err := commitDiagnosticRuntimeLogFixture(ctx, sqliteStore, eventtest.DiagnosticDirect(
		runtimeLogEvent, events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{}`), 0,
		runID, "", events.EventEnvelope{}, base.Add(time.Millisecond),
	)); err != nil {
		t.Fatalf("seed runtime-log trace row: %v", err)
	}

	allRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage all: %v", err)
	}
	if got := traceEventIDs(allRows); !sameStrings(got, []string{businessEvent, runtimeLogEvent}) {
		t.Fatalf("all trace rows = %#v, want business and runtime_log", got)
	}
	filteredRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 10, ExcludeRuntimeLogs: true})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage filtered: %v", err)
	}
	if got := traceEventIDs(filteredRows); !sameStrings(got, []string{businessEvent}) {
		t.Fatalf("filtered trace rows = %#v, want business row only", got)
	}
}

func TestSQLiteRunDebugTracePageIncludesStatelessAuditSessionsInWatermark(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	base := time.Unix(1700003200, 0).UTC()
	runID := "00000000-0000-0000-0000-000000001430"
	eventID := "00000000-0000-0000-0000-000000001431"
	sessionID := "00000000-0000-0000-0000-000000001433"
	turnID := "00000000-0000-0000-0000-000000001434"
	agentID := "agent-task"

	requireRunFixtureForTest(t, ctx, NewSQLiteRuntimeStoreForTest(sqliteStore.backend.ConstructionHandle()), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: base.Add(-time.Minute)})
	if err := commitSemanticEventFixture(ctx, sqliteStore, eventtest.PersistedProjection(
		eventID, events.EventType("trace.task_audit"), "runtime", "", json.RawMessage(`{}`), 0,
		runID, "", events.EventEnvelope{}, base,
	)); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	event := loadSQLiteDeliveryFixtureEvent(t, ctx, sqliteStore.backend.ConstructionHandle(), eventID)
	seedSQLiteTraceAgent(t, ctx, sqliteStore, agentID, base)
	fields := testAgentIdentityStorageFields(t, testAgentIdentity(t, agentID, "flow-a"))
	if _, err := sqliteStore.backend.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			entity_id, memory_enabled, memory_source,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 'platform_default',
			'[]', 0, '{}', 'active', ?, ?
		)
	`, sessionID, runID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		base.Add(time.Second), base.Add(5*time.Second)); err != nil {
		t.Fatalf("seed task audit: %v", err)
	}
	route := testAgentDeliveryRoute(t, agentID, "flow-a")
	if err := commitDeliveryObligationFixture(ctx, sqliteStore, event, route); err != nil {
		t.Fatalf("commit task delivery: %v", err)
	}
	claimed, err := claimDeliveryFixture(ctx, sqliteStore, event, route)
	if err != nil {
		t.Fatalf("claim task delivery: %v", err)
	}
	insertSQLiteTraceTurnWithMemory(t, ctx, sqliteStore, claimed.Claim, event, turnID, runID, agentID, sessionID, false, base.Add(2*time.Second))
	delivered, err := sqliteStore.SettleSuccess(ctx, claimed.Claim, nil, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection())
	if err != nil {
		t.Fatalf("settle task delivery: %v", err)
	}
	setSQLiteDeliveryFixtureTimes(t, ctx, sqliteStore.backend.ConstructionHandle(), delivered, base.Add(time.Second), base.Add(2*time.Second))
	if _, err := sqliteStore.backend.ExecContext(ctx, `UPDATE agent_conversation_audits SET updated_at = ? WHERE session_id = ?`, base.Add(5*time.Second), sessionID); err != nil {
		t.Fatalf("restore task audit watermark: %v", err)
	}

	rows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %#v, want one task audit trace row", rows)
	}
	row := rows[0]
	if row.SessionID != sessionID || row.SessionKind != "turn_audit" || row.SessionMemory || row.SessionMemorySource != "platform_default" {
		t.Fatalf("stateless audit session fields = %#v, want platform-default turn_audit session %s", row, sessionID)
	}
	if row.SessionUpdatedAt == nil || !row.SessionUpdatedAt.Equal(base.Add(5*time.Second)) {
		t.Fatalf("task audit session updated_at = %#v, want %s", row.SessionUpdatedAt, base.Add(5*time.Second))
	}

	since := base.Add(4 * time.Second)
	sinceRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 10, Since: &since})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage since: %v", err)
	}
	if len(sinceRows) != 1 || sinceRows[0].EventID != eventID {
		t.Fatalf("since rows = %#v, want task row included by audit updated_at watermark", sinceRows)
	}
	until := base.Add(4 * time.Second)
	untilRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 10, Until: &until})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage until: %v", err)
	}
	if len(untilRows) != 0 {
		t.Fatalf("until rows = %#v, want task row excluded by audit updated_at watermark", untilRows)
	}
}

type sqliteRunTraceParityFixture struct {
	runID             string
	eventOnlyID       string
	lateDeliveredID   string
	failedID          string
	secondDeliveredID string
	tieDeliveryAID    string
	tieDeliveryBID    string
	tieTurnA1ID       string
	tieTurnA2ID       string
	tieTurnBID        string
	base              time.Time
}

func seedSQLiteRunTraceParityRows(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore) sqliteRunTraceParityFixture {
	t.Helper()
	base := time.Unix(1700003000, 0).UTC()
	fixture := sqliteRunTraceParityFixture{
		runID:             "00000000-0000-0000-0000-000000001428",
		eventOnlyID:       "00000000-0000-0000-0000-000000000001",
		lateDeliveredID:   "00000000-0000-0000-0000-000000000002",
		failedID:          "00000000-0000-0000-0000-000000000003",
		secondDeliveredID: "00000000-0000-0000-0000-000000000004",
		tieTurnA1ID:       "00000000-0000-0000-0000-000000000201",
		tieTurnA2ID:       "00000000-0000-0000-0000-000000000202",
		tieTurnBID:        "00000000-0000-0000-0000-000000000203",
		base:              base,
	}
	requireRunFixtureForTest(t, ctx, NewSQLiteRuntimeStoreForTest(sqliteStore.backend.ConstructionHandle()), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: fixture.runID, StartedAt: base.Add(-time.Minute)})
	eventRows := []struct {
		id   string
		name string
		at   time.Time
	}{
		{fixture.eventOnlyID, "trace.event_only", base},
		{fixture.lateDeliveredID, "trace.late_delivered", base},
		{fixture.failedID, "trace.failed", base.Add(time.Second)},
		{fixture.secondDeliveredID, "trace.second_delivered", base.Add(2 * time.Second)},
		{"00000000-0000-0000-0000-000000000005", "trace.tie", base.Add(10 * time.Second)},
	}
	for _, event := range eventRows {
		if err := commitSemanticEventFixture(ctx, sqliteStore, eventtest.PersistedProjection(
			event.id, events.EventType(event.name), "runtime", "", json.RawMessage(`{}`), 0,
			fixture.runID, "", events.EventEnvelope{}, event.at,
		)); err != nil {
			t.Fatalf("seed event %s: %v", event.name, err)
		}
	}
	for _, agentID := range []string{"agent-late", "agent-failed", "agent-second", "agent-a", "agent-b"} {
		seedSQLiteTraceAgent(t, ctx, sqliteStore, agentID, base)
	}
	insertSQLiteTraceSession(t, ctx, sqliteStore, "00000000-0000-0000-0000-000000000301", fixture.runID, "agent-late", base.Add(4*time.Second))
	insertSQLiteTraceSession(t, ctx, sqliteStore, "00000000-0000-0000-0000-000000000302", fixture.runID, "agent-failed", base.Add(2500*time.Millisecond))
	insertSQLiteTraceSession(t, ctx, sqliteStore, "00000000-0000-0000-0000-000000000303", fixture.runID, "agent-second", base.Add(6*time.Second))
	insertSQLiteTraceSession(t, ctx, sqliteStore, "00000000-0000-0000-0000-000000000304", fixture.runID, "agent-a", base.Add(11*time.Second))
	insertSQLiteTraceSession(t, ctx, sqliteStore, "00000000-0000-0000-0000-000000000305", fixture.runID, "agent-b", base.Add(11*time.Second))
	type traceTurn struct {
		id string
		at time.Time
	}
	turnsByDelivery := map[string][]traceTurn{
		fixture.lateDeliveredID + "\x00agent-late":     {{id: "00000000-0000-0000-0000-000000000401", at: base.Add(5 * time.Second)}},
		fixture.failedID + "\x00agent-failed":          {{id: "00000000-0000-0000-0000-000000000402", at: base.Add(3 * time.Second)}},
		fixture.secondDeliveredID + "\x00agent-second": {{id: "00000000-0000-0000-0000-000000000403", at: base.Add(6 * time.Second)}},
		"00000000-0000-0000-0000-000000000005\x00agent-a": {
			{id: fixture.tieTurnA1ID, at: base.Add(11 * time.Second)},
			{id: fixture.tieTurnA2ID, at: base.Add(11 * time.Second)},
		},
		"00000000-0000-0000-0000-000000000005\x00agent-b": {{id: fixture.tieTurnBID, at: base.Add(11 * time.Second)}},
	}

	seedDelivery := func(eventID, agentID, sessionID string, state runtimedelivery.State, createdAt, transitionAt time.Time) runtimedelivery.Snapshot {
		t.Helper()
		event := loadSQLiteDeliveryFixtureEvent(t, ctx, sqliteStore.backend.ConstructionHandle(), eventID)
		route := testAgentDeliveryRoute(t, agentID, "flow-a")
		if err := commitDeliveryObligationFixture(ctx, sqliteStore, event, route); err != nil {
			t.Fatalf("commit trace delivery %s/%s: %v", eventID, agentID, err)
		}
		claimed, err := claimDeliveryFixture(ctx, sqliteStore, event, route)
		if err != nil {
			t.Fatalf("claim trace delivery %s/%s: %v", eventID, agentID, err)
		}
		if _, err := sqliteStore.BindAgentSession(ctx, claimed.Claim, sessionID); err != nil {
			t.Fatalf("bind trace delivery %s/%s: %v", eventID, agentID, err)
		}
		for _, turn := range turnsByDelivery[eventID+"\x00"+agentID] {
			insertSQLiteTraceTurnWithMemory(t, ctx, sqliteStore, claimed.Claim, event, turn.id, fixture.runID, agentID, sessionID, true, turn.at)
		}
		var snapshot runtimedelivery.Snapshot
		switch state {
		case runtimedelivery.StateDelivered:
			snapshot, err = sqliteStore.SettleSuccess(ctx, claimed.Claim, nil, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection())
		case runtimedelivery.StateRetrying:
			failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "trace_failure", nil)
			snapshot, err = sqliteStore.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
				Disposition: runtimedelivery.FailureRetry,
				ReasonCode:  "handler_error",
				Failure:     &failure,
				RetryBase:   time.Hour, RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
			})
		default:
			t.Fatalf("trace fixture state %q is unsupported", state)
		}
		if err != nil {
			t.Fatalf("settle trace delivery %s/%s: %v", eventID, agentID, err)
		}
		setSQLiteDeliveryFixtureTimes(t, ctx, sqliteStore.backend.ConstructionHandle(), snapshot, createdAt, transitionAt)
		return snapshot
	}
	seedDelivery(fixture.lateDeliveredID, "agent-late", "00000000-0000-0000-0000-000000000301", runtimedelivery.StateDelivered, base.Add(time.Second), base.Add(3*time.Second))
	seedDelivery(fixture.failedID, "agent-failed", "00000000-0000-0000-0000-000000000302", runtimedelivery.StateRetrying, base.Add(1500*time.Millisecond), base.Add(2*time.Second))
	seedDelivery(fixture.secondDeliveredID, "agent-second", "00000000-0000-0000-0000-000000000303", runtimedelivery.StateDelivered, base.Add(3*time.Second), base.Add(5*time.Second))
	tieA := seedDelivery("00000000-0000-0000-0000-000000000005", "agent-a", "00000000-0000-0000-0000-000000000304", runtimedelivery.StateDelivered, base.Add(10*time.Second), base.Add(10*time.Second))
	tieB := seedDelivery("00000000-0000-0000-0000-000000000005", "agent-b", "00000000-0000-0000-0000-000000000305", runtimedelivery.StateDelivered, base.Add(10*time.Second), base.Add(10*time.Second))
	fixture.tieDeliveryAID = tieA.DeliveryID
	fixture.tieDeliveryBID = tieB.DeliveryID
	return fixture
}

func seedSQLiteTraceAgent(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore, agentID string, startedAt time.Time) {
	t.Helper()
	seedTestAgentRow(t, ctx, sqliteStore.backend.ConstructionHandle(), false, testAgentIdentity(t, agentID, "flow-a"), "active")
}

func insertSQLiteTraceSession(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore, sessionID, runID, agentID string, updatedAt time.Time) {
	t.Helper()
	fields := testAgentIdentityStorageFields(t, testAgentIdentity(t, agentID, "flow-a"))
	if _, err := sqliteStore.backend.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored',
			'[]', 0, '{}', 'active', ?, ?
		)
	`, sessionID, runID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		updatedAt.Add(-time.Second), updatedAt); err != nil {
		t.Fatalf("seed session %s: %v", agentID, err)
	}
}

func insertSQLiteTraceTurnWithMemory(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore, claim runtimedelivery.Claim, event events.Event, turnID, runID, agentID, sessionID string, memoryEnabled bool, createdAt time.Time) {
	t.Helper()
	memory := agentmemory.PlatformDefault()
	if memoryEnabled {
		memory = agentmemory.Authored(true)
	}
	identity := testAgentIdentity(t, agentID, "flow-a")
	if err := persistManagedAgentTurnReadbackFixtureWithOptions(t, runtimedelivery.WithClaim(ctx, claim), sqliteStore, runtimellm.AgentTurnRecord{
		AgentID: agentID, Identity: agentmemory.Identity{RunID: runID, Agent: identity}, RunID: runID,
		FlowInstance: identity.FlowInstance(), Memory: memory, SessionID: sessionID,
		TriggerEventID: event.ID(), TriggerEventType: string(event.Type()), TaskID: "task-1",
		RequestPayload: []byte(`{}`), ResponseRaw: []byte(`{}`), ParseOK: true,
	}, managedAgentTurnFixtureOptions{TurnID: turnID, Now: createdAt, OriginEvent: &event}); err != nil {
		t.Fatalf("seed turn %s/%s: %v", agentID, turnID, err)
	}
}

func traceEventIDs(rows []operatorread.RunDebugTraceRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.EventID)
	}
	return out
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
