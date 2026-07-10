package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLiteRunDebugTracePagePaginationWindowAndFilterParity(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	fixture := seedSQLiteRunTraceParityRows(t, ctx, sqliteStore)
	mainFilter := RunDebugTraceFilter{
		EventNames: []string{"trace.event_only", "trace.late_delivered", "trace.failed", "trace.second_delivered"},
	}

	page1, next, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, RunDebugTraceQueryOptions{Limit: 2, Filter: mainFilter})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage page1: %v", err)
	}
	if len(page1) != 2 || page1[0].EventID != fixture.eventOnlyID || page1[1].EventID != fixture.lateDeliveredID || next == "" {
		t.Fatalf("page1 rows=%#v next=%q, want event-only then late-delivered with next cursor", page1, next)
	}
	page2, next2, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, RunDebugTraceQueryOptions{Limit: 2, Cursor: next, Filter: mainFilter})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage page2: %v", err)
	}
	if len(page2) != 2 || page2[0].EventID != fixture.failedID || page2[1].EventID != fixture.secondDeliveredID || next2 != "" {
		t.Fatalf("page2 rows=%#v next=%q, want failed then second-delivered and no next cursor", page2, next2)
	}
	if _, _, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, RunDebugTraceQueryOptions{Limit: 2, Cursor: "not-a-cursor"}); !errors.Is(err, ErrInvalidObservabilityCursor) {
		t.Fatalf("invalid cursor error = %v, want ErrInvalidObservabilityCursor", err)
	}

	sinceRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, RunDebugTraceQueryOptions{Limit: 10, Since: &fixture.base, Filter: mainFilter})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage since: %v", err)
	}
	if got := traceEventIDs(sinceRows); !sameStrings(got, []string{fixture.lateDeliveredID, fixture.failedID, fixture.secondDeliveredID}) {
		t.Fatalf("since rows = %#v, want late materialized rows only", got)
	}
	until := fixture.base.Add(3500 * time.Millisecond)
	untilRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, RunDebugTraceQueryOptions{Limit: 10, Until: &until, Filter: mainFilter})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage until: %v", err)
	}
	if got := traceEventIDs(untilRows); !sameStrings(got, []string{fixture.eventOnlyID, fixture.failedID}) {
		t.Fatalf("until rows = %#v, want rows whose materialization watermark is <= until", got)
	}
	emptyWindowRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, RunDebugTraceQueryOptions{Limit: 10, Since: &fixture.base, Until: &fixture.base, Filter: mainFilter})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage equal since/until: %v", err)
	}
	if len(emptyWindowRows) != 0 {
		t.Fatalf("equal since/until rows = %#v, want empty strict/inclusive window", emptyWindowRows)
	}

	deliveredPage1, deliveredNext, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, RunDebugTraceQueryOptions{
		Limit: 1,
		Since: &fixture.base,
		Filter: RunDebugTraceFilter{
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
	deliveredPage2, deliveredNext2, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, RunDebugTraceQueryOptions{
		Limit:  1,
		Cursor: deliveredNext,
		Since:  &fixture.base,
		Filter: RunDebugTraceFilter{
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
	ctx := context.Background()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	fixture := seedSQLiteRunTraceParityRows(t, ctx, sqliteStore)

	var got []string
	cursor := ""
	for {
		rows, next, err := sqliteStore.LoadRunDebugTracePage(ctx, fixture.runID, RunDebugTraceQueryOptions{
			Limit:  1,
			Cursor: cursor,
			Filter: RunDebugTraceFilter{
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
	if !sameStrings(got, want) {
		t.Fatalf("tie paging rows = %#v, want %#v", got, want)
	}
}

func TestSQLiteRunDebugTracePageExcludeRuntimeLogs(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)

	runID := "00000000-0000-0000-0000-000000001816"
	businessEvent := "00000000-0000-0000-0000-000000001817"
	runtimeLogEvent := "00000000-0000-0000-0000-000000001818"
	base := time.Unix(1700000600, 0).UTC()
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, base); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO events (run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at)
		VALUES
			(?, ?, 'item.received', NULL, 'global', '{}', 'runtime', 'platform', ?),
			(?, ?, 'platform.runtime_log', NULL, 'global', '{}', 'runtime', 'platform', ?)
	`, runID, businessEvent, base, runID, runtimeLogEvent, base.Add(time.Millisecond)); err != nil {
		t.Fatalf("seed trace rows: %v", err)
	}

	allRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage all: %v", err)
	}
	if got := traceEventIDs(allRows); !sameStrings(got, []string{businessEvent, runtimeLogEvent}) {
		t.Fatalf("all trace rows = %#v, want business and runtime_log", got)
	}
	filteredRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10, ExcludeRuntimeLogs: true})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage filtered: %v", err)
	}
	if got := traceEventIDs(filteredRows); !sameStrings(got, []string{businessEvent}) {
		t.Fatalf("filtered trace rows = %#v, want business row only", got)
	}
}

func TestSQLiteRunDebugTracePageIncludesTaskAuditSessionsInWatermark(t *testing.T) {
	ctx := context.Background()
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	base := time.Unix(1700003200, 0).UTC()
	runID := "00000000-0000-0000-0000-000000001430"
	eventID := "00000000-0000-0000-0000-000000001431"
	deliveryID := "00000000-0000-0000-0000-000000001432"
	sessionID := "00000000-0000-0000-0000-000000001433"
	turnID := "00000000-0000-0000-0000-000000001434"
	agentID := "agent-task"

	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, base.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO events (run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at)
		VALUES (?, ?, 'trace.task_audit', NULL, 'global', '{}', 'runtime', 'platform', ?)
	`, runID, eventID, base); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	seedSQLiteTraceAgent(t, ctx, sqliteStore, agentID, base)
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		) VALUES (
			?, ?, ?, NULL, 'flow-a', 'global', 'global',
			'[]', 1, 'task', '{}', 'active', ?, ?
		)
	`, sessionID, runID, agentID, base.Add(time.Second), base.Add(5*time.Second)); err != nil {
		t.Fatalf("seed task audit: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at, started_at, delivered_at
		) VALUES (
			?, ?, ?, 'agent', ?, 'delivered', 'ok', ?, ?, ?
		)
	`, deliveryID, runID, eventID, agentID, base.Add(time.Second), base.Add(time.Second), base.Add(2*time.Second)); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	insertSQLiteTraceTurnWithMode(t, ctx, sqliteStore, turnID, runID, agentID, sessionID, eventID, "trace.task_audit", "task", base.Add(2*time.Second))

	rows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %#v, want one task audit trace row", rows)
	}
	row := rows[0]
	if row.SessionID != sessionID || row.SessionKind != "turn_audit" || row.SessionRuntimeMode != "task" {
		t.Fatalf("task audit session fields = %#v, want turn_audit task session %s", row, sessionID)
	}
	if row.SessionUpdatedAt == nil || !row.SessionUpdatedAt.Equal(base.Add(5*time.Second)) {
		t.Fatalf("task audit session updated_at = %#v, want %s", row.SessionUpdatedAt, base.Add(5*time.Second))
	}

	since := base.Add(4 * time.Second)
	sinceRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10, Since: &since})
	if err != nil {
		t.Fatalf("LoadRunDebugTracePage since: %v", err)
	}
	if len(sinceRows) != 1 || sinceRows[0].EventID != eventID {
		t.Fatalf("since rows = %#v, want task row included by audit updated_at watermark", sinceRows)
	}
	until := base.Add(4 * time.Second)
	untilRows, _, err := sqliteStore.LoadRunDebugTracePage(ctx, runID, RunDebugTraceQueryOptions{Limit: 10, Until: &until})
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
		tieDeliveryAID:    "00000000-0000-0000-0000-000000000101",
		tieDeliveryBID:    "00000000-0000-0000-0000-000000000102",
		tieTurnA1ID:       "00000000-0000-0000-0000-000000000201",
		tieTurnA2ID:       "00000000-0000-0000-0000-000000000202",
		tieTurnBID:        "00000000-0000-0000-0000-000000000203",
		base:              base,
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, fixture.runID, base.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	events := []struct {
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
	for _, event := range events {
		if _, err := sqliteStore.DB.ExecContext(ctx, `
			INSERT INTO events (run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at)
			VALUES (?, ?, ?, NULL, 'global', '{}', 'runtime', 'platform', ?)
		`, fixture.runID, event.id, event.name, event.at); err != nil {
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

	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, reason_code, active_session_id, created_at, started_at, delivered_at
		) VALUES
			('00000000-0000-0000-0000-000000000011', ?, ?, 'agent', 'agent-late', 'delivered', 'ok', '00000000-0000-0000-0000-000000000301', ?, ?, ?),
			('00000000-0000-0000-0000-000000000012', ?, ?, 'agent', 'agent-failed', 'failed', 'handler_error', '00000000-0000-0000-0000-000000000302', ?, ?, NULL),
			('00000000-0000-0000-0000-000000000013', ?, ?, 'agent', 'agent-second', 'delivered', 'ok', '00000000-0000-0000-0000-000000000303', ?, ?, ?),
			(?, ?, '00000000-0000-0000-0000-000000000005', 'agent', 'agent-a', 'delivered', 'ok', '00000000-0000-0000-0000-000000000304', ?, ?, ?),
			(?, ?, '00000000-0000-0000-0000-000000000005', 'agent', 'agent-b', 'delivered', 'ok', '00000000-0000-0000-0000-000000000305', ?, ?, ?)
	`, fixture.runID, fixture.lateDeliveredID, base.Add(time.Second), base.Add(2*time.Second), base.Add(3*time.Second),
		fixture.runID, fixture.failedID, base.Add(1500*time.Millisecond), base.Add(2*time.Second),
		fixture.runID, fixture.secondDeliveredID, base.Add(3*time.Second), base.Add(4*time.Second), base.Add(5*time.Second),
		fixture.tieDeliveryAID, fixture.runID, base.Add(10*time.Second), base.Add(10*time.Second), base.Add(10*time.Second),
		fixture.tieDeliveryBID, fixture.runID, base.Add(10*time.Second), base.Add(10*time.Second), base.Add(10*time.Second)); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	insertSQLiteTraceTurn(t, ctx, sqliteStore, "00000000-0000-0000-0000-000000000401", fixture.runID, "agent-late", "00000000-0000-0000-0000-000000000301", fixture.lateDeliveredID, "trace.late_delivered", base.Add(5*time.Second))
	insertSQLiteTraceTurn(t, ctx, sqliteStore, "00000000-0000-0000-0000-000000000402", fixture.runID, "agent-failed", "00000000-0000-0000-0000-000000000302", fixture.failedID, "trace.failed", base.Add(3*time.Second))
	insertSQLiteTraceTurn(t, ctx, sqliteStore, "00000000-0000-0000-0000-000000000403", fixture.runID, "agent-second", "00000000-0000-0000-0000-000000000303", fixture.secondDeliveredID, "trace.second_delivered", base.Add(6*time.Second))
	insertSQLiteTraceTurn(t, ctx, sqliteStore, fixture.tieTurnA1ID, fixture.runID, "agent-a", "00000000-0000-0000-0000-000000000304", "00000000-0000-0000-0000-000000000005", "trace.tie", base.Add(11*time.Second))
	insertSQLiteTraceTurn(t, ctx, sqliteStore, fixture.tieTurnA2ID, fixture.runID, "agent-a", "00000000-0000-0000-0000-000000000304", "00000000-0000-0000-0000-000000000005", "trace.tie", base.Add(11*time.Second))
	insertSQLiteTraceTurn(t, ctx, sqliteStore, fixture.tieTurnBID, fixture.runID, "agent-b", "00000000-0000-0000-0000-000000000305", "00000000-0000-0000-0000-000000000005", "trace.tie", base.Add(11*time.Second))
	return fixture
}

func seedSQLiteTraceAgent(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore, agentID string, startedAt time.Time) {
	t.Helper()
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, role, model, llm_backend, conversation_mode,
			config, subscriptions, emit_events, tools, permissions, runtime_descriptor, status, created_at
		) VALUES (
			?, 'operator', 'regular', 'anthropic', 'session',
			'{}', '[]', '[]', '[]', '[]', '{}', 'active', ?
		)
	`, agentID, startedAt); err != nil {
		t.Fatalf("seed agent %s: %v", agentID, err)
	}
}

func insertSQLiteTraceSession(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore, sessionID, runID, agentID string, updatedAt time.Time) {
	t.Helper()
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		) VALUES (
			?, ?, ?, NULL, 'flow-a', 'global', 'global',
			'[]', 1, 'session', '{}', 'active', ?, ?
		)
	`, sessionID, runID, agentID, updatedAt.Add(-time.Second), updatedAt); err != nil {
		t.Fatalf("seed session %s: %v", agentID, err)
	}
}

func insertSQLiteTraceTurn(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore, turnID, runID, agentID, sessionID, eventID, eventName string, createdAt time.Time) {
	t.Helper()
	insertSQLiteTraceTurnWithMode(t, ctx, sqliteStore, turnID, runID, agentID, sessionID, eventID, eventName, "session", createdAt)
}

func insertSQLiteTraceTurnWithMode(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore, turnID, runID, agentID, sessionID, eventID, eventName, runtimeMode string, createdAt time.Time) {
	t.Helper()
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
			trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
			emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
			request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count, failure, created_at
		) VALUES (
			?, ?, ?, ?, ?, 'global', NULL,
			?, ?, 'task-1', '[]', '[]',
			'[]', '{}', '[]', '[]',
			'{}', '{}', '[]', 1, 0, 0, NULL, ?
		)
	`, turnID, runID, agentID, sessionID, runtimeMode, eventID, eventName, createdAt); err != nil {
		t.Fatalf("seed turn %s/%s: %v", agentID, turnID, err)
	}
}

func traceEventIDs(rows []RunDebugTraceRow) []string {
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
