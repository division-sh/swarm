package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	eventtestsql "github.com/division-sh/swarm/internal/store/testsql"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeStoreListEventsMissingPipelineReceiptExcludesDiagnosticDirectEvents(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	now := time.Now().Add(-time.Minute).UTC()
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, now); err != nil {
		t.Fatalf("seed sqlite replay run: %v", err)
	}

	runtimeLogID := persistSQLiteRuntimeLogForReplayTest(t, ctx, store, runID)
	executableID := uuid.NewString()
	appendSQLiteReplayTestEvent(t, ctx, store, eventtest.PersistedProjection(executableID,

		events.EventType("workflow.executable"),
		"runtime", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, now.Add(3*time.Second)))
	eventtestsql.CorruptEventStore(t, ctx, store.DB, runtimeauthoractivity.DialectSQLite, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.named_operation_atomicity",
		Reason:    "prove recovery fails closed when durable replay-scope evidence is missing",
	}, `DELETE FROM event_deliveries WHERE event_id = ? AND subscriber_type = ? AND subscriber_id = ?`, "", executableID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID)

	globalMissing, err := store.ListEventsMissingPipelineReceipt(ctx, now.Add(-time.Hour), 20)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceipt: %v", err)
	}
	assertReplayEventIDs(t, globalMissing, []string{executableID})

	runMissing, err := store.ListEventsMissingPipelineReceiptForRun(ctx, runID, now.Add(-time.Hour), 20)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceiptForRun: %v", err)
	}
	assertReplayEventIDs(t, runMissing, []string{executableID})

	logs, err := store.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		Level:     "warn",
		Component: "diagnostic_replay",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs: %v", err)
	}
	if len(logs.Logs) != 1 || logs.Logs[0].LogID != runtimeLogID {
		t.Fatalf("runtime logs = %#v, want runtime log %s", logs.Logs, runtimeLogID)
	}

	bus, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus(sqlite): %v", err)
	}
	swept, err := bus.SweepUndispatched(ctx, time.Hour, 20)
	if err != nil {
		t.Fatalf("SweepUndispatched(sqlite): %v", err)
	}
	if swept != 0 {
		t.Fatalf("SweepUndispatched(sqlite) redelivered = %d, want 0", swept)
	}

	assertNoSQLitePipelineReceipt(t, ctx, store, runtimeLogID)
	assertSQLitePipelineReceipt(t, ctx, store, executableID, "dead_letter", "committed_replay_scope_missing")
}

func TestPostgresStoreListEventsMissingPipelineReceiptExcludesDiagnosticDirectEvents(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	runID := uuid.NewString()
	now := time.Now().Add(-time.Minute).UTC()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, now); err != nil {
		t.Fatalf("seed postgres replay run: %v", err)
	}

	runtimeLogID := persistPostgresRuntimeLogForReplayTest(t, ctx, pg, runID)
	executableID := uuid.NewString()
	appendPostgresReplayTestEvent(t, ctx, pg, eventtest.PersistedProjection(executableID,

		events.EventType("workflow.executable"),
		"runtime", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, now.Add(3*time.Second)))
	eventtestsql.CorruptEventStore(t, ctx, pg.DB, runtimeauthoractivity.DialectPostgres, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.named_operation_atomicity",
		Reason:    "prove recovery fails closed when durable replay-scope evidence is missing",
	}, "", `DELETE FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = $2 AND subscriber_id = $3`, executableID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID)

	globalMissing, err := pg.ListEventsMissingPipelineReceipt(ctx, now.Add(-time.Hour), 20)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceipt: %v", err)
	}
	assertReplayEventIDs(t, globalMissing, []string{executableID})

	runMissing, err := pg.ListEventsMissingPipelineReceiptForRun(ctx, runID, now.Add(-time.Hour), 20)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceiptForRun: %v", err)
	}
	assertReplayEventIDs(t, runMissing, []string{executableID})

	logs, err := pg.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		RunID:     runID,
		Level:     "warn",
		Component: "diagnostic_replay",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs: %v", err)
	}
	if len(logs.Logs) != 1 || logs.Logs[0].LogID != runtimeLogID {
		t.Fatalf("runtime logs = %#v, want runtime log %s", logs.Logs, runtimeLogID)
	}

	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus(postgres): %v", err)
	}
	swept, err := bus.SweepUndispatched(ctx, time.Hour, 20)
	if err != nil {
		t.Fatalf("SweepUndispatched(postgres): %v", err)
	}
	if swept != 0 {
		t.Fatalf("SweepUndispatched(postgres) redelivered = %d, want 0", swept)
	}

	assertNoPostgresPipelineReceipt(t, ctx, pg, runtimeLogID)
	assertPostgresPipelineReceipt(t, ctx, pg, executableID, "dead_letter", "committed_replay_scope_missing")
}

func persistSQLiteRuntimeLogForReplayTest(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, runID string) string {
	t.Helper()
	payload := json.RawMessage(`{"log_level":"warn","message":"diagnostic replay proof","details":{"component":"diagnostic_replay","action":"proof"}}`)
	if err := store.PersistRuntimeLog(ctx, runtimepkg.RuntimeLogPersistenceRecord{RunID: runID, Payload: payload, ExecutionMode: executionmode.Live}); err != nil {
		t.Fatalf("PersistRuntimeLog(sqlite): %v", err)
	}
	var eventID string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT event_id
		FROM events
		WHERE run_id = ?
		  AND event_name = 'platform.runtime_log'
		ORDER BY created_at DESC, event_id DESC
		LIMIT 1
	`, runID).Scan(&eventID); err != nil {
		t.Fatalf("load sqlite runtime log event_id: %v", err)
	}
	return eventID
}

func persistPostgresRuntimeLogForReplayTest(t *testing.T, ctx context.Context, pg *PostgresStore, runID string) string {
	t.Helper()
	payload := json.RawMessage(`{"log_level":"warn","message":"diagnostic replay proof","details":{"component":"diagnostic_replay","action":"proof"}}`)
	if err := pg.PersistRuntimeLog(ctx, runtimepkg.RuntimeLogPersistenceRecord{RunID: runID, Payload: payload, ExecutionMode: executionmode.Live}); err != nil {
		t.Fatalf("PersistRuntimeLog(postgres): %v", err)
	}
	var eventID string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT event_id::text
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'platform.runtime_log'
		ORDER BY created_at DESC, event_id DESC
		LIMIT 1
	`, runID).Scan(&eventID); err != nil {
		t.Fatalf("load postgres runtime log event_id: %v", err)
	}
	return eventID
}

func appendSQLiteReplayTestEvent(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, evt events.Event) {
	t.Helper()
	if err := commitSemanticEventFixture(ctx, store, evt); err != nil {
		t.Fatalf("AppendEvent(%s): %v", evt.Type(), err)
	}
}

func appendPostgresReplayTestEvent(t *testing.T, ctx context.Context, pg *PostgresStore, evt events.Event) {
	t.Helper()
	if err := commitSemanticEventFixture(ctx, pg, evt); err != nil {
		t.Fatalf("AppendEvent(%s): %v", evt.Type(), err)
	}
}

func assertReplayEventIDs(t *testing.T, got []events.PersistedReplayEvent, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("replay event IDs = %#v, want %#v", replayEventIDs(got), want)
	}
	for i, eventID := range want {
		if got[i].Event.ID() != eventID {
			t.Fatalf("replay event IDs = %#v, want %#v", replayEventIDs(got), want)
		}
	}
}

func replayEventIDs(records []events.PersistedReplayEvent) []string {
	out := make([]string, 0, len(records))
	for _, record := range records {
		out = append(out, record.Event.ID())
	}
	return out
}

func assertNoSQLitePipelineReceipt(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, eventID string) {
	t.Helper()
	var count int
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = ?
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&count); err != nil {
		t.Fatalf("count sqlite pipeline receipts: %v", err)
	}
	if count != 0 {
		t.Fatalf("sqlite pipeline receipts for %s = %d, want 0", eventID, count)
	}
}

func assertSQLitePipelineReceipt(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, eventID, outcome, reason string) {
	t.Helper()
	var gotOutcome, gotReason string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = ?
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&gotOutcome, &gotReason); err != nil {
		t.Fatalf("load sqlite pipeline receipt: %v", err)
	}
	if gotOutcome != outcome || gotReason != reason {
		t.Fatalf("sqlite pipeline receipt for %s = outcome:%q reason:%q, want outcome:%q reason:%q", eventID, gotOutcome, gotReason, outcome, reason)
	}
}

func assertNoPostgresPipelineReceipt(t *testing.T, ctx context.Context, pg *PostgresStore, eventID string) {
	t.Helper()
	var count int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&count); err != nil {
		t.Fatalf("count postgres pipeline receipts: %v", err)
	}
	if count != 0 {
		t.Fatalf("postgres pipeline receipts for %s = %d, want 0", eventID, count)
	}
}

func assertPostgresPipelineReceipt(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, outcome, reason string) {
	t.Helper()
	var gotOutcome, gotReason string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&gotOutcome, &gotReason); err != nil {
		t.Fatalf("load postgres pipeline receipt: %v", err)
	}
	if gotOutcome != outcome || gotReason != reason {
		t.Fatalf("postgres pipeline receipt for %s = outcome:%q reason:%q, want outcome:%q reason:%q", eventID, gotOutcome, gotReason, outcome, reason)
	}
}
