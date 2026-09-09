package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	eventtestsql "github.com/division-sh/swarm/internal/store/testsql"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeStoreListEventsMissingPipelineReceiptExcludesDiagnosticDirectEvents(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	now := time.Now().Add(-time.Minute).UTC()
	requireRunFixtureForTest(t, ctx, NewSQLiteRuntimeStoreForTest(store.backend.ConstructionHandle()), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: now})

	runtimeLogID := persistSQLiteRuntimeLogForReplayTest(t, ctx, store, runID)
	executableID := uuid.NewString()
	appendSQLiteReplayTestEvent(t, ctx, store, eventtest.PersistedProjection(executableID,

		events.EventType("workflow.executable"),
		"runtime", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, now.Add(3*time.Second)))
	eventtestsql.CorruptEventStore(t, ctx, store.backend.ConstructionHandle(), authoractivityfixture.DialectSQLite, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.named_operation_atomicity",
		Reason:    "prove recovery fails closed when durable replay-scope evidence is missing",
	}, `DELETE FROM committed_replay_scopes WHERE event_id = ?`, "", executableID)

	presence, err := store.PipelineObligations().GlobalWorkPresence(ctx)
	if err != nil {
		t.Fatalf("GlobalWorkPresence: %v", err)
	}
	if !presence.ProcessingEligible {
		t.Fatalf("pipeline work presence = %#v, want executable recovery work", presence)
	}
	summary, err := store.PipelineObligations().SummarizeRun(ctx, runID)
	if err != nil {
		t.Fatalf("SummarizeRun: %v", err)
	}
	if summary.Replayable != 1 || summary.DiagnosticExcluded == 0 {
		t.Fatalf("pipeline summary = %#v, want one executable and diagnostic exclusion", summary)
	}

	logs, err := store.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{
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

	bus, err := newStoreTestEventBus(t, store)
	if err != nil {
		t.Fatalf("NewEventBus(sqlite): %v", err)
	}
	result, err := bus.SweepPipelineObligations(ctx, 20)
	if err != nil {
		t.Fatalf("SweepPipelineObligations(sqlite): %v", err)
	}
	if result.Settled != 1 {
		t.Fatalf("SweepPipelineObligations(sqlite) settled = %d, want typed corrupt-work settlement", result.Settled)
	}

	assertNoSQLitePipelineReceipt(t, ctx, store, runtimeLogID)
	assertSQLitePipelineReceipt(t, ctx, store, executableID, "dead_letter", "committed_pipeline_scope_missing")
}

func TestPostgresStoreListEventsMissingPipelineReceiptExcludesDiagnosticDirectEvents(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	runID := uuid.NewString()
	now := time.Now().Add(-time.Minute).UTC()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: now})

	runtimeLogID := persistPostgresRuntimeLogForReplayTest(t, ctx, pg, runID)
	executableID := uuid.NewString()
	appendPostgresReplayTestEvent(t, ctx, pg, eventtest.PersistedProjection(executableID,

		events.EventType("workflow.executable"),
		"runtime", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, now.Add(3*time.Second)))
	eventtestsql.CorruptEventStore(t, ctx, pg.backend.ConstructionHandle(), authoractivityfixture.DialectPostgres, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.named_operation_atomicity",
		Reason:    "prove recovery fails closed when durable replay-scope evidence is missing",
	}, "", `DELETE FROM committed_replay_scopes WHERE event_id = $1::uuid`, executableID)

	presence, err := pg.PipelineObligations().GlobalWorkPresence(ctx)
	if err != nil {
		t.Fatalf("GlobalWorkPresence: %v", err)
	}
	if !presence.ProcessingEligible {
		t.Fatalf("pipeline work presence = %#v, want executable recovery work", presence)
	}
	summary, err := pg.PipelineObligations().SummarizeRun(ctx, runID)
	if err != nil {
		t.Fatalf("SummarizeRun: %v", err)
	}
	if summary.Replayable != 1 || summary.DiagnosticExcluded == 0 {
		t.Fatalf("pipeline summary = %#v, want one executable and diagnostic exclusion", summary)
	}

	logs, err := pg.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{
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

	bus, err := newStoreTestEventBus(t, pg)
	if err != nil {
		t.Fatalf("NewEventBus(postgres): %v", err)
	}
	result, err := bus.SweepPipelineObligations(ctx, 20)
	if err != nil {
		t.Fatalf("SweepPipelineObligations(postgres): %v", err)
	}
	if result.Settled != 1 {
		t.Fatalf("SweepPipelineObligations(postgres) settled = %d, want typed corrupt-work settlement", result.Settled)
	}

	assertNoPostgresPipelineReceipt(t, ctx, pg, runtimeLogID)
	assertPostgresPipelineReceipt(t, ctx, pg, executableID, "dead_letter", "committed_pipeline_scope_missing")
}

func persistSQLiteRuntimeLogForReplayTest(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, runID string) string {
	t.Helper()
	payload := json.RawMessage(`{"log_level":"warn","message":"diagnostic replay proof","details":{"component":"diagnostic_replay","action":"proof"}}`)
	record := runtimeLogPersistenceRecordForReplayTest(t, runID, payload)
	if err := store.PersistRuntimeLog(ctx, record); err != nil {
		t.Fatalf("PersistRuntimeLog(sqlite): %v", err)
	}
	var eventID string
	if err := store.backend.QueryRowContext(ctx, `
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
	record := runtimeLogPersistenceRecordForReplayTest(t, runID, payload)
	if err := pg.PersistRuntimeLog(ctx, record); err != nil {
		t.Fatalf("PersistRuntimeLog(postgres): %v", err)
	}
	var eventID string
	if err := pg.backend.QueryRowContext(ctx, `
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

func runtimeLogPersistenceRecordForReplayTest(t *testing.T, runID string, payload json.RawMessage) runtimepkg.RuntimeLogPersistenceRecord {
	t.Helper()
	event := eventtest.DiagnosticDirect(
		uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0, "", "", events.EventEnvelope{},
		time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC),
	)
	admission, err := eventtest.PayloadAdmission(event, "", string(events.EventTypePlatformRuntimeLog))
	if err != nil {
		t.Fatalf("bind runtime-log admission fixture: %v", err)
	}
	return runtimepkg.RuntimeLogPersistenceRecord{
		RunID: runID, Payload: payload, PayloadAdmission: admission, ExecutionMode: executionmode.Live,
	}
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

func assertNoSQLitePipelineReceipt(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, eventID string) {
	t.Helper()
	var count int
	if err := store.backend.QueryRowContext(ctx, `
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
	if err := store.backend.QueryRowContext(ctx, `
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
	if err := pg.backend.QueryRowContext(ctx, `
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
	if err := pg.backend.QueryRowContext(ctx, `
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
