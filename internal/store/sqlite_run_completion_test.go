package store

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type sqliteNormalRunCompletionFixture struct {
	RunID    string
	EventID  string
	EntityID string
}

func seedSQLiteNormalRunCompletionFixture(t *testing.T, store *SQLiteRuntimeStore, state string) sqliteNormalRunCompletionFixture {
	t.Helper()
	ctx := testAuthorActivityContext()
	now := time.Now().UTC()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if err := store.AppendEvent(ctx, eventtest.PersistedProjection(
		eventID,
		events.EventType("example.started"),
		"test",
		"",
		json.RawMessage(`{"example":true}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		now,
	)); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (?, ?, '', 'default', 'example', 'Example', ?,
			'{}', '{}', '{}', 1, ?, ?, ?)
	`, runID, entityID, state, now, now, now); err != nil {
		t.Fatalf("seed sqlite entity state: %v", err)
	}
	return sqliteNormalRunCompletionFixture{RunID: runID, EventID: eventID, EntityID: entityID}
}

func assertSQLiteRunCompletionStatus(t *testing.T, db *sql.DB, runID, want string, wantEnded bool) {
	t.Helper()
	var (
		status string
		ended  any
	)
	if err := db.QueryRowContext(testAuthorActivityContext(), `
		SELECT COALESCE(status, ''), ended_at
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&status, &ended); err != nil {
		t.Fatalf("load sqlite run status: %v", err)
	}
	if status != want {
		t.Fatalf("run status = %q, want %q", status, want)
	}
	if _, ok, err := sqliteTimeValue(ended); err != nil {
		t.Fatalf("parse ended_at: %v", err)
	} else if ok != wantEnded {
		t.Fatalf("ended_at present = %v, want %v", ok, wantEnded)
	}
}

func TestSQLiteRuntimeStoreConvergeNormalRunCompletionMarksCompletedAndIgnoresRuntimeLogs(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	fixture := seedSQLiteNormalRunCompletionFixture(t, store, "done")
	if err := store.UpsertPipelineReceipt(ctx, fixture.EventID, "processed", nil); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, entity_id, flow_instance, source_route, target_route, target_set,
			scope, payload, chain_depth, produced_by, produced_by_type, source_event_id, created_at
		)
		VALUES ('live', ?, ?, ?, NULL, NULL, '{}', '{}', '[]', 'global', ?, 0, 'runtime', 'platform', ?, ?)
	`, uuid.NewString(), fixture.RunID, runtimeLogEventName, `{"message":"diagnostic"}`, fixture.EventID, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite runtime log: %v", err)
	}

	if err := store.ConvergeNormalRunCompletion(ctx, fixture.EventID, []string{"done"}, nil); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion: %v", err)
	}
	assertSQLiteRunCompletionStatus(t, store.DB, fixture.RunID, "completed", true)

	snap, err := store.LoadRunLifecycleSnapshot(ctx, fixture.RunID)
	if err != nil {
		t.Fatalf("LoadRunLifecycleSnapshot: %v", err)
	}
	if snap.Status != "completed" || snap.EndedAt == nil || snap.EventCount != 2 || snap.EntityCount != 1 {
		t.Fatalf("snapshot = %#v, want completed with runtime-log-inclusive counters", snap)
	}
	header, err := store.LoadRunHeader(ctx, fixture.RunID)
	if err != nil {
		t.Fatalf("LoadRunHeader: %v", err)
	}
	if header.Status != "completed" || header.EndedAt == nil {
		t.Fatalf("run header = status:%q ended:%v, want completed with ended_at", header.Status, header.EndedAt)
	}
}

func TestSQLiteRuntimeStoreMarkRunTerminalPreservesFailureAndRejectsConflict(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}
	failure := testFailureEnvelope(runtimefailures.ClassInternalFailure, "run_quiescence_failed", nil)
	snap, err := store.MarkRunTerminal(ctx, runID, "failed", &failure, time.Now().UTC())
	if err != nil {
		t.Fatalf("MarkRunTerminal(failed): %v", err)
	}
	if snap.Failure == nil || !failureEnvelopesEqual(*snap.Failure, failure) {
		t.Fatalf("snapshot failure = %#v, want %#v", snap.Failure, failure)
	}
	if _, err := store.MarkRunTerminal(ctx, runID, "failed", &failure, time.Now().UTC()); err != nil {
		t.Fatalf("idempotent failed terminal write: %v", err)
	}
	conflicting := testFailureEnvelope(runtimefailures.ClassInternalFailure, "different_run_failure", nil)
	if _, err := store.MarkRunTerminal(ctx, runID, "failed", &conflicting, time.Now().UTC()); err == nil {
		t.Fatal("conflicting sqlite terminal write was accepted")
	}
	if _, err := store.MarkRunTerminal(ctx, uuid.NewString(), "failed", nil, time.Now().UTC()); err == nil {
		t.Fatal("failed sqlite terminal write without failure was accepted")
	}
}

func TestSQLiteRunLifecycleEntityCountUsesEntityState(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	now := time.Now().UTC()
	runID := uuid.NewString()
	eventEntityA := uuid.NewString()
	eventEntityB := uuid.NewString()
	currentEntity := uuid.NewString()
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, event_count, entity_count, started_at)
		VALUES (?, 'running', 99, 9, ?)
	`, runID, now); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO events (execution_mode, event_id, run_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at)
		VALUES
			('live', ?, ?, 'scan.requested', ?, 'entity', '{}', 'test', 'agent', ?),
			('live', ?, ?, 'scan.replayed', ?, 'entity', '{}', 'test', 'agent', ?)
	`, uuid.NewString(), runID, eventEntityA, now.Add(time.Second), uuid.NewString(), runID, eventEntityB, now.Add(2*time.Second)); err != nil {
		t.Fatalf("seed sqlite events: %v", err)
	}
	seedSQLiteEntityStateRows(t, store.DB, ctx, runID, currentEntity)

	snap, err := store.LoadRunLifecycleSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("LoadRunLifecycleSnapshot: %v", err)
	}
	if snap.EntityCount != 1 {
		t.Fatalf("snapshot entity_count = %d, want entity_state count 1 despite stale run/event overcount", snap.EntityCount)
	}

	if err := sqliteSyncRunCounts(ctx, store.DB, runID); err != nil {
		t.Fatalf("sqliteSyncRunCounts: %v", err)
	}
	var eventCount, entityCount int
	if err := store.DB.QueryRowContext(ctx, `
		SELECT event_count, entity_count
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&eventCount, &entityCount); err != nil {
		t.Fatalf("load synced sqlite counters: %v", err)
	}
	if eventCount != 2 || entityCount != 1 {
		t.Fatalf("synced counters event_count=%d entity_count=%d, want 2/1 from events/entity_state", eventCount, entityCount)
	}
}

func TestSQLiteRuntimeStoreConvergeNormalRunCompletionFailsClosedWhileDeliveryActive(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	fixture := seedSQLiteNormalRunCompletionFixture(t, store, "done")
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		)
		VALUES (?, ?, ?, 'node', 'terminal-node', 'pending', 'matched_node_subscription', ?)
	`, uuid.NewString(), fixture.RunID, fixture.EventID, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite active delivery: %v", err)
	}
	if err := store.UpsertPipelineReceipt(ctx, fixture.EventID, "processed", nil); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	if err := store.ConvergeNormalRunCompletion(ctx, fixture.EventID, []string{"done"}, nil); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion active: %v", err)
	}
	assertSQLiteRunCompletionStatus(t, store.DB, fixture.RunID, "running", false)

	if _, err := store.DB.ExecContext(ctx, `
		UPDATE event_deliveries
		SET status = 'delivered',
		    reason_code = 'node_processed',
		    delivered_at = ?
		WHERE event_id = ?
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, time.Now().UTC(), fixture.EventID); err != nil {
		t.Fatalf("settle sqlite active delivery: %v", err)
	}
	if err := store.ConvergeNormalRunCompletion(ctx, fixture.EventID, []string{"done"}, nil); err != nil {
		t.Fatalf("ConvergeNormalRunCompletion settled: %v", err)
	}
	assertSQLiteRunCompletionStatus(t, store.DB, fixture.RunID, "completed", true)
}
