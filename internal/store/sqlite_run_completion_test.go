package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/google/uuid"
)

type sqliteNormalRunCompletionFixture struct {
	RunID    string
	EventID  string
	EntityID string
}

func seedSQLiteNormalRunCompletionFixture(t *testing.T, store *SQLiteRuntimeStore, state string) sqliteNormalRunCompletionFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	if err := store.AppendEvent(ctx, events.Event{
		ID:          eventID,
		RunID:       runID,
		Type:        events.EventType("example.started"),
		SourceAgent: "test",
		Payload:     json.RawMessage(`{"example":true}`),
		CreatedAt:   now,
	}.WithEntityID(entityID)); err != nil {
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
	if err := db.QueryRowContext(context.Background(), `
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
	ctx := context.Background()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	fixture := seedSQLiteNormalRunCompletionFixture(t, store, "done")
	if err := store.UpsertPipelineReceipt(ctx, fixture.EventID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, entity_id, flow_instance, source_route, target_route, target_set,
			scope, payload, chain_depth, produced_by, produced_by_type, source_event_id, created_at
		)
		VALUES (?, ?, ?, NULL, NULL, '{}', '{}', '[]', 'global', ?, 0, 'runtime', 'platform', ?, ?)
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

func TestSQLiteRuntimeStoreConvergeNormalRunCompletionFailsClosedWhileDeliveryActive(t *testing.T) {
	ctx := context.Background()
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
	if err := store.UpsertPipelineReceipt(ctx, fixture.EventID, "processed", ""); err != nil {
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
