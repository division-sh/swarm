package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	runtimedeadletters "swarm/internal/runtime/deadletters"
	"swarm/internal/testutil"
)

func TestRunDebugReadSurface_ListRunDebugRuns_UsesCanonicalRunScope(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	olderRunID := uuid.NewString()
	newerRunID := uuid.NewString()
	olderEventID := uuid.NewString()
	newerEventID := uuid.NewString()
	now := time.Unix(1700000000, 0).UTC()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at, ended_at)
		VALUES
			($1::uuid, 'completed', $3, $4),
			($2::uuid, 'running', $5, NULL)
	`, olderRunID, newerRunID, now.Add(-2*time.Hour), now.Add(-90*time.Minute), now.Add(-1*time.Hour)); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'scan.requested', 'global', '{}'::jsonb, 'test', 'agent', $5),
			($1::uuid, gen_random_uuid(), 'scan.completed', 'global', '{}'::jsonb, 'test', 'agent', $6),
			($3::uuid, $4::uuid, 'scan.requested', 'global', '{}'::jsonb, 'test', 'agent', $7)
	`, olderRunID, olderEventID, newerRunID, newerEventID, now.Add(-119*time.Minute), now.Add(-91*time.Minute), now.Add(-59*time.Minute)); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	runs, err := pg.ListRunDebugRuns(ctx, 10)
	if err != nil {
		t.Fatalf("ListRunDebugRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("ListRunDebugRuns len = %d, want 2", len(runs))
	}
	if runs[0].RunID != newerRunID {
		t.Fatalf("runs[0].RunID = %q, want %q", runs[0].RunID, newerRunID)
	}
	if runs[0].RootEventID != newerEventID || runs[0].RootEventType != "scan.requested" {
		t.Fatalf("runs[0] root = %#v", runs[0])
	}
	if runs[1].RunID != olderRunID {
		t.Fatalf("runs[1].RunID = %q, want %q", runs[1].RunID, olderRunID)
	}
	if runs[1].EventCount != 2 {
		t.Fatalf("runs[1].EventCount = %d, want 2", runs[1].EventCount)
	}
}

func TestRunDebugReadSurface_LoadRunDebugReport_UsesCanonicalRunIDForLogsAndMutations(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	targetRunID := uuid.NewString()
	otherRunID := uuid.NewString()
	targetEntityID := uuid.NewString()
	otherEntityID := uuid.NewString()
	targetEventID := uuid.NewString()
	otherEventID := uuid.NewString()
	now := time.Unix(1700000000, 0).UTC()

	for _, runID := range []string{targetRunID, otherRunID} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, started_at)
			VALUES ($1::uuid, 'running', $2)
		`, runID, now.Add(-5*time.Minute)); err != nil {
			t.Fatalf("seed run %s: %v", runID, err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'scan.requested', $3::uuid, 'global', '{}'::jsonb, 'test', 'agent', $4),
			($5::uuid, $6::uuid, 'scan.requested', $7::uuid, 'global', '{}'::jsonb, 'test', 'agent', $8)
	`, targetRunID, targetEventID, targetEntityID, now.Add(-4*time.Minute), otherRunID, otherEventID, otherEntityID, now.Add(-3*time.Minute)); err != nil {
		t.Fatalf("seed root events: %v", err)
	}

	insertRuntimeLog := func(runID string, payloadRunID string, component string, action string, createdAt time.Time) {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"log_level": "warn",
			"message":   action,
			"details": map[string]any{
				"run_id":    payloadRunID,
				"component": component,
				"action":    action,
				"error":     action + "-error",
			},
		})
		if err != nil {
			t.Fatalf("marshal runtime log payload: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (
				run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
			)
			VALUES ($1::uuid, gen_random_uuid(), 'platform.runtime_log', 'global', $2::jsonb, 'test', 'agent', $3)
		`, runID, string(payload), createdAt); err != nil {
			t.Fatalf("seed runtime log for run %s: %v", runID, err)
		}
	}

	insertRuntimeLog(targetRunID, otherRunID, "scheduler", "canonical-owner", now)
	insertRuntimeLog(otherRunID, targetRunID, "scheduler", "payload-only", now.Add(1*time.Minute))

	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', $4::jsonb, $5::jsonb, $3::uuid, 'platform', 'runner', 'step-a', $6),
			($7::uuid, $8::uuid, 'current_state', $10::jsonb, $11::jsonb, $9::uuid, 'platform', 'runner', 'step-b', $12)
	`, targetRunID, targetEntityID, targetEventID, `"queued"`, `"running"`, now.Add(2*time.Minute), otherRunID, otherEntityID, otherEventID, `"queued"`, `"failed"`, now.Add(3*time.Minute)); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, delivered_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'agent-1', 'delivered', $3, $4)
	`, targetRunID, targetEventID, now.Add(10*time.Second), now.Add(5*time.Second)); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if err := runtimedeadletters.Insert(ctx, db, runtimedeadletters.Record{
		OriginalEventID: targetEventID,
		OriginalEvent:   "scan.requested",
		EntityID:        targetEntityID,
		FailureType:     "handler_error",
		ErrorMessage:    "boom",
		HandlerNode:     "node-a",
		Timestamp:       now.Add(4 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("seed dead letter: %v", err)
	}

	report, err := pg.LoadRunDebugReport(ctx, targetRunID, RunDebugQueryOptions{
		LogsAllLevels:   false,
		Component:       "scheduler",
		EventLimit:      10,
		MutationLimit:   10,
		RuntimeLogLimit: 10,
		DeadLetterLimit: 10,
	})
	if err != nil {
		t.Fatalf("LoadRunDebugReport: %v", err)
	}
	if report.RunID != targetRunID {
		t.Fatalf("RunID = %q, want %q", report.RunID, targetRunID)
	}
	if report.RootEventID != targetEventID {
		t.Fatalf("RootEventID = %q, want %q", report.RootEventID, targetEventID)
	}
	if report.WarnErrorLogCount != 1 {
		t.Fatalf("WarnErrorLogCount = %d, want 1", report.WarnErrorLogCount)
	}
	if len(report.RuntimeLogs) != 1 {
		t.Fatalf("RuntimeLogs len = %d, want 1", len(report.RuntimeLogs))
	}
	if got := report.RuntimeLogs[0]; got.Component != "scheduler" || got.Action != "canonical-owner" {
		t.Fatalf("RuntimeLogs[0] = %#v", got)
	}
	if len(report.Mutations) != 1 {
		t.Fatalf("Mutations len = %d, want 1", len(report.Mutations))
	}
	if got := report.Mutations[0]; got.EntityID != targetEntityID || got.Field != "current_state" || got.WriterType != "platform" || got.WriterID != "runner" {
		t.Fatalf("Mutations[0] = %#v", got)
	}
	if len(report.DeadLetters) != 1 {
		t.Fatalf("DeadLetters len = %d, want 1", len(report.DeadLetters))
	}
	if len(report.Deliveries) != 1 {
		t.Fatalf("Deliveries len = %d, want 1", len(report.Deliveries))
	}
}
