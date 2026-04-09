package store

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"swarm/internal/testutil"
)

func TestPostgresStore_ListAgentLifecycleFacts_UsesCanonicalLiveLifecycle(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	activeEventID := uuid.NewString()
	oldDeadLetterEventID := uuid.NewString()
	for _, eventID := range []string{activeEventID, oldDeadLetterEventID} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (
				event_id, run_id, event_name, scope, payload, produced_by, produced_by_type
			) VALUES (
				$1::uuid, $2::uuid, 'task.completed', 'global', '{}'::jsonb, 'runtime', 'agent'
			)
		`, eventID, runID); err != nil {
			t.Fatalf("seed event %s: %v", eventID, err)
		}
	}

	activeSessionID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, created_at
		) VALUES
			($1::uuid, $2::uuid, 'agent', 'agent-1', 'in_progress', $3::uuid, now() - interval '1 minute'),
			($1::uuid, $4::uuid, 'agent', 'agent-1', 'dead_letter', NULL, now() - interval '2 hours')
	`, runID, activeEventID, activeSessionID, oldDeadLetterEventID); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}

	facts, err := pg.ListAgentLifecycleFacts(ctx, []string{"agent-1"})
	if err != nil {
		t.Fatalf("ListAgentLifecycleFacts: %v", err)
	}
	if got := facts["agent-1"].CurrentState; got != "active" {
		t.Fatalf("current_state = %q, want active", got)
	}
	if got := facts["agent-1"].BlockingLayer; got != "session_execution" {
		t.Fatalf("blocking_layer = %q, want session_execution", got)
	}
}

func TestPostgresStore_ListAgentLifecycleFacts_UsesCanonicalTerminalLifecycleWhenNoLiveWorkRemains(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type
		) VALUES (
			$1::uuid, $2::uuid, 'task.completed', 'global', '{}'::jsonb, 'runtime', 'agent'
		)
	`, eventID, runID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, created_at, delivered_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-1', 'dead_letter', now() - interval '5 minutes', now() - interval '1 minute'
		)
	`, runID, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	facts, err := pg.ListAgentLifecycleFacts(ctx, []string{"agent-1"})
	if err != nil {
		t.Fatalf("ListAgentLifecycleFacts: %v", err)
	}
	if got := facts["agent-1"].CurrentState; got != "exhausted" {
		t.Fatalf("current_state = %q, want exhausted", got)
	}
	if got := facts["agent-1"].BlockingLayer; got != "delivery_terminal" {
		t.Fatalf("blocking_layer = %q, want delivery_terminal", got)
	}
}
