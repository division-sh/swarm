package store

import (
	"context"
	"errors"
	"testing"

	runtimeagentcontrol "swarm/internal/runtime/agentcontrol"
	"swarm/internal/testutil"
)

func TestPostgresStoreResolveAgentDirectiveRunTarget(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := context.Background()
	pg := &PostgresStore{DB: db}
	createDirectiveRunTargetTables(t, ctx, pg)

	runA := "00000000-0000-0000-0000-0000000000a1"
	runB := "00000000-0000-0000-0000-0000000000b1"
	runDone := "00000000-0000-0000-0000-0000000000c1"
	insertDirectiveRun(t, ctx, pg, runA, "running")
	insertDirectiveRun(t, ctx, pg, runB, "paused")
	insertDirectiveRun(t, ctx, pg, runDone, "completed")

	explicit, err := pg.ResolveAgentDirectiveRunTarget(ctx, "agent-a", runA)
	if err != nil {
		t.Fatalf("explicit target: %v", err)
	}
	if explicit.RunID != runA || explicit.Mode != runtimeagentcontrol.RunResolutionSpecified {
		t.Fatalf("explicit target = %#v", explicit)
	}

	missingID := "00000000-0000-0000-0000-000000000404"
	_, err = pg.ResolveAgentDirectiveRunTarget(ctx, "agent-a", missingID)
	if !errors.Is(err, runtimeagentcontrol.ErrRunNotFound) {
		t.Fatalf("missing target err = %v, want run not found", err)
	}

	_, err = pg.ResolveAgentDirectiveRunTarget(ctx, "agent-a", runDone)
	if !errors.Is(err, runtimeagentcontrol.ErrRunAlreadyTerminal) {
		t.Fatalf("terminal target err = %v, want run already terminal", err)
	}

	allocated, err := pg.ResolveAgentDirectiveRunTarget(ctx, "agent-empty", "")
	if err != nil {
		t.Fatalf("zero active target: %v", err)
	}
	if allocated.RunID == "" || allocated.Mode != runtimeagentcontrol.RunResolutionNewRunAllocated {
		t.Fatalf("zero active target = %#v", allocated)
	}

	insertDirectiveSession(t, ctx, pg, "00000000-0000-0000-0000-000000000101", "agent-one", runB)
	active, err := pg.ResolveAgentDirectiveRunTarget(ctx, "agent-one", "")
	if err != nil {
		t.Fatalf("one active target: %v", err)
	}
	if active.RunID != runB || active.Mode != runtimeagentcontrol.RunResolutionActiveSession || len(active.ActiveSessions) != 1 {
		t.Fatalf("one active target = %#v", active)
	}

	insertDirectiveSession(t, ctx, pg, "00000000-0000-0000-0000-000000000201", "agent-many", runA)
	insertDirectiveSession(t, ctx, pg, "00000000-0000-0000-0000-000000000202", "agent-many", runB)
	_, err = pg.ResolveAgentDirectiveRunTarget(ctx, "agent-many", "")
	if !errors.Is(err, runtimeagentcontrol.ErrAmbiguousRunTarget) {
		t.Fatalf("many active err = %v, want ambiguous", err)
	}

	insertDirectiveSession(t, ctx, pg, "00000000-0000-0000-0000-000000000301", "agent-null", "")
	_, err = pg.ResolveAgentDirectiveRunTarget(ctx, "agent-null", "")
	if !errors.Is(err, runtimeagentcontrol.ErrAmbiguousRunTarget) {
		t.Fatalf("null run active err = %v, want ambiguous", err)
	}
}

func createDirectiveRunTargetTables(t *testing.T, ctx context.Context, pg *PostgresStore) {
	t.Helper()
	if _, err := pg.DB.ExecContext(ctx, `DROP TABLE IF EXISTS agent_sessions CASCADE; DROP TABLE IF EXISTS runs CASCADE;`); err != nil {
		t.Fatalf("drop directive target tables: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		CREATE TABLE runs (
			run_id UUID PRIMARY KEY,
			status TEXT NOT NULL,
			started_at TIMESTAMPTZ,
			trigger_event_id UUID,
			trigger_event_type TEXT,
			event_count INTEGER NOT NULL DEFAULT 0,
			entity_count INTEGER NOT NULL DEFAULT 0,
			error_summary TEXT,
			ended_at TIMESTAMPTZ,
			bundle_fingerprint TEXT
		);
		CREATE TABLE agent_sessions (
			session_id UUID PRIMARY KEY,
			run_id UUID REFERENCES runs(run_id),
			agent_id TEXT NOT NULL,
			entity_id UUID,
			flow_instance TEXT,
			scope_key TEXT NOT NULL,
			scope TEXT NOT NULL DEFAULT 'global',
			conversation JSONB NOT NULL DEFAULT '[]'::jsonb,
			turn_count INTEGER NOT NULL DEFAULT 0,
			runtime_mode TEXT NOT NULL DEFAULT 'session',
			runtime_state JSONB NOT NULL DEFAULT '{}'::jsonb,
			lease_holder TEXT,
			lease_expires_at TIMESTAMPTZ,
			status TEXT NOT NULL DEFAULT 'active',
			termination_reason TEXT,
			termination_detail TEXT,
			successor_session_id UUID,
			terminated_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`); err != nil {
		t.Fatalf("create directive target tables: %v", err)
	}
}

func insertDirectiveRun(t *testing.T, ctx context.Context, pg *PostgresStore, runID, status string) {
	t.Helper()
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, $2)`, runID, status); err != nil {
		t.Fatalf("insert run %s: %v", runID, err)
	}
}

func insertDirectiveSession(t *testing.T, ctx context.Context, pg *PostgresStore, sessionID, agentID, runID string) {
	t.Helper()
	if runID == "" {
		if _, err := pg.DB.ExecContext(ctx, `
			INSERT INTO agent_sessions (session_id, agent_id, scope_key, scope, runtime_mode, status)
			VALUES ($1::uuid, $2, 'global', 'global', 'session', 'active')
		`, sessionID, agentID); err != nil {
			t.Fatalf("insert session %s: %v", sessionID, err)
		}
		return
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, run_id, agent_id, scope_key, scope, runtime_mode, status)
		VALUES ($1::uuid, $2::uuid, $3, 'global', 'global', 'session', 'active')
	`, sessionID, runID, agentID); err != nil {
		t.Fatalf("insert session %s: %v", sessionID, err)
	}
}
