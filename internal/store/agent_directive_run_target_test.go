package store

import (
	"context"
	"errors"
	"testing"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresStoreResolveAgentDirectiveRunTarget(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := testAuthorActivityContext()
	pg := admitTestPostgresStore(t, db)
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
			failure JSONB,
			ended_at TIMESTAMPTZ,
			bundle_fingerprint TEXT
		);
		CREATE TABLE agent_sessions (
			session_id UUID PRIMARY KEY,
			run_id UUID NOT NULL REFERENCES runs(run_id),
			agent_id TEXT NOT NULL,
			flow_instance TEXT NOT NULL,
			memory_enabled BOOLEAN NOT NULL DEFAULT TRUE,
			memory_source TEXT NOT NULL DEFAULT 'authored',
			conversation JSONB NOT NULL DEFAULT '[]'::jsonb,
			turn_count INTEGER NOT NULL DEFAULT 0,
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
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status)
		VALUES ($1::uuid, $2::uuid, $3, 'directive', TRUE, 'authored', 'active')
	`, sessionID, runID, agentID); err != nil {
		t.Fatalf("insert session %s: %v", sessionID, err)
	}
}
