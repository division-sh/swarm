package store

import (
	"context"
	"errors"
	"testing"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresStoreResolveAgentDirectiveRunTarget(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := testAuthorActivityContext()
	pg := admitTestPostgresStore(t, db)

	runA := "00000000-0000-0000-0000-0000000000a1"
	runB := "00000000-0000-0000-0000-0000000000b1"
	runDone := "00000000-0000-0000-0000-0000000000c1"
	insertDirectiveRun(t, ctx, pg, runA, "running")
	insertDirectiveRun(t, ctx, pg, runB, "paused")
	insertDirectiveRun(t, ctx, pg, runDone, "cancelled")

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

func insertDirectiveRun(t *testing.T, ctx context.Context, pg *PostgresStore, runID, status string) {
	t.Helper()
	state, err := runtimerunlifecycle.ParseState(status)
	if err != nil {
		t.Fatalf("parse run state %q: %v", status, err)
	}
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, State: state})
}

func insertDirectiveSession(t *testing.T, ctx context.Context, pg *PostgresStore, sessionID, agentID, runID string) {
	t.Helper()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ExecutionMode: "live",
			ID:            agentID,
			Type:          "worker",
			Role:          agentID,
			FlowID:        "global",
			Model:         "regular",
		},
		Status: "active",
	}); err != nil {
		t.Fatalf("upsert agent %s: %v", agentID, err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status)
		VALUES ($1::uuid, $2::uuid, $3, 'directive', TRUE, 'authored', 'active')
	`, sessionID, runID, agentID); err != nil {
		t.Fatalf("insert session %s: %v", sessionID, err)
	}
}
