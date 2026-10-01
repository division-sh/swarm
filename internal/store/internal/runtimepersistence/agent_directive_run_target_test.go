package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
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

	agentA := mustTestAgentIdentityForRun(runA, "agent-a", "directive/agent-a")
	explicit, err := pg.ResolveAgentDirectiveRunTarget(ctx, agentA)
	if err != nil {
		t.Fatalf("explicit target: %v", err)
	}
	if explicit.RunID != runA || explicit.Mode != runtimeagentcontrol.RunResolutionSpecified {
		t.Fatalf("explicit target = %#v", explicit)
	}

	missingID := "00000000-0000-0000-0000-000000000404"
	_, err = pg.ResolveAgentDirectiveRunTarget(ctx, mustTestAgentIdentityForRun(missingID, "agent-a", "directive/agent-a"))
	if !errors.Is(err, runtimeagentcontrol.ErrRunNotFound) {
		t.Fatalf("missing target err = %v, want run not found", err)
	}

	_, err = pg.ResolveAgentDirectiveRunTarget(ctx, mustTestAgentIdentityForRun(runDone, "agent-a", "directive/agent-a"))
	if !errors.Is(err, runtimeagentcontrol.ErrRunAlreadyTerminal) {
		t.Fatalf("terminal target err = %v, want run already terminal", err)
	}

	active, err := pg.ResolveAgentDirectiveRunTarget(ctx, mustTestAgentIdentityForRun(runB, "agent-active", "directive/active"))
	if err != nil {
		t.Fatalf("exact active target: %v", err)
	}
	if active.RunID != runB || active.Mode != runtimeagentcontrol.RunResolutionSpecified {
		t.Fatalf("exact active target = %#v", active)
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
