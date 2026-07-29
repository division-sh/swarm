package pipeline

import (
	"context"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkflowInstanceStore_RequiresRunContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := newPostgresWorkflowInstanceStoreForTest(db)

	err := store.Upsert(testAuthorActivityContext(t, context.Background()), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      uuid.NewString(),
		StorageRef:      uuid.NewString(),
		WorkflowName:    "run-scope",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
	}))
	if err == nil || !strings.Contains(err.Error(), "run_id is required") {
		t.Fatalf("Upsert error = %v, want missing run_id", err)
	}
}

func TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := newPostgresWorkflowInstanceStoreForTest(db)
	runA := uuid.NewString()
	runB := uuid.NewString()
	entityID := uuid.NewString()
	for _, runID := range []string{runA, runB} {
		runlifecyclefixture.RequirePostgres(t, testAuthorActivityContext(t, context.Background()), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
	}
	ctxA := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runA)
	ctxB := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runB)
	for _, tc := range []struct {
		ctx   context.Context
		state string
	}{
		{ctx: ctxA, state: "source_state"},
		{ctx: ctxB, state: "fork_state"},
	} {
		if err := store.Upsert(tc.ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
			InstanceID:      entityID,
			StorageRef:      entityID,
			WorkflowName:    "run-scope",
			WorkflowVersion: "1.0.0",
			CurrentState:    tc.state,
		})); err != nil {
			t.Fatalf("upsert %s: %v", tc.state, err)
		}
	}
	gotA, ok, err := store.Load(ctxA, entityID)
	if err != nil || !ok {
		t.Fatalf("load source ok=%v err=%v", ok, err)
	}
	gotB, ok, err := store.Load(ctxB, entityID)
	if err != nil || !ok {
		t.Fatalf("load fork ok=%v err=%v", ok, err)
	}
	if gotA.CurrentState != "source_state" || gotB.CurrentState != "fork_state" {
		t.Fatalf("states = source:%q fork:%q", gotA.CurrentState, gotB.CurrentState)
	}
}
