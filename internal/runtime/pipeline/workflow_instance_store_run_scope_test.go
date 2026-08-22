package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkflowInstanceStore_RequiresRunContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := newPostgresWorkflowInstanceStoreForTest(db)

	err := store.upsert(testAuthorActivityContext(t, context.Background()), materializedWorkflowInstanceForTest(WorkflowInstance{
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
	if err := store.upsert(ctxA, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      "run-scope",
		StorageRef:      "run-scope",
		EntityID:        entityID,
		WorkflowName:    "run-scope",
		WorkflowVersion: "1.0.0",
		CurrentState:    "source_state",
	})); err != nil {
		t.Fatalf("upsert source state: %v", err)
	}
	// flow_instances is global while entity_state is run-scoped. This read
	// isolation proof establishes a complete second-run pair directly instead
	// of asking the runtime mutation path to repair lifecycle-only persistence.
	now := time.Now().UTC()
	if _, err := db.ExecContext(ctxB, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, bookkeeping, accumulator, revision,
			entered_state_at, created_at, updated_at
		) VALUES ($1::uuid, $2::uuid, 'run-scope', 'run-scope', 'fork_state',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $3, $3, $3)
	`, runB, entityID, now); err != nil {
		t.Fatalf("seed complete fork-run state: %v", err)
	}
	gotA, ok, err := store.Load(ctxA, testWorkflowInstanceRoute("run-scope"))
	if err != nil || !ok {
		t.Fatalf("load source ok=%v err=%v", ok, err)
	}
	gotB, ok, err := store.Load(ctxB, testWorkflowInstanceRoute("run-scope"))
	if err != nil || !ok {
		t.Fatalf("load fork ok=%v err=%v", ok, err)
	}
	if gotA.CurrentState != "source_state" || gotB.CurrentState != "fork_state" {
		t.Fatalf("states = source:%q fork:%q", gotA.CurrentState, gotB.CurrentState)
	}
}
