package pipeline

import (
	"context"
	"strings"
	"testing"

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
		EntityType:      "test_entity",
	}))
	if err == nil || !strings.Contains(err.Error(), "run_id is required") {
		t.Fatalf("Upsert error = %v, want missing run_id", err)
	}
}

func TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed(t *testing.T) {
	for _, backend := range []struct {
		name  string
		store func(*testing.T) *workflowInstanceStore
	}{
		{name: "postgres", store: func(t *testing.T) *workflowInstanceStore {
			_, db, _ := testutil.StartPostgres(t)
			return newPostgresWorkflowInstanceStoreForTest(db)
		}},
		{name: "sqlite", store: func(t *testing.T) *workflowInstanceStore {
			return newSQLiteWorkflowInstanceStoreForTest(t, newSQLiteWorkflowInstanceStoreTestDB(t))
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.store(t)
			runA := uuid.NewString()
			runB := uuid.NewString()
			entityID := uuid.NewString()
			for _, runID := range []string{runA, runB} {
				ensurePipelineTestRun(t, store, runID)
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
				EntityType:      "test_entity",
			})); err != nil {
				t.Fatalf("upsert source state: %v", err)
			}
			if err := store.upsert(ctxB, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID:      "run-scope",
				StorageRef:      "run-scope",
				EntityID:        entityID,
				WorkflowName:    "run-scope",
				WorkflowVersion: "1.0.0",
				CurrentState:    "fork_state",
				EntityType:      "test_entity",
			})); err != nil {
				t.Fatalf("upsert fork state: %v", err)
			}
			gotA, ok, err := store.Load(ctxA, testRunScopedWorkflowInstanceFromContext(ctxA, "run-scope"))
			if err != nil || !ok {
				t.Fatalf("load source ok=%v err=%v", ok, err)
			}
			gotB, ok, err := store.Load(ctxB, testRunScopedWorkflowInstanceFromContext(ctxB, "run-scope"))
			if err != nil || !ok {
				t.Fatalf("load fork ok=%v err=%v", ok, err)
			}
			if gotA.CurrentState != "source_state" || gotB.CurrentState != "fork_state" {
				t.Fatalf("states = source:%q fork:%q", gotA.CurrentState, gotB.CurrentState)
			}
		})
	}
}
