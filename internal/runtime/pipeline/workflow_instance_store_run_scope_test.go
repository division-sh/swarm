package pipeline

import (
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

func VerifyWorkflowInstanceStore_RequiresRunContextForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	fixture := open(t)
	ctx := fixture.Context
	if runtimecorrelation.RunIDFromContext(ctx) != "" {
		t.Fatal("missing-run negative control inherited a run")
	}

	err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      uuid.NewString(),
		StorageRef:      uuid.NewString(),
		WorkflowName:    "run-scope",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		EntityType:      "test_entity",
	}))
	if err == nil || err.Error() != "flow instance activation readiness: dynamic flow runtime readiness requires run_id" {
		t.Fatalf("Upsert error = %v, want missing run_id", err)
	}
}

func VerifyWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleedForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	fixture := open(t)
	runA := uuid.NewString()
	runB := uuid.NewString()
	entityID := uuid.NewString()
	for _, runID := range []string{runA, runB} {
		if err := fixture.RequireRun(runtimecorrelation.WithRunID(fixture.Context, runID), runID); err != nil {
			t.Fatal(err)
		}
	}
	ctxA := runtimecorrelation.WithRunID(fixture.Context, runA)
	ctxB := runtimecorrelation.WithRunID(fixture.Context, runB)
	if err := fixture.Construct(ctxA, materializedWorkflowInstanceForTest(WorkflowInstance{
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
	if err := fixture.Construct(ctxB, materializedWorkflowInstanceForTest(WorkflowInstance{
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
	gotA, ok, err := fixture.Persistence.LoadWorkflowInstance(ctxA, testRunScopedWorkflowInstanceFromContext(ctxA, "run-scope"))
	if err != nil || !ok {
		t.Fatalf("load source ok=%v err=%v", ok, err)
	}
	gotB, ok, err := fixture.Persistence.LoadWorkflowInstance(ctxB, testRunScopedWorkflowInstanceFromContext(ctxB, "run-scope"))
	if err != nil || !ok {
		t.Fatalf("load fork ok=%v err=%v", ok, err)
	}
	if gotA.CurrentState != "source_state" || gotB.CurrentState != "fork_state" {
		t.Fatalf("states = source:%q fork:%q", gotA.CurrentState, gotB.CurrentState)
	}
}
