package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/testutil"
)

func TestWorkflowInstanceStore_RequiresRunContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)

	err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      uuid.NewString(),
		StorageRef:      uuid.NewString(),
		WorkflowName:    "run-scope",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
	})
	if err == nil || !strings.Contains(err.Error(), "run_id is required") {
		t.Fatalf("Upsert error = %v, want missing run_id", err)
	}
}

func TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	runA := uuid.NewString()
	runB := uuid.NewString()
	entityID := uuid.NewString()
	for _, runID := range []string{runA, runB} {
		if _, err := db.ExecContext(context.Background(), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
			t.Fatalf("seed run %s: %v", runID, err)
		}
	}
	ctxA := runtimecorrelation.WithRunID(context.Background(), runA)
	ctxB := runtimecorrelation.WithRunID(context.Background(), runB)
	for _, tc := range []struct {
		ctx   context.Context
		state string
	}{
		{ctx: ctxA, state: "source_state"},
		{ctx: ctxB, state: "fork_state"},
	} {
		if err := store.Upsert(tc.ctx, WorkflowInstance{
			InstanceID:      entityID,
			StorageRef:      entityID,
			WorkflowName:    "run-scope",
			WorkflowVersion: "1.0.0",
			CurrentState:    tc.state,
		}); err != nil {
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

func TestWorkflowInstanceStore_RunScopedTimerRowsDoNotBleed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	runA := uuid.NewString()
	runB := uuid.NewString()
	entityID := uuid.NewString()
	now := time.Now().UTC().Round(time.Microsecond)
	for _, runID := range []string{runA, runB} {
		if _, err := db.ExecContext(context.Background(), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
			t.Fatalf("seed run %s: %v", runID, err)
		}
	}
	ctxA := runtimecorrelation.WithRunID(context.Background(), runA)
	ctxB := runtimecorrelation.WithRunID(context.Background(), runB)

	if err := store.Upsert(ctxA, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "run-scope",
		WorkflowVersion: "1.0.0",
		CurrentState:    "source_state",
		TimerState: []WorkflowTimerState{{
			TimerID:   "shared_timer",
			EventType: "timer.source",
			CreatedAt: now,
			FiresAt:   now.Add(time.Hour),
		}},
	}); err != nil {
		t.Fatalf("upsert source timer: %v", err)
	}
	if err := store.Upsert(ctxB, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "run-scope",
		WorkflowVersion: "1.0.0",
		CurrentState:    "fork_state",
		TimerState: []WorkflowTimerState{{
			TimerID:   "shared_timer",
			EventType: "timer.fork",
			CreatedAt: now.Add(time.Minute),
			FiresAt:   now.Add(2 * time.Hour),
			Cancelled: true,
		}},
	}); err != nil {
		t.Fatalf("upsert fork timer: %v", err)
	}

	gotA, ok, err := store.Load(ctxA, entityID)
	if err != nil || !ok {
		t.Fatalf("load source ok=%v err=%v", ok, err)
	}
	gotB, ok, err := store.Load(ctxB, entityID)
	if err != nil || !ok {
		t.Fatalf("load fork ok=%v err=%v", ok, err)
	}
	if len(gotA.TimerState) != 1 || gotA.TimerState[0].EventType != "timer.source" || gotA.TimerState[0].Cancelled {
		t.Fatalf("source timers = %#v, want active source timer only", gotA.TimerState)
	}
	if len(gotB.TimerState) != 1 || gotB.TimerState[0].EventType != "timer.fork" || !gotB.TimerState[0].Cancelled {
		t.Fatalf("fork timers = %#v, want cancelled fork timer only", gotB.TimerState)
	}

	var timerRows int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM timers
		WHERE entity_id = $1::uuid
		  AND timer_name = 'shared_timer'
		  AND owner_node = $2
		  AND owner_agent IS NULL
	`, entityID, workflowInstanceTimerOwnerNode).Scan(&timerRows); err != nil {
		t.Fatalf("count workflow timer rows: %v", err)
	}
	if timerRows != 2 {
		t.Fatalf("workflow timer rows = %d, want one per run", timerRows)
	}
}
