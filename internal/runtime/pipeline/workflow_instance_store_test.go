package pipeline

import (
	"context"
	"testing"
	"time"

	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkflowInstanceStore_NilDBIsDisabled(t *testing.T) {
	store := NewWorkflowInstanceStore(nil)
	if store.Enabled() {
		t.Fatal("expected nil-db workflow instance store to be disabled")
	}
	if _, ok, err := store.Load(context.Background(), uuid.NewString()); err != nil || ok {
		t.Fatalf("expected disabled store load to be empty, ok=%v err=%v", ok, err)
	}
}

func TestWorkflowInstanceStore_UpsertLoadDelete(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	ctx := context.Background()
	now := time.Now().UTC().Round(time.Second)
	instanceID := uuid.NewString()

	inst := WorkflowInstance{
		InstanceID:      instanceID,
		WorkflowName:    "empire_vertical_pipeline",
		WorkflowVersion: "2.1.0",
		CurrentStage:    "researching",
		EnteredStageAt:  now,
		TransitionHistory: []WorkflowTransitionRecord{{
			TransitionID:   "shortlisted_to_researching",
			From:           "shortlisted",
			To:             "researching",
			TriggerEventID: uuid.NewString(),
			FiredAt:        now,
			GuardsEvaluated: []string{"has_vertical_id"},
		}},
		AccumulatorState: map[string]any{
			"pipeline-coordinator": map[string]any{"g1_research": true},
		},
		TimerState: []WorkflowTimerState{{
			TimerID:   "scan_timeout",
			CreatedAt: now,
			FiresAt:   now.Add(90 * time.Minute),
		}},
		Metadata: map[string]any{
			"revision_count": 1,
		},
		CreatedAt: now,
	}
	if err := store.Upsert(ctx, inst); err != nil {
		t.Fatalf("upsert workflow instance: %v", err)
	}

	loaded, ok, err := store.Load(ctx, instanceID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected stored workflow instance")
	}
	if loaded.WorkflowName != inst.WorkflowName || loaded.CurrentStage != inst.CurrentStage {
		t.Fatalf("unexpected workflow instance: %+v", loaded)
	}
	if len(loaded.TransitionHistory) != 1 || loaded.TransitionHistory[0].TransitionID != "shortlisted_to_researching" {
		t.Fatalf("unexpected transition history: %+v", loaded.TransitionHistory)
	}
	if len(loaded.TimerState) != 1 || loaded.TimerState[0].TimerID != "scan_timeout" {
		t.Fatalf("unexpected timer state: %+v", loaded.TimerState)
	}
	if got := asInt(loaded.Metadata["revision_count"]); got != 1 {
		t.Fatalf("unexpected revision_count: %v", loaded.Metadata["revision_count"])
	}

	if err := store.Delete(ctx, instanceID); err != nil {
		t.Fatalf("delete workflow instance: %v", err)
	}
	if _, ok, err := store.Load(ctx, instanceID); err != nil || ok {
		t.Fatalf("expected workflow instance to be deleted, ok=%v err=%v", ok, err)
	}
}
