package pipeline

import (
	"context"
	"testing"
	"time"

	runtimecontracts "empireai/internal/runtime/contracts"
	runtimeengine "empireai/internal/runtime/engine"
)

func TestApplyEngineStateMutationMirrorsDataAccumulationIntoEntityProjection(t *testing.T) {
	instance := &WorkflowInstance{
		Metadata:     map[string]any{"research_context": map[string]any{"summary": "done"}},
		StateBuckets: map[string]any{},
	}
	mutation := runtimeengine.StateMutation{
		Metadata: map[string]any{
			"research_context":              map[string]any{"summary": "done"},
			"last_data_accumulation_event":  "research.completed",
			"last_data_accumulation_source": "research.completed",
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "research_context", SourceField: "payload.research_context"},
			},
		},
	}
	applyEngineStateMutation(instance, mutation, map[string]struct{}{"research_context": {}})

	entityProjection, _ := workflowStateBucketObject(*instance, workflowStateBucketEntityProjection)
	got, ok := entityProjection["research_context"].(map[string]any)
	if !ok || got["summary"] != "done" {
		t.Fatalf("entity_projection research_context = %#v", entityProjection["research_context"])
	}
	if got := instance.Metadata["last_data_accumulation_event"]; got != "research.completed" {
		t.Fatalf("last_data_accumulation_event = %#v", got)
	}
}

type recordingScheduleStore struct {
	upserts []Schedule
	cancels []Schedule
}

func (s *recordingScheduleStore) UpsertSchedule(_ context.Context, sc Schedule) error {
	s.upserts = append(s.upserts, sc)
	return nil
}
func (s *recordingScheduleStore) CancelSchedule(context.Context, string, string) error { return nil }
func (s *recordingScheduleStore) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}
func (s *recordingScheduleStore) MarkScheduleFired(context.Context, Schedule) error { return nil }
func (s *recordingScheduleStore) CancelScheduleExact(_ context.Context, sc Schedule) error {
	s.cancels = append(s.cancels, sc)
	return nil
}
func (s *recordingScheduleStore) MarkScheduleFiredExact(context.Context, Schedule) error { return nil }

func TestPipelineEngineTimerApplierPersistsTimersAndDefersSchedulerToPostCommit(t *testing.T) {
	store := &recordingScheduleStore{}
	scheduler := NewScheduler()
	defer scheduler.Stop()
	pc := &FactoryPipelineCoordinator{
		timerScheduler:     scheduler,
		timerScheduleStore: store,
	}
	actions := make([]func(), 0, 2)
	ctx := withPipelinePostCommitActions(context.Background(), &actions)
	sc := Schedule{
		AgentID:   "owner",
		EventType: "timer.review",
		Mode:      "once",
		At:        time.Now().Add(time.Hour),
		EntityID:  "ent-1",
		TaskID:    "timer-1",
	}

	pc.persistWorkflowTimerSchedule(ctx, sc)
	if got := len(store.upserts); got != 1 {
		t.Fatalf("persisted schedules = %d, want 1", got)
	}
	if got := len(actions); got != 1 {
		t.Fatalf("post-commit actions = %d, want 1", got)
	}
	if got := len(scheduler.tasks); got != 0 {
		t.Fatalf("scheduler tasks before flush = %d, want 0", got)
	}
	flushPipelinePostCommitActions(actions)
	if got := len(scheduler.tasks); got != 1 {
		t.Fatalf("scheduler tasks after flush = %d, want 1", got)
	}

	cancelActions := make([]func(), 0, 1)
	cancelCtx := withPipelinePostCommitActions(context.Background(), &cancelActions)
	pc.persistWorkflowTimerCancellation(cancelCtx, sc)
	if got := len(store.cancels); got != 1 {
		t.Fatalf("persisted cancels = %d, want 1", got)
	}
	if got := len(cancelActions); got != 1 {
		t.Fatalf("cancel post-commit actions = %d, want 1", got)
	}
	flushPipelinePostCommitActions(cancelActions)
	if got := len(scheduler.tasks); got != 0 {
		t.Fatalf("scheduler tasks after cancel flush = %d, want 0", got)
	}
}
