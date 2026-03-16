package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
)

func TestScheduleEventPayloadInjectsEntityID(t *testing.T) {
	payload := scheduleEventPayload(runtimepipeline.Schedule{
		EntityID: "ent-001",
		Payload:  []byte(`{"timer_id":"check_timer","trigger_reason":"check_timer"}`),
	})
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("Unmarshal(payload): %v", err)
	}
	if got := decoded["entity_id"]; got != "ent-001" {
		t.Fatalf("entity_id = %#v, want %q", got, "ent-001")
	}
	if _, ok := decoded["timer_id"]; ok {
		t.Fatalf("timer_id should be stripped from published payload, got %#v", decoded["timer_id"])
	}
	if _, ok := decoded["trigger_reason"]; ok {
		t.Fatalf("trigger_reason should be stripped from published payload, got %#v", decoded["trigger_reason"])
	}
}

func TestScheduleEventPayloadPreservesExistingEntityID(t *testing.T) {
	payload := scheduleEventPayload(runtimepipeline.Schedule{
		EntityID: "ent-001",
		Payload:  []byte(`{"entity_id":"payload-entity","timer_id":"check_timer","__schedule_task_id":"timer-1"}`),
	})
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("Unmarshal(payload): %v", err)
	}
	if got := decoded["entity_id"]; got != "payload-entity" {
		t.Fatalf("entity_id = %#v, want %q", got, "payload-entity")
	}
	if _, ok := decoded["timer_id"]; ok {
		t.Fatalf("timer_id should be stripped from published payload, got %#v", decoded["timer_id"])
	}
	if _, ok := decoded["__schedule_task_id"]; ok {
		t.Fatalf("__schedule_task_id should be stripped from published payload, got %#v", decoded["__schedule_task_id"])
	}
}

type recordingRuntimeScheduleStore struct {
	schedules []runtimepipeline.Schedule
}

func (s *recordingRuntimeScheduleStore) UpsertSchedule(_ context.Context, sc runtimepipeline.Schedule) error {
	s.schedules = append(s.schedules, sc)
	return nil
}

func (*recordingRuntimeScheduleStore) CancelSchedule(context.Context, string, string) error {
	return nil
}

func (*recordingRuntimeScheduleStore) LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error) {
	return nil, nil
}

func (*recordingRuntimeScheduleStore) MarkScheduleFired(context.Context, runtimepipeline.Schedule) error {
	return nil
}

type semanticOnlyWorkflowRuntime struct {
	source semanticview.Source
}

func (s semanticOnlyWorkflowRuntime) SemanticSource() semanticview.Source { return s.source }
func (semanticOnlyWorkflowRuntime) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}
func (semanticOnlyWorkflowRuntime) WorkflowNodes() []runtimepipeline.WorkflowNode { return nil }
func (semanticOnlyWorkflowRuntime) WorkflowInstanceStore() runtimepipeline.WorkflowInstancePersistence {
	return nil
}
func (semanticOnlyWorkflowRuntime) TransitionEvaluator() runtimepipeline.TransitionEvaluator {
	return nil
}
func (semanticOnlyWorkflowRuntime) GuardRegistry() runtimepipeline.GuardRegistry { return nil }
func (semanticOnlyWorkflowRuntime) ActionRegistry() runtimepipeline.ActionRegistry { return nil }

func TestEnsureRecurringWorkflowSchedulesSkipsLifecycleScopedRecurringTimers(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", "test-timer-recurring")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	store := &recordingRuntimeScheduleStore{}
	err = ensureRecurringWorkflowSchedules(context.Background(), store, semanticOnlyWorkflowRuntime{
		source: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("ensureRecurringWorkflowSchedules: %v", err)
	}
	if len(store.schedules) != 0 {
		t.Fatalf("startup recurring schedules = %#v, want none for start_on recurring timers", store.schedules)
	}
}
