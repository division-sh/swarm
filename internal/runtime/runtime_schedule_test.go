package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	llm "swarm/internal/runtime/llm"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

func TestScheduleEventPayloadInjectsEntityID(t *testing.T) {
	payload := scheduleEventPayload(runtimepipeline.Schedule{
		EntityID: "ent-001",
		Payload:  []byte(`{"timer_handle":{"kind":"workflow_timer","timer_id":"check_timer"}}`),
	})
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("Unmarshal(payload): %v", err)
	}
	if got := decoded["entity_id"]; got != "ent-001" {
		t.Fatalf("entity_id = %#v, want %q", got, "ent-001")
	}
	if _, ok := decoded["timer_handle"]; ok {
		t.Fatalf("timer_handle should be stripped from published workflow timer payload, got %#v", decoded["timer_handle"])
	}
}

func TestScheduleEventPayloadPreservesExistingEntityID(t *testing.T) {
	payload := scheduleEventPayload(runtimepipeline.Schedule{
		EntityID: "ent-001",
		Payload:  []byte(`{"entity_id":"payload-entity","timer_handle":{"kind":"workflow_timer","timer_id":"check_timer"},"__schedule_task_id":"timer-1"}`),
	})
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("Unmarshal(payload): %v", err)
	}
	if got := decoded["entity_id"]; got != "payload-entity" {
		t.Fatalf("entity_id = %#v, want %q", got, "payload-entity")
	}
	if _, ok := decoded["timer_handle"]; ok {
		t.Fatalf("timer_handle should be stripped from published workflow timer payload, got %#v", decoded["timer_handle"])
	}
	if _, ok := decoded["__schedule_task_id"]; ok {
		t.Fatalf("__schedule_task_id should be stripped from published payload, got %#v", decoded["__schedule_task_id"])
	}
}

func TestScheduleEventPayloadPreservesAccumulationTimeoutHandle(t *testing.T) {
	payload := scheduleEventPayload(runtimepipeline.Schedule{
		EntityID: "ent-001",
		Payload:  []byte(`{"timer_handle":{"kind":"accumulation_timeout","bucket":{"node_id":"collector","event_type":"item.arrived"}}}`),
	})
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("Unmarshal(payload): %v", err)
	}
	if _, ok := decoded["timer_handle"]; !ok {
		t.Fatalf("expected accumulation timeout handle to remain in published payload, got %#v", decoded)
	}
	if got := decoded["entity_id"]; got != "ent-001" {
		t.Fatalf("entity_id = %#v, want %q", got, "ent-001")
	}
}

func TestScheduledEventUsesTypedScheduleEnvelope(t *testing.T) {
	evt := scheduledEvent(runtimepipeline.Schedule{
		RunID:        "11111111-1111-1111-1111-111111111111",
		AgentID:      "runtime",
		EventType:    "timer.check",
		EntityID:     "ent-001",
		FlowInstance: "review/inst-1",
		Payload:      []byte(`{"entity_id":"payload-entity","flow_instance":"payload-flow"}`),
	})

	if got := evt.EntityID(); got != "ent-001" {
		t.Fatalf("event entity_id = %q, want ent-001", got)
	}
	if got := evt.FlowInstance(); got != "review/inst-1" {
		t.Fatalf("event flow_instance = %q, want review/inst-1", got)
	}
	if evt.RunID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("event run_id = %q, want schedule run_id", evt.RunID)
	}
	if got := evt.Scope(); got != events.EventScopeEntity {
		t.Fatalf("event scope = %q, want %q", got, events.EventScopeEntity)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("Unmarshal(payload): %v", err)
	}
	if got := payload["entity_id"]; got != "payload-entity" {
		t.Fatalf("payload entity_id = %#v, want payload-entity", got)
	}
	if got := payload["flow_instance"]; got != "payload-flow" {
		t.Fatalf("payload flow_instance = %#v, want payload-flow", got)
	}
}

type recordingRuntimeScheduleStore struct {
	schedules  []runtimepipeline.Schedule
	active     []runtimepipeline.Schedule
	claims     []recordedScheduleClaim
	firedExact atomic.Int32
	firedOwned atomic.Int32
	fired      chan runtimepipeline.Schedule
}

type recordedScheduleClaim struct {
	Claimed bool
	Err     error
}

func (s *recordingRuntimeScheduleStore) UpsertSchedule(_ context.Context, sc runtimepipeline.Schedule) error {
	s.schedules = append(s.schedules, sc)
	return nil
}

func (*recordingRuntimeScheduleStore) CancelScheduleExact(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (*recordingRuntimeScheduleStore) CancelScheduleExactTerminal(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (s *recordingRuntimeScheduleStore) LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error) {
	return append([]runtimepipeline.Schedule(nil), s.active...), nil
}

func (s *recordingRuntimeScheduleStore) ClaimSchedule(context.Context, runtimepipeline.Schedule) (bool, error) {
	if len(s.claims) == 0 {
		return true, nil
	}
	claim := s.claims[0]
	s.claims = s.claims[1:]
	return claim.Claimed, claim.Err
}

func (*recordingRuntimeScheduleStore) ReleaseSchedule(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (*recordingRuntimeScheduleStore) ReleaseScheduleClaims(context.Context) error {
	return nil
}

func (s *recordingRuntimeScheduleStore) MarkScheduleFiredExact(_ context.Context, sc runtimepipeline.Schedule) error {
	s.firedExact.Add(1)
	if s.fired != nil {
		select {
		case s.fired <- sc:
		default:
		}
	}
	return nil
}

func (s *recordingRuntimeScheduleStore) CompleteScheduleFireExact(_ context.Context, sc runtimepipeline.Schedule) error {
	s.firedOwned.Add(1)
	if s.fired != nil {
		select {
		case s.fired <- sc:
		default:
		}
	}
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
func (semanticOnlyWorkflowRuntime) GuardRegistry() runtimepipeline.GuardRegistry   { return nil }
func (semanticOnlyWorkflowRuntime) ActionRegistry() runtimepipeline.ActionRegistry { return nil }

type noopLLMRuntime struct{}

func (noopLLMRuntime) StartSession(context.Context, string, string, []llm.ToolDefinition) (*llm.Session, error) {
	return &llm.Session{}, nil
}

func (noopLLMRuntime) ContinueSession(context.Context, *llm.Session, llm.Message) (*llm.Response, error) {
	return &llm.Response{}, nil
}

func TestEnsureRecurringWorkflowSchedulesSkipsLifecycleScopedRecurringTimers(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", "test-timer-recurring")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	store := &recordingRuntimeScheduleStore{}
	err = ensureRecurringWorkflowSchedules(context.Background(), store, nil, semanticOnlyWorkflowRuntime{
		source: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("ensureRecurringWorkflowSchedules: %v", err)
	}
	if len(store.schedules) != 0 {
		t.Fatalf("startup recurring schedules = %#v, want none for start_on recurring timers", store.schedules)
	}
}

func TestEnsureRecurringWorkflowSchedulesRegistersBootRecurringTimers(t *testing.T) {
	store := &recordingRuntimeScheduleStore{}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Timers: []runtimecontracts.WorkflowTimerContract{{
				ID:        "daily_report",
				Owner:     "runtime",
				Event:     "timer.daily_report",
				Delay:     "24h",
				StartOn:   "boot",
				Recurring: true,
			}},
		},
	}
	err := ensureRecurringWorkflowSchedules(context.Background(), store, nil, semanticOnlyWorkflowRuntime{
		source: semanticview.Wrap(bundle),
	})
	if err != nil {
		t.Fatalf("ensureRecurringWorkflowSchedules: %v", err)
	}
	if len(store.schedules) != 1 {
		t.Fatalf("startup recurring schedules = %#v, want 1 boot schedule", store.schedules)
	}
	if got := store.schedules[0].EventType; got != "timer.daily_report" {
		t.Fatalf("scheduled event = %q, want timer.daily_report", got)
	}
}

func TestNewRuntime_SchedulerMarksSchedulesFiredThroughCanonicalTerminalHelper(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", "test-boot-success")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	store := &recordingRuntimeScheduleStore{fired: make(chan runtimepipeline.Schedule, 1)}
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{
		Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
		LLM:     config.LLMConfig{Backend: "anthropic"},
	}, Stores: Stores{ScheduleStore: store}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: semanticOnlyWorkflowRuntime{source: semanticview.Wrap(bundle)},
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if rt.Scheduler == nil {
		t.Fatal("expected scheduler")
	}
	t.Cleanup(rt.Scheduler.Stop)

	sc := runtimepipeline.Schedule{
		AgentID:   "runtime",
		EventType: "timer.check",
		Mode:      "once",
		TaskID:    "check_timer",
		At:        time.Now().Add(10 * time.Millisecond),
		EntityID:  "ent-001",
	}
	if err := rt.Scheduler.Register(sc); err != nil {
		t.Fatalf("Register(schedule): %v", err)
	}

	select {
	case fired := <-store.fired:
		if fired.TaskID != sc.TaskID {
			t.Fatalf("fired task_id = %q, want %q", fired.TaskID, sc.TaskID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for exact schedule fire persistence")
	}
	if got := store.firedOwned.Load(); got != 1 {
		t.Fatalf("CompleteScheduleFireExact calls = %d, want 1", got)
	}
}

func TestRuntime_StartRestoresExactSchedulesDistinctByFlowInstance(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", "test-boot-success")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	store := &recordingRuntimeScheduleStore{
		active: []runtimepipeline.Schedule{
			{
				AgentID:      "runtime",
				EventType:    "timer.check",
				Mode:         "once",
				At:           time.Now().Add(25 * time.Millisecond),
				EntityID:     "ent-001",
				FlowInstance: "review/inst-1",
				TaskID:       "check_timer",
				Payload:      []byte(`{"timer_id":"check_timer"}`),
			},
			{
				AgentID:      "runtime",
				EventType:    "timer.check",
				Mode:         "once",
				At:           time.Now().Add(50 * time.Millisecond),
				EntityID:     "ent-001",
				FlowInstance: "review/inst-2",
				TaskID:       "check_timer",
				Payload:      []byte(`{"timer_id":"check_timer"}`),
			},
		},
		fired: make(chan runtimepipeline.Schedule, 2),
	}
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{
		Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
		LLM:     config.LLMConfig{Backend: "anthropic"},
	}, Stores: Stores{ScheduleStore: store}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: semanticOnlyWorkflowRuntime{source: semanticview.Wrap(bundle)},
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if rt.Scheduler == nil {
		t.Fatal("expected scheduler")
	}
	t.Cleanup(rt.Scheduler.Stop)

	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := rt.Start(runCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	seenFlows := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for len(seenFlows) < 2 {
		select {
		case fired := <-store.fired:
			if fired.TaskID != "check_timer" {
				t.Fatalf("fired task_id = %q, want check_timer", fired.TaskID)
			}
			seenFlows[fired.FlowInstance] = true
		case <-deadline:
			t.Fatalf("timed out waiting for restored exact schedules to fire; seen flow instances = %#v", seenFlows)
		}
	}
	if got := store.firedOwned.Load(); got != 2 {
		t.Fatalf("CompleteScheduleFireExact calls = %d, want 2 for distinct restored flow instances", got)
	}
}
