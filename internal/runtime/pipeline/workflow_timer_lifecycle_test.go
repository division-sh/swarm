package pipeline

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/testutil"
)

type recordingSchedulePersistence struct {
	schedules []Schedule
	cancels   []Schedule
}

func (s *recordingSchedulePersistence) UpsertSchedule(_ context.Context, sc Schedule) error {
	s.schedules = append(s.schedules, sc)
	return nil
}

func (s *recordingSchedulePersistence) CancelSchedule(context.Context, string, string) error {
	return nil
}

func (s *recordingSchedulePersistence) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}

func (s *recordingSchedulePersistence) MarkScheduleFired(context.Context, Schedule) error {
	return nil
}

func (s *recordingSchedulePersistence) CancelScheduleExact(_ context.Context, sc Schedule) error {
	s.cancels = append(s.cancels, sc)
	return nil
}

func (s *recordingSchedulePersistence) MarkScheduleFiredExact(context.Context, Schedule) error {
	return nil
}

func TestExecuteNodeHandlerPlan_EventTimerStartOnRegistersSchedule(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", "test-timer-fire")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	previous := defaultWorkflowModuleFactory
	SetDefaultWorkflowModuleFactory(func() WorkflowModule { return module })
	t.Cleanup(func() { SetDefaultWorkflowModuleFactory(previous) })
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	store := &recordingSchedulePersistence{}
	pc.SetTimerScheduling(NewScheduler(), store)

	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	evt := events.Event{
		ID:          "evt-start",
		Type:        events.EventType("timer.scheduled"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"ent-001"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("ent-001")

	if handled := pc.executeNodeHandlerPlan(context.Background(), "test-node", evt); !handled {
		t.Fatal("expected timer.scheduled handler to be handled")
	}
	if len(store.schedules) != 1 {
		t.Fatalf("registered schedules = %d, want 1", len(store.schedules))
	}
	got := store.schedules[0]
	if got.EventType != "timer.check" {
		t.Fatalf("scheduled event = %q, want timer.check", got.EventType)
	}
	if got.EntityID != "ent-001" {
		t.Fatalf("scheduled entity_id = %q, want ent-001", got.EntityID)
	}
	if got.TaskID != "check_timer" {
		t.Fatalf("scheduled task_id = %q, want check_timer", got.TaskID)
	}
}

func TestPipelineIntercept_EventTimerStartOnRegistersSchedule(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", "test-timer-fire")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	previous := defaultWorkflowModuleFactory
	SetDefaultWorkflowModuleFactory(func() WorkflowModule { return module })
	t.Cleanup(func() { SetDefaultWorkflowModuleFactory(previous) })
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	store := &recordingSchedulePersistence{}
	pc.SetTimerScheduling(NewScheduler(), store)

	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	evt := events.Event{
		ID:          "evt-start",
		Type:        events.EventType("timer.scheduled"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"ent-001"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("ent-001")

	_, handled := pc.interceptPolicy("timer.scheduled", evt)
	if !handled {
		t.Fatal("expected timer.scheduled to be interceptable")
	}
	passThrough, emitted, err := pc.Intercept(context.Background(), evt)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected timer start event to remain visible downstream")
	}
	if len(emitted) != 0 {
		t.Fatalf("emitted = %#v, want none for timer start event", emitted)
	}
	if len(store.schedules) != 1 {
		t.Fatalf("registered schedules = %d, want 1", len(store.schedules))
	}
	if got := store.schedules[0].EventType; got != "timer.check" {
		t.Fatalf("scheduled event = %q, want timer.check", got)
	}
}

func TestExecuteNodeHandlerPlan_DoesNotRunOtherNodeHandler(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-absolute-path")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	previous := defaultWorkflowModuleFactory
	SetDefaultWorkflowModuleFactory(func() WorkflowModule { return module })
	t.Cleanup(func() { SetDefaultWorkflowModuleFactory(previous) })
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	evt := events.Event{
		ID:          "evt-child-done",
		Type:        events.EventType("child/task.done"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"ent-001"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("ent-001")

	if handled := pc.executeNodeHandlerPlan(context.Background(), "dispatcher", evt); handled {
		t.Fatal("dispatcher should not handle child/task.done")
	}
	instance, ok, err := pc.workflowStore.Load(context.Background(), "ent-001")
	if err != nil {
		t.Fatalf("load workflow instance after wrong node execution: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after wrong node execution")
	}
	if got := instance.CurrentState; got != "waiting" {
		t.Fatalf("state after wrong node execution = %q, want waiting", got)
	}

	if handled := pc.executeNodeHandlerPlan(context.Background(), "listener", evt); !handled {
		t.Fatal("listener should handle child/task.done")
	}
	instance, ok, err = pc.workflowStore.Load(context.Background(), "ent-001")
	if err != nil {
		t.Fatalf("load workflow instance after listener execution: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after listener execution")
	}
	if got := instance.CurrentState; got != "done" {
		t.Fatalf("state after listener execution = %q, want done", got)
	}
}

func TestExecuteNodeHandlerPlan_AccumulateTimeoutRegistersSchedule(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", "test-accumulate-timeout")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	store := &recordingSchedulePersistence{}
	pc.SetTimerScheduling(NewScheduler(), store)

	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
		Metadata:        map[string]any{"expected_count": 5},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	start := time.Now().UTC()
	evt := events.Event{
		ID:          "evt-item-a",
		Type:        events.EventType("item.arrived"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"ent-001","item_id":"a"}`),
		CreatedAt:   start,
	}.WithEntityID("ent-001")

	if handled := pc.executeNodeHandlerPlan(context.Background(), "test-node", evt); !handled {
		t.Fatal("expected item.arrived handler to be handled")
	}
	if len(store.schedules) != 1 {
		t.Fatalf("registered schedules = %d, want 1", len(store.schedules))
	}
	got := store.schedules[0]
	if got.EventType != "accumulate.timeout" {
		t.Fatalf("scheduled event = %q, want accumulate.timeout", got.EventType)
	}
	if got.AgentID != runtimeWorkflowID {
		t.Fatalf("scheduled agent_id = %q, want %q", got.AgentID, runtimeWorkflowID)
	}
	if got.TaskID != accumulationTimeoutTaskID("test-node", "item.arrived") {
		t.Fatalf("scheduled task_id = %q", got.TaskID)
	}
	if got.EntityID != "ent-001" {
		t.Fatalf("scheduled entity_id = %q, want ent-001", got.EntityID)
	}
	if got.At.Before(start.Add(4900*time.Millisecond)) || got.At.After(start.Add(5100*time.Millisecond)) {
		t.Fatalf("scheduled at = %s, want about %s", got.At.Format(time.RFC3339Nano), start.Add(5*time.Second).Format(time.RFC3339Nano))
	}
	payload := parsePayloadMap(got.Payload)
	if asString(payload["node_id"]) != "test-node" || asString(payload["bucket_event_type"]) != "item.arrived" {
		t.Fatalf("scheduled payload = %#v", payload)
	}
}

func TestExecuteNodeHandlerPlan_AccumulateTimeoutCancelsScheduleOnTimeout(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", "test-accumulate-timeout")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	store := &recordingSchedulePersistence{}
	pc.SetTimerScheduling(NewScheduler(), store)

	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
		Metadata:        map[string]any{"expected_count": 5},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	evt := events.Event{
		ID:          "evt-item-a",
		Type:        events.EventType("item.arrived"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"ent-001","item_id":"a"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("ent-001")
	if handled := pc.executeNodeHandlerPlan(context.Background(), "test-node", evt); !handled {
		t.Fatal("expected item.arrived handler to be handled")
	}

	timeoutEvt := events.Event{
		ID:          "evt-timeout",
		Type:        events.EventType("accumulate.timeout"),
		SourceAgent: runtimeWorkflowID,
		Payload: mustJSON(map[string]any{
			"entity_id":         "ent-001",
			"node_id":           "test-node",
			"bucket_event_type": "item.arrived",
		}),
		CreatedAt: time.Now().UTC(),
	}.WithEntityID("ent-001")
	if handled := pc.executeNodeHandlerPlan(context.Background(), "test-node", timeoutEvt); !handled {
		t.Fatal("expected accumulate.timeout handler to be handled")
	}
	if len(store.cancels) != 1 {
		t.Fatalf("cancelled schedules = %d, want 1", len(store.cancels))
	}
	if got := store.cancels[0].TaskID; got != accumulationTimeoutTaskID("test-node", "item.arrived") {
		t.Fatalf("cancelled task_id = %q", got)
	}
}

func TestExecuteNodeHandlerPlan_PreservesRootStateForChildFlowTransitions(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-pin-wiring")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	previous := defaultWorkflowModuleFactory
	SetDefaultWorkflowModuleFactory(func() WorkflowModule { return module })
	t.Cleanup(func() { SetDefaultWorkflowModuleFactory(previous) })
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "ready",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	trigger := events.Event{
		ID:          "evt-work-requested",
		Type:        events.EventType("work.requested"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"ent-001"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("ent-001")

	if handled := pc.executeNodeHandlerPlan(context.Background(), "child-worker", trigger); !handled {
		t.Fatal("child-worker should handle work.requested through the input-pin alias")
	}
	instance, ok, err := pc.workflowStore.Load(context.Background(), "ent-001")
	if err != nil {
		t.Fatalf("load workflow instance after child-worker execution: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after child-worker execution")
	}
	if got := instance.CurrentState; got != "ready" {
		t.Fatalf("root state after child-worker execution = %q, want ready", got)
	}

	listenerCtx := withPipelineFlowScope(context.Background(), "child")
	completion := events.Event{
		ID:          "evt-child-work-completed",
		Type:        events.EventType("child/work.completed"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"ent-001"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("ent-001")
	handler, ok := pc.SemanticSource().NodeEventHandler("parent-listener", "child/work.completed")
	if !ok {
		t.Fatal("parent-listener handler missing for child/work.completed")
	}
	result, err := pc.executeNodeContractHandler(withPipelineFlowScope(context.Background(), "child"), "parent-listener", handler, workflowTriggerContext{
		Event: completion,
		State: pc.currentWorkflowState(withPipelineFlowScope(context.Background(), ""), "ent-001"),
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if result.Outcome == nil || len(result.Outcome.Emits) != 1 || result.Outcome.Emits[0] != "job.done" {
		t.Fatalf("handler emits = %#v, want [job.done]", result.Outcome)
	}

	if handled := pc.executeNodeHandlerPlan(listenerCtx, "parent-listener", completion); !handled {
		t.Fatal("parent-listener should clear inherited child flow scope and handle child/work.completed")
	}
	instance, ok, err = pc.workflowStore.Load(context.Background(), "ent-001")
	if err != nil {
		t.Fatalf("load workflow instance after parent-listener execution: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after parent-listener execution")
	}
	if got := instance.CurrentState; got != "done" {
		t.Fatalf("root state after parent-listener execution = %q, want done", got)
	}
}

func TestPipelineIntercept_HandlesChildFlowOutputForRootListener(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-pin-wiring")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	previous := defaultWorkflowModuleFactory
	SetDefaultWorkflowModuleFactory(func() WorkflowModule { return module })
	t.Cleanup(func() { SetDefaultWorkflowModuleFactory(previous) })
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "ready",
		Metadata:        map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	completion := events.Event{
		ID:          "evt-child-work-completed",
		Type:        events.EventType("child/work.completed"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"ent-001"}`),
		CreatedAt:   time.Now().UTC(),
	}.WithEntityID("ent-001")
	passThrough, emitted, err := pc.Intercept(context.Background(), completion)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected child/work.completed to remain visible downstream")
	}
	if len(emitted) != 1 || string(emitted[0].Type) != "job.done" {
		t.Fatalf("emitted = %#v, want [job.done]", emitted)
	}
}
