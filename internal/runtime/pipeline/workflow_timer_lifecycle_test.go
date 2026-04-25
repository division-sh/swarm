package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/timeridentity"
	"swarm/internal/testutil"
)

type recordingSchedulePersistence struct {
	schedules    []Schedule
	cancels      []Schedule
	releases     []Schedule
	cancelExacts int
	cancelOwned  int
	cancelErr    error
}

func (s *recordingSchedulePersistence) UpsertSchedule(_ context.Context, sc Schedule) error {
	s.schedules = append(s.schedules, sc)
	return nil
}

func (s *recordingSchedulePersistence) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}

func (*recordingSchedulePersistence) ClaimSchedule(context.Context, Schedule) (bool, error) {
	return true, nil
}

func (s *recordingSchedulePersistence) ReleaseSchedule(_ context.Context, sc Schedule) error {
	s.releases = append(s.releases, sc)
	return nil
}

func (*recordingSchedulePersistence) ReleaseScheduleClaims(context.Context) error {
	return nil
}

func (s *recordingSchedulePersistence) CancelScheduleExact(_ context.Context, sc Schedule) error {
	s.cancelExacts++
	s.cancels = append(s.cancels, sc)
	return nil
}

func (s *recordingSchedulePersistence) CancelScheduleExactTerminal(_ context.Context, sc Schedule) error {
	s.cancelOwned++
	s.cancels = append(s.cancels, sc)
	return s.cancelErr
}

func (s *recordingSchedulePersistence) MarkScheduleFiredExact(context.Context, Schedule) error {
	return nil
}

func (*recordingSchedulePersistence) CompleteScheduleFireExact(context.Context, Schedule) error {
	return nil
}

func newTimerLifecycleCoordinator(bus Bus, db *sql.DB, module WorkflowModule, store SchedulePersistence) *PipelineCoordinator {
	opts := PipelineCoordinatorOptions{Module: module}
	if store != nil {
		opts.TimerScheduler = NewScheduler()
		opts.TimerScheduleStore = store
	}
	return NewPipelineCoordinatorWithOptions(bus, db, opts)
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
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := &recordingSchedulePersistence{}
	pc := newTimerLifecycleCoordinator(noopPipelineBus{}, db, module, store)
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
	payload := parsePayloadMap(got.Payload)
	handle, ok := timeridentity.ParseTimerHandle(payload)
	if !ok || handle.Kind != timeridentity.TimerHandleWorkflowTimer || handle.TimerID != "check_timer" {
		t.Fatalf("scheduled payload handle = %#v", payload)
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
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := &recordingSchedulePersistence{}
	pc := newTimerLifecycleCoordinator(noopPipelineBus{}, db, module, store)
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

func TestPersistWorkflowTimerCancellation_ReleasesClaimAfterCanonicalCancel(t *testing.T) {
	store := &recordingSchedulePersistence{}
	pc := &PipelineCoordinator{
		timerScheduleStore: store,
	}
	sc := Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "once",
		EntityID:     "ent-001",
		FlowInstance: "review/inst-1",
		TaskID:       "timer-a",
	}

	pc.persistWorkflowTimerCancellation(context.Background(), sc)

	if got := store.cancelOwned; got != 1 {
		t.Fatalf("cancel owned calls = %d, want 1", got)
	}
	if len(store.releases) != 0 {
		t.Fatalf("caller-side releases = %d, want 0", len(store.releases))
	}
}

func TestPersistWorkflowTimerCancellation_StillCancelsSchedulerWhenClaimReleaseFailsAfterPersist(t *testing.T) {
	store := &recordingSchedulePersistence{
		cancelErr: &ScheduleTerminalError{
			Stage:             "release_claim",
			TransitionApplied: true,
			Err:               context.DeadlineExceeded,
		},
	}
	scheduler := NewScheduler()
	defer scheduler.Stop()
	pc := &PipelineCoordinator{
		timerScheduleStore: store,
		timerScheduler:     scheduler,
	}
	sc := Schedule{
		AgentID:      "validation-orchestrator",
		EventType:    "timer.validation_timeout",
		Mode:         "once",
		At:           time.Now().Add(time.Hour),
		EntityID:     "ent-001",
		FlowInstance: "review/inst-1",
		TaskID:       "timer-a",
	}
	if err := scheduler.Register(sc); err != nil {
		t.Fatalf("Register(schedule): %v", err)
	}

	pc.persistWorkflowTimerCancellation(context.Background(), sc)

	if got := store.cancelOwned; got != 1 {
		t.Fatalf("cancel owned calls = %d, want 1", got)
	}
	if got := len(scheduler.tasks); got != 0 {
		t.Fatalf("scheduler tasks after partial failure = %d, want 0", got)
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

	store := &recordingSchedulePersistence{}
	pc := newTimerLifecycleCoordinator(noopPipelineBus{}, db, module, store)
	if pc == nil {
		t.Fatal("expected coordinator")
	}

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
	wantHandle := timeridentity.AccumulationTimeoutHandle(timeridentity.NewAccumulatorBucketRef("test-node", "item.arrived"))
	if got.TaskID != wantHandle.TaskID() {
		t.Fatalf("scheduled task_id = %q", got.TaskID)
	}
	if got.EntityID != "ent-001" {
		t.Fatalf("scheduled entity_id = %q, want ent-001", got.EntityID)
	}
	if got.At.Before(start.Add(4900*time.Millisecond)) || got.At.After(start.Add(5100*time.Millisecond)) {
		t.Fatalf("scheduled at = %s, want about %s", got.At.Format(time.RFC3339Nano), start.Add(5*time.Second).Format(time.RFC3339Nano))
	}
	payload := parsePayloadMap(got.Payload)
	handle, ok := timeridentity.ParseTimerHandle(payload)
	if !ok || handle.Kind != timeridentity.TimerHandleAccumulationTimeout {
		t.Fatalf("scheduled payload = %#v", payload)
	}
	if handle.Bucket.NodeID != "test-node" || handle.Bucket.EventType != "item.arrived" {
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

	store := &recordingSchedulePersistence{}
	pc := newTimerLifecycleCoordinator(noopPipelineBus{}, db, module, store)
	if pc == nil {
		t.Fatal("expected coordinator")
	}

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
			"entity_id": "ent-001",
			"timer_handle": map[string]any{
				"kind": "accumulation_timeout",
				"bucket": map[string]any{
					"node_id":    "test-node",
					"event_type": "item.arrived",
				},
			},
		}),
		CreatedAt: time.Now().UTC(),
	}.WithEntityID("ent-001")
	if handled := pc.executeNodeHandlerPlan(context.Background(), "test-node", timeoutEvt); !handled {
		t.Fatal("expected accumulate.timeout handler to be handled")
	}
	if len(store.cancels) != 1 {
		t.Fatalf("cancelled schedules = %d, want 1", len(store.cancels))
	}
	if store.cancelOwned != 1 {
		t.Fatalf("CancelScheduleExactTerminal calls = %d, want 1", store.cancelOwned)
	}
	if got := store.cancels[0].TaskID; got != timeridentity.AccumulationTimeoutHandle(timeridentity.NewAccumulatorBucketRef("test-node", "item.arrived")).TaskID() {
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

func TestPipelineCoordinatorIntercept_NestedDescendantCompletionDoesNotEmitChildContinuation(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
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

	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	childEntityID := FlowInstanceEntityID("child/inst-1")
	grandchildEntityID := FlowInstanceEntityID("child/grandchild/inst-1")
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      childEntityID,
		SubjectID:       rootEntityID,
		StorageRef:      "child/inst-1",
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Metadata: map[string]any{
			"entity_id":        childEntityID,
			"flow_path":        "child/inst-1",
			"subject_id":       rootEntityID,
			"parent_entity_id": rootEntityID,
		},
	}); err != nil {
		t.Fatalf("seed child instance: %v", err)
	}
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      grandchildEntityID,
		SubjectID:       rootEntityID,
		StorageRef:      "child/grandchild/inst-1",
		WorkflowName:    "grandchild",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "finished",
		Metadata: map[string]any{
			"entity_id":        grandchildEntityID,
			"flow_path":        "child/grandchild/inst-1",
			"subject_id":       rootEntityID,
			"parent_entity_id": childEntityID,
		},
	}); err != nil {
		t.Fatalf("seed grandchild instance: %v", err)
	}

	passThrough, emitted, err := pc.Intercept(context.Background(), (events.Event{
		ID:          "evt-nested-done",
		Type:        events.EventType("child/grandchild/micro.done"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"` + grandchildEntityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(grandchildEntityID))
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected nested descendant completion to remain visible downstream")
	}
	if len(emitted) != 0 {
		t.Fatalf("emitted = %#v, want none without subject-link back-propagation", emitted)
	}

	child, found, err := pc.workflowStore.Load(context.Background(), childEntityID)
	if err != nil {
		t.Fatalf("load child instance: %v", err)
	}
	if !found {
		t.Fatal("expected child instance")
	}
	if got := strings.TrimSpace(child.CurrentState); got != "waiting" {
		t.Fatalf("child current_state = %q, want waiting", got)
	}
}

func TestPipelineCoordinatorIntercept_NestedDescendantCompletionAlreadyTargetedToParentStillEmitsRootResult(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
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

	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	const (
		rootEntityID  = "11111111-1111-1111-1111-111111111111"
		childFlowPath = "child/9c38251c-4fba-4a18-9afc-774ede7cc866"
	)
	childRowID := FlowInstanceEntityID(childFlowPath)
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "idle",
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      childRowID,
		SubjectID:       rootEntityID,
		StorageRef:      childFlowPath,
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Metadata: map[string]any{
			"entity_id":        childRowID,
			"flow_path":        childFlowPath,
			"subject_id":       rootEntityID,
			"parent_entity_id": rootEntityID,
		},
	}); err != nil {
		t.Fatalf("seed child instance: %v", err)
	}
	if consume, handled := pc.workflowNodeInterceptPolicy("child/grandchild/micro.done", (events.Event{
		Type: events.EventType("child/grandchild/micro.done"),
	}).WithEntityID(childRowID)); !handled {
		t.Fatalf("workflowNodeInterceptPolicy handled = %v, consume = %v, want handled", handled, consume)
	}

	passThrough, emitted, err := pc.Intercept(context.Background(), (events.Event{
		ID:          "evt-nested-done-parent-targeted",
		Type:        events.EventType("child/grandchild/micro.done"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"` + childRowID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(childRowID))
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected nested descendant completion to remain visible downstream")
	}
	if len(emitted) != 1 || string(emitted[0].Type) != "pipeline.complete" {
		t.Fatalf("emitted = %#v, want [pipeline.complete]", emitted)
	}
	if got := emitted[0].EntityID(); got != childRowID {
		t.Fatalf("emitted entity_id = %q, want child target %q", got, childRowID)
	}
}

func TestPipelineCoordinatorIntercept_NestedDescendantCompletionInsideOuterSQLTxStillEmitsRootResult(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
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

	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	const (
		rootEntityID  = "11111111-1111-1111-1111-111111111111"
		childFlowPath = "child/9c38251c-4fba-4a18-9afc-774ede7cc866"
	)
	childRowID := FlowInstanceEntityID(childFlowPath)
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "idle",
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      childRowID,
		SubjectID:       rootEntityID,
		StorageRef:      childFlowPath,
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Metadata: map[string]any{
			"entity_id":        childRowID,
			"flow_path":        childFlowPath,
			"subject_id":       rootEntityID,
			"parent_entity_id": rootEntityID,
		},
	}); err != nil {
		t.Fatalf("seed child instance: %v", err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	ctx := WithPipelineSQLTxContext(context.Background(), tx)

	passThrough, emitted, err := pc.Intercept(ctx, (events.Event{
		ID:          "evt-nested-done-tx",
		Type:        events.EventType("child/grandchild/micro.done"),
		SourceAgent: "cataloge2e",
		Payload:     []byte(`{"entity_id":"` + childRowID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(childRowID))
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected nested descendant completion to remain visible downstream")
	}
	if len(emitted) != 1 || string(emitted[0].Type) != "pipeline.complete" {
		t.Fatalf("emitted = %#v, want [pipeline.complete]", emitted)
	}
	if got := emitted[0].EntityID(); got != childRowID {
		t.Fatalf("emitted entity_id = %q, want child target %q", got, childRowID)
	}
}
