package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type recordingSchedulePersistence struct {
	schedules    []Schedule
	cancels      []Schedule
	releases     []Schedule
	upsertTx     []bool
	cancelTx     []bool
	claimTx      []bool
	cancelExacts int
	cancelOwned  int
	cancelErr    error
}

func (s *recordingSchedulePersistence) UpsertSchedule(ctx context.Context, sc Schedule) error {
	s.schedules = append(s.schedules, sc)
	_, txActive := PipelineSQLTxFromContext(ctx)
	s.upsertTx = append(s.upsertTx, txActive)
	return nil
}

func (s *recordingSchedulePersistence) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}

func (s *recordingSchedulePersistence) ClaimSchedule(ctx context.Context, _ Schedule) (bool, error) {
	_, txActive := PipelineSQLTxFromContext(ctx)
	s.claimTx = append(s.claimTx, txActive)
	return true, nil
}

func (s *recordingSchedulePersistence) ReleaseSchedule(_ context.Context, sc Schedule) error {
	s.releases = append(s.releases, sc)
	return nil
}

func (*recordingSchedulePersistence) ReleaseScheduleClaims(context.Context) error {
	return nil
}

func (s *recordingSchedulePersistence) CancelScheduleExact(ctx context.Context, sc Schedule) error {
	s.cancelExacts++
	s.cancels = append(s.cancels, sc)
	_, txActive := PipelineSQLTxFromContext(ctx)
	s.cancelTx = append(s.cancelTx, txActive)
	return nil
}

func (s *recordingSchedulePersistence) CancelScheduleExactTerminal(ctx context.Context, sc Schedule) error {
	s.cancelOwned++
	s.cancels = append(s.cancels, sc)
	_, txActive := PipelineSQLTxFromContext(ctx)
	s.cancelTx = append(s.cancelTx, txActive)
	return s.cancelErr
}

func (s *recordingSchedulePersistence) MarkScheduleFiredExact(context.Context, Schedule) error {
	return nil
}

func (*recordingSchedulePersistence) CompleteScheduleFireExact(context.Context, Schedule) error {
	return nil
}

func newTimerLifecycleCoordinator(bus Bus, db *sql.DB, module WorkflowModule, store SchedulePersistence) *PipelineCoordinator {
	opts := PipelineCoordinatorOptions{
		Module:                  module,
		EventReceiptsCapability: eventReceiptsCapabilityStub{enabled: true}.resolve,
	}
	if store != nil {
		opts.TimerScheduler = NewScheduler()
		opts.TimerScheduleStore = store
	}
	return NewPipelineCoordinatorWithOptions(bus, db, opts)
}

func testActivePipelineSQLTxContext(t *testing.T, db *sql.DB, ctx context.Context) context.Context {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return WithPipelineSQLTxContext(ctx, tx)
}

func TestExecuteNodeHandlerPlan_EventTimerStartOnRegistersSchedule(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier5-flow-lifecycle", "test-timer-fire")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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

	ctx := testPipelineCoordinatorRunContext(t, pc)
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	evt := eventtest.WithEntityID(eventtest.Projection(uuid.NewString(),
		events.EventType("timer.scheduled"),
		"cataloge2e", "", []byte(`{"entity_id":"ent-001"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		"ent-001")

	seedPipelineNodeDeliveryAuthority(t, db, evt, "test-node")

	txctx := testActivePipelineSQLTxContext(t, db, ctx)
	if handled := pc.executeNodeHandlerPlan(txctx, "test-node", evt); !handled {
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
	if len(store.upsertTx) != 1 || !store.upsertTx[0] {
		t.Fatalf("schedule upsert tx-active flags = %#v, want [true]", store.upsertTx)
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
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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

	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	evt := eventtest.WithEntityID(eventtest.Projection(uuid.NewString(),
		events.EventType("timer.scheduled"),
		"cataloge2e", "", []byte(`{"entity_id":"ent-001"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		"ent-001")

	seedPipelineNodeDeliveryAuthority(t, db, evt, "test-node")

	_, handled := pc.interceptPolicy("timer.scheduled", evt)
	if !handled {
		t.Fatal("expected timer.scheduled to be interceptable")
	}
	passThrough, emitted, err := pc.Intercept(testPipelineCoordinatorRunContext(t, pc), evt)
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

func TestRegisterWorkflowTimerSchedule_PostCommitClaimDropsPipelineTransaction(t *testing.T) {
	assertWorkflowTimerSchedulePostCommitClaimDropsPipelineTransaction(t, func(pc *PipelineCoordinator, ctx context.Context, sc Schedule) {
		pc.registerWorkflowTimerSchedule(ctx, sc)
	})
}

func TestPersistWorkflowTimerSchedule_PostCommitClaimDropsPipelineTransaction(t *testing.T) {
	assertWorkflowTimerSchedulePostCommitClaimDropsPipelineTransaction(t, func(pc *PipelineCoordinator, ctx context.Context, sc Schedule) {
		pc.persistWorkflowTimerSchedule(ctx, sc)
	})
}

func assertWorkflowTimerSchedulePostCommitClaimDropsPipelineTransaction(t *testing.T, schedule func(*PipelineCoordinator, context.Context, Schedule)) {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := &recordingSchedulePersistence{}
	scheduler := NewScheduler()
	defer scheduler.Stop()
	pc := &PipelineCoordinator{
		timerScheduleStore: store,
		timerScheduler:     scheduler,
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	committed := false
	t.Cleanup(func() {
		if !committed {
			_ = tx.Rollback()
		}
	})
	actions := make([]func(), 0, 1)
	ctx := WithPipelineSQLTxContext(withPipelinePostCommitActions(context.Background(), &actions), tx)
	sc := Schedule{
		AgentID:   "owner",
		EventType: "timer.review",
		Mode:      "once",
		At:        time.Now().Add(time.Hour),
		EntityID:  "ent-1",
		TaskID:    "timer-1",
	}

	schedule(pc, ctx, sc)
	if len(store.upsertTx) != 1 || !store.upsertTx[0] {
		t.Fatalf("schedule upsert tx-active flags = %#v, want [true]", store.upsertTx)
	}
	if len(store.claimTx) != 0 {
		t.Fatalf("claim tx-active flags before flush = %#v, want none", store.claimTx)
	}
	if len(actions) != 1 {
		t.Fatalf("post-commit actions = %d, want 1", len(actions))
	}
	if got := len(scheduler.tasks); got != 0 {
		t.Fatalf("scheduler tasks before flush = %d, want 0", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	committed = true
	flushPipelinePostCommitActions(actions)
	if len(store.claimTx) != 1 || store.claimTx[0] {
		t.Fatalf("claim tx-active flags after flush = %#v, want [false]", store.claimTx)
	}
	if got := len(scheduler.tasks); got != 1 {
		t.Fatalf("scheduler tasks after flush = %d, want 1", got)
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

	pc.persistWorkflowTimerCancellation(testPipelineCoordinatorRunContext(t, pc), sc)

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

	pc.persistWorkflowTimerCancellation(testPipelineCoordinatorRunContext(t, pc), sc)

	if got := store.cancelOwned; got != 1 {
		t.Fatalf("cancel owned calls = %d, want 1", got)
	}
	if got := len(scheduler.tasks); got != 0 {
		t.Fatalf("scheduler tasks after partial failure = %d, want 0", got)
	}
}

func TestCancelWorkflowTimerSchedule_StillCancelsSchedulerWhenClaimReleaseFailsAfterPersist(t *testing.T) {
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

	pc.cancelWorkflowTimerSchedule(testPipelineCoordinatorRunContext(t, pc), sc)

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
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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
		Module:                  module,
		EventReceiptsCapability: eventReceiptsCapabilityStub{enabled: true}.resolve,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	ctx := testPipelineCoordinatorRunContext(t, pc)
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	evt := eventtest.WithEntityID(eventtest.Projection(uuid.NewString(),
		events.EventType("child/task.done"),
		"cataloge2e", "", []byte(`{"entity_id":"ent-001"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		"ent-001")

	seedPipelineNodeDeliveryAuthority(t, db, evt, "listener")

	if handled := pc.executeNodeHandlerPlan(testPipelineCoordinatorRunContext(t, pc), "dispatcher", evt); handled {
		t.Fatal("dispatcher should not handle child/task.done")
	}
	instance, ok, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), "ent-001")
	if err != nil {
		t.Fatalf("load workflow instance after wrong node execution: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after wrong node execution")
	}
	if got := instance.CurrentState; got != "waiting" {
		t.Fatalf("state after wrong node execution = %q, want waiting", got)
	}

	if handled := pc.executeNodeHandlerPlan(testPipelineCoordinatorRunContext(t, pc), "listener", evt); !handled {
		t.Fatal("listener should handle child/task.done")
	}
	instance, ok, err = pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), "ent-001")
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
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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

	ctx := testPipelineCoordinatorRunContext(t, pc)
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
		Metadata:        map[string]any{"expected_count": 5},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	start := time.Now().UTC()
	evt := eventtest.WithEntityID(eventtest.Projection(uuid.NewString(),
		events.EventType("item.arrived"),
		"cataloge2e", "", []byte(`{"entity_id":"ent-001","item_id":"a"}`), 0, "", "", events.EventEnvelope{}, start),
		"ent-001")

	seedPipelineNodeDeliveryAuthority(t, db, evt, "test-node")

	txctx := testActivePipelineSQLTxContext(t, db, ctx)
	if handled := pc.executeNodeHandlerPlan(txctx, "test-node", evt); !handled {
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
	if len(store.upsertTx) != 1 || !store.upsertTx[0] {
		t.Fatalf("schedule upsert tx-active flags = %#v, want [true]", store.upsertTx)
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

func TestReconcileAccumulationTimeoutScheduleUsesMatchedHandlerEventKey(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", "test-accumulate-timeout")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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
	start := time.Now().UTC()
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			ExpectedFrom: "entity.expected_count",
			Completion:   runtimecontracts.ParseAccumulateCompletion("timeout"),
			TimeoutMS:    5000,
		},
	}
	stateBuckets := map[string]any{
		"test-node": map[string]any{
			"handler_accumulators": map[string]any{
				timeridentity.NewAccumulatorBucketRef("test-node", "item.arrived").Key(): map[string]any{
					"started_at": start.Format(time.RFC3339Nano),
				},
			},
		},
	}
	evt := eventtest.WithFlowInstance(eventtest.WithEntityID(eventtest.Projection("evt-item-a",
		events.EventType("component-scaffold/a/item.arrived"),
		"cataloge2e", "", []byte(`{"entity_id":"ent-001","item_id":"a"}`), 0, "", "", events.EventEnvelope{}, start),
		"ent-001"),
		"component-scaffold/a")

	if err := pc.reconcileAccumulationTimeoutSchedule(context.Background(), "ent-001", "test-node", handler, evt, "item.arrived", stateBuckets, true); err != nil {
		t.Fatalf("reconcileAccumulationTimeoutSchedule: %v", err)
	}
	if len(store.schedules) != 1 {
		t.Fatalf("registered schedules = %d, want 1", len(store.schedules))
	}
	got := store.schedules[0]
	wantHandle := timeridentity.AccumulationTimeoutHandle(timeridentity.NewAccumulatorBucketRef("test-node", "item.arrived"))
	if got.TaskID != wantHandle.TaskID() {
		t.Fatalf("scheduled task_id = %q, want %q", got.TaskID, wantHandle.TaskID())
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
		t.Fatalf("scheduled payload bucket = %#v", handle.Bucket)
	}
	if _, ok := findAccumulationTimeoutHandlerForBucket(semanticview.Wrap(bundle), handle.Bucket); !ok {
		t.Fatalf("timeout handler did not resolve for scheduled bucket %#v", handle.Bucket)
	}
}

func TestExecuteNodeHandlerPlan_AccumulateTimeoutCancelsScheduleOnTimeout(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier2-accumulation", "test-accumulate-timeout")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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

	ctx := testPipelineCoordinatorRunContext(t, pc)
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
		Metadata:        map[string]any{"expected_count": 5},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	evt := eventtest.WithEntityID(eventtest.Projection(uuid.NewString(),
		events.EventType("item.arrived"),
		"cataloge2e", "", []byte(`{"entity_id":"ent-001","item_id":"a"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		"ent-001")

	seedPipelineNodeDeliveryAuthority(t, db, evt, "test-node")
	if handled := pc.executeNodeHandlerPlan(ctx, "test-node", evt); !handled {
		t.Fatal("expected item.arrived handler to be handled")
	}

	timeoutEvt := eventtest.WithEntityID(eventtest.Projection(uuid.NewString(),
		events.EventType("accumulate.timeout"),
		runtimeWorkflowID, "", mustJSON(map[string]any{
			"entity_id": "ent-001",
			"timer_handle": map[string]any{
				"kind": "accumulation_timeout",
				"bucket": map[string]any{
					"node_id":    "test-node",
					"event_type": "item.arrived",
				},
			},
		}), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		"ent-001")

	seedPipelineNodeDeliveryAuthority(t, db, timeoutEvt, "test-node")
	txctx := testActivePipelineSQLTxContext(t, db, ctx)
	if handled := pc.executeNodeHandlerPlan(txctx, "test-node", timeoutEvt); !handled {
		t.Fatal("expected accumulate.timeout handler to be handled")
	}
	if len(store.cancels) != 1 {
		t.Fatalf("cancelled schedules = %d, want 1", len(store.cancels))
	}
	if store.cancelOwned != 1 {
		t.Fatalf("CancelScheduleExactTerminal calls = %d, want 1", store.cancelOwned)
	}
	if len(store.cancelTx) == 0 || !store.cancelTx[len(store.cancelTx)-1] {
		t.Fatalf("schedule cancel tx-active flags = %#v, want final true", store.cancelTx)
	}
	if got := store.cancels[0].TaskID; got != timeridentity.AccumulationTimeoutHandle(timeridentity.NewAccumulatorBucketRef("test-node", "item.arrived")).TaskID() {
		t.Fatalf("cancelled task_id = %q", got)
	}
}

func TestExecuteNodeHandlerPlan_PreservesRootStateForChildFlowTransitions(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-pin-wiring")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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
		Module:                  module,
		EventReceiptsCapability: eventReceiptsCapabilityStub{enabled: true}.resolve,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "ready",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	trigger := eventtest.WithEntityID(eventtest.Projection(uuid.NewString(),
		events.EventType("work.requested"),
		"cataloge2e", "", []byte(`{"entity_id":"ent-001"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		"ent-001")

	seedPipelineNodeDeliveryAuthority(t, db, trigger, "child-worker")

	if handled := pc.executeNodeHandlerPlan(testPipelineCoordinatorRunContext(t, pc), "child-worker", trigger); !handled {
		t.Fatal("child-worker should handle work.requested through the input-pin alias")
	}
	instance, ok, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), "ent-001")
	if err != nil {
		t.Fatalf("load workflow instance after child-worker execution: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after child-worker execution")
	}
	if got := instance.CurrentState; got != "ready" {
		t.Fatalf("root state after child-worker execution = %q, want ready", got)
	}

	listenerCtx := withPipelineFlowScope(testPipelineCoordinatorRunContext(t, pc), "child")
	completion := eventtest.WithEntityID(eventtest.Projection(uuid.NewString(),
		events.EventType("child/work.completed"),
		"cataloge2e", "", []byte(`{"entity_id":"ent-001"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		"ent-001")

	seedPipelineNodeDeliveryAuthority(t, db, completion, "parent-listener")
	handler, ok := pc.SemanticSource().NodeEventHandler("parent-listener", "child/work.completed")
	if !ok {
		t.Fatal("parent-listener handler missing for child/work.completed")
	}
	result, err := pc.executeNodeContractHandler(withPipelineFlowScope(testPipelineCoordinatorRunContext(t, pc), "child"), "parent-listener", handler, workflowTriggerContext{
		Event: completion,
		State: pc.currentWorkflowState(withPipelineFlowScope(testPipelineCoordinatorRunContext(t, pc), ""), "ent-001"),
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
	instance, ok, err = pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), "ent-001")
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
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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
		Module:                  module,
		EventReceiptsCapability: eventReceiptsCapabilityStub{enabled: true}.resolve,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      "ent-001",
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "ready",
		Metadata:        map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	completion := eventtest.WithEntityID(eventtest.Projection(uuid.NewString(),
		events.EventType("child/work.completed"),
		"cataloge2e", "", []byte(`{"entity_id":"ent-001"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		"ent-001")

	seedPipelineNodeDeliveryAuthority(t, db, completion, "parent-listener")
	passThrough, emitted, err := pc.Intercept(testPipelineCoordinatorRunContext(t, pc), completion)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected child/work.completed to remain visible downstream")
	}
	if len(emitted) != 1 || string(emitted[0].Type()) != "job.done" {
		t.Fatalf("emitted = %#v, want [job.done]", emitted)
	}
}

func TestPipelineCoordinatorIntercept_NestedDescendantCompletionDoesNotEmitChildContinuation(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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
		Module:                  module,
		EventReceiptsCapability: eventReceiptsCapabilityStub{enabled: true}.resolve,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	childEntityID := FlowInstanceEntityID("child/inst-1")
	grandchildEntityID := FlowInstanceEntityID("child/grandchild/inst-1")
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      childEntityID,
		StorageRef:      "child/inst-1",
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Metadata: map[string]any{
			"entity_id":        childEntityID,
			"flow_path":        "child/inst-1",
			"parent_entity_id": rootEntityID,
		},
	}); err != nil {
		t.Fatalf("seed child instance: %v", err)
	}
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      grandchildEntityID,
		StorageRef:      "child/grandchild/inst-1",
		WorkflowName:    "grandchild",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "finished",
		Metadata: map[string]any{
			"entity_id":        grandchildEntityID,
			"flow_path":        "child/grandchild/inst-1",
			"parent_entity_id": childEntityID,
		},
	}); err != nil {
		t.Fatalf("seed grandchild instance: %v", err)
	}

	completion := eventtest.WithEntityID((eventtest.Projection(uuid.NewString(),
		events.EventType("child/grandchild/micro.done"),
		"cataloge2e", "", []byte(`{"entity_id":"`+grandchildEntityID+`"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())), grandchildEntityID)

	seedPipelineNodeDeliveryAuthority(t, db, completion, "root-collector")
	passThrough, emitted, err := pc.Intercept(testPipelineCoordinatorRunContext(t, pc), completion)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected nested descendant completion to remain visible downstream")
	}
	if len(emitted) != 0 {
		t.Fatalf("emitted = %#v, want none without subject-link back-propagation", emitted)
	}

	child, found, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), childEntityID)
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
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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
		Module:                  module,
		EventReceiptsCapability: eventReceiptsCapabilityStub{enabled: true}.resolve,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	const (
		rootEntityID  = "11111111-1111-1111-1111-111111111111"
		childFlowPath = "child/9c38251c-4fba-4a18-9afc-774ede7cc866"
	)
	childRowID := FlowInstanceEntityID(childFlowPath)
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "idle",
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      childRowID,
		StorageRef:      childFlowPath,
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Metadata: map[string]any{
			"entity_id":        childRowID,
			"flow_path":        childFlowPath,
			"parent_entity_id": rootEntityID,
		},
	}); err != nil {
		t.Fatalf("seed child instance: %v", err)
	}
	if consume, handled := pc.workflowNodeInterceptPolicy("child/grandchild/micro.done", eventtest.WithEntityID((eventtest.Projection("", events.EventType("child/grandchild/micro.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})), childRowID)); !handled {
		t.Fatalf("workflowNodeInterceptPolicy handled = %v, consume = %v, want handled", handled, consume)
	}

	completion := eventtest.WithEntityID((eventtest.Projection(uuid.NewString(),
		events.EventType("child/grandchild/micro.done"),
		"cataloge2e", "", []byte(`{"entity_id":"`+childRowID+`"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())), childRowID)

	seedPipelineNodeDeliveryAuthority(t, db, completion, "root-collector")
	passThrough, emitted, err := pc.Intercept(testPipelineCoordinatorRunContext(t, pc), completion)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected nested descendant completion to remain visible downstream")
	}
	if len(emitted) != 1 || string(emitted[0].Type()) != "pipeline.complete" {
		t.Fatalf("emitted = %#v, want [pipeline.complete]", emitted)
	}
	if got := emitted[0].EntityID(); got != childRowID {
		t.Fatalf("emitted entity_id = %q, want child target %q", got, childRowID)
	}
}

func TestPipelineCoordinatorIntercept_NestedDescendantCompletionInsideOuterSQLTxStillEmitsRootResult(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
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
		Module:                  module,
		EventReceiptsCapability: eventReceiptsCapabilityStub{enabled: true}.resolve,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	const (
		rootEntityID  = "11111111-1111-1111-1111-111111111111"
		childFlowPath = "child/9c38251c-4fba-4a18-9afc-774ede7cc866"
	)
	childRowID := FlowInstanceEntityID(childFlowPath)
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "idle",
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      childRowID,
		StorageRef:      childFlowPath,
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Metadata: map[string]any{
			"entity_id":        childRowID,
			"flow_path":        childFlowPath,
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
	ctx := WithPipelineSQLTxContext(testPipelineCoordinatorRunContext(t, pc), tx)

	completion := eventtest.WithEntityID((eventtest.Projection(uuid.NewString(),
		events.EventType("child/grandchild/micro.done"),
		"cataloge2e", "", []byte(`{"entity_id":"`+childRowID+`"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())), childRowID)

	seedPipelineNodeDeliveryAuthority(t, db, completion, "root-collector")
	passThrough, emitted, err := pc.Intercept(ctx, completion)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected nested descendant completion to remain visible downstream")
	}
	if len(emitted) != 1 || string(emitted[0].Type()) != "pipeline.complete" {
		t.Fatalf("emitted = %#v, want [pipeline.complete]", emitted)
	}
	if got := emitted[0].EntityID(); got != childRowID {
		t.Fatalf("emitted entity_id = %q, want child target %q", got, childRowID)
	}
}
