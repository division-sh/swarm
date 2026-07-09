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
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
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

func stageTimerLifecycleBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			StageDeclarations: runtimecontracts.FlowStageDeclarations{
				Declared: true,
				Entries: []runtimecontracts.FlowStageDeclaration{
					{ID: "awaiting_review", Initial: true},
					{ID: "expired", Terminal: true},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:           "stage-timer-test",
			Version:        "1.0.0",
			InitialStage:   "awaiting_review",
			TerminalStages: []string{"expired"},
			Timers: []runtimecontracts.WorkflowTimerContract{
				{
					ID:         "awaiting_review.review.sla_escalated",
					Stage:      "awaiting_review",
					Event:      "review.sla_escalated",
					Owner:      "runtime",
					StageOwned: true,
					Delay:      "48h",
					StartOn:    "state:awaiting_review",
				},
				{
					ID:         "awaiting_review.expired",
					Stage:      "awaiting_review",
					Event:      runtimecontracts.WorkflowStageTimerInternalEvent,
					Owner:      "runtime",
					StageOwned: true,
					AdvancesTo: "expired",
					Delay:      "72h",
					StartOn:    "state:awaiting_review",
				},
			},
		},
	}
}

func stageTimerTemplateLifecycleBundle() *runtimecontracts.WorkflowContractBundle {
	review := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Path:  "review",
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"sla_hours": {Value: 2},
		}},
	}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{review},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": &review,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"review": {Mode: "template"},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:         "stage-timer-test",
			Version:      "1.0.0",
			InitialStage: "awaiting_review",
			FlowInitial: map[string]string{
				"review": "awaiting_review",
			},
			FlowTerminal: map[string][]string{
				"review": {"expired"},
			},
			FlowPrefix: map[string]string{
				"review": "review",
			},
			Timers: []runtimecontracts.WorkflowTimerContract{
				{
					ID:         "review.awaiting_review.expired",
					Stage:      "awaiting_review",
					Event:      runtimecontracts.WorkflowStageTimerInternalEvent,
					Owner:      "runtime",
					FlowID:     "review",
					StageOwned: true,
					AdvancesTo: "expired",
					Delay:      "{{sla_hours}}h",
					StartOn:    "state:awaiting_review",
				},
			},
		},
	}
}

func assertWorkflowTimerState(t *testing.T, instance WorkflowInstance, timerID string, wantFired bool) {
	t.Helper()
	for _, timer := range instance.TimerState {
		if strings.TrimSpace(timer.TimerID) != timerID {
			continue
		}
		if timer.Fired != wantFired {
			t.Fatalf("timer %s fired = %v, want %v in %#v", timerID, timer.Fired, wantFired, instance.TimerState)
		}
		return
	}
	t.Fatalf("timer %s missing from %#v", timerID, instance.TimerState)
}

func workflowTimerLifecycleLogContains(logs []RuntimeLogEntry, action string) bool {
	for _, log := range logs {
		if strings.TrimSpace(log.Action) == action {
			return true
		}
	}
	return false
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

func TestWorkflowTimerFireAtSnapshotsPolicyDelayAtStart(t *testing.T) {
	now := time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC)
	timer := runtimecontracts.WorkflowTimerContract{
		ID:    "sla_timeout",
		Owner: "support-node",
		Event: "timer.sla_timeout",
		Delay: "{{sla_timeout_hours}}h",
	}

	fireAt, ok := workflowTimerFireAt(timer, now, map[string]any{
		"sla_timeout_hours": 2,
	})
	if !ok {
		t.Fatal("expected policy-backed timer delay to render")
	}
	if want := now.Add(2 * time.Hour); !fireAt.Equal(want) {
		t.Fatalf("fireAt = %s, want %s", fireAt, want)
	}

	schedule := workflowTimerSchedule(timer, "ent-001", "support", fireAt, map[string]any{
		"sla_timeout_hours": 8,
	})
	if !schedule.At.Equal(fireAt) {
		t.Fatalf("schedule At = %s, want persisted fireAt %s", schedule.At, fireAt)
	}
}

func TestWorkflowTimerFireAtSupportsCanonicalDayDelay(t *testing.T) {
	now := time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC)
	timer := runtimecontracts.WorkflowTimerContract{
		ID:    "sla_timeout",
		Owner: "support-node",
		Event: "timer.sla_timeout",
		Delay: "7d",
	}

	fireAt, ok := workflowTimerFireAt(timer, now, nil)
	if !ok {
		t.Fatal("expected canonical day delay to render")
	}
	if want := now.Add(7 * 24 * time.Hour); !fireAt.Equal(want) {
		t.Fatalf("fireAt = %s, want %s", fireAt, want)
	}
}

func TestWorkflowTimerFireAtSupportsPolicyRenderedDayDelay(t *testing.T) {
	now := time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC)
	timer := runtimecontracts.WorkflowTimerContract{
		ID:    "sla_timeout",
		Owner: "support-node",
		Event: "timer.sla_timeout",
		Delay: "{{sla_timeout_days}}d",
	}

	fireAt, ok := workflowTimerFireAt(timer, now, map[string]any{
		"sla_timeout_days": 3,
	})
	if !ok {
		t.Fatal("expected policy-backed day delay to render")
	}
	if want := now.Add(3 * 24 * time.Hour); !fireAt.Equal(want) {
		t.Fatalf("fireAt = %s, want %s", fireAt, want)
	}
}

func TestHandleWorkflowStageTimerFireMarksFiredAndAdvancesWithSQLiteStore(t *testing.T) {
	runID := uuid.NewString()
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	if _, err := db.Exec(`INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	source := semanticview.Wrap(stageTimerLifecycleBundle())
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:           bus,
		module:        &pipelineFixtureWorkflowModule{source: source},
		workflowStore: store,
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	now := time.Now().UTC().Round(time.Microsecond)

	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      "ent-emit",
		StorageRef:      "ent-emit",
		WorkflowName:    "stage-timer-test",
		WorkflowVersion: "1.0.0",
		CurrentState:    "awaiting_review",
		TimerState: []WorkflowTimerState{{
			TimerID:   "awaiting_review.review.sla_escalated",
			EventType: "review.sla_escalated",
			CreatedAt: now.Add(-3 * time.Hour),
			FiresAt:   now.Add(-2 * time.Hour),
			StartedBy: "state:awaiting_review",
		}},
	}); err != nil {
		t.Fatalf("seed emit workflow instance: %v", err)
	}
	emitEvt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("review.sla_escalated"),
		"runtime",
		"awaiting_review.review.sla_escalated",
		[]byte(`{"entity_id":"ent-emit"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-emit"),
		now,
	)
	recognized, fired, err := pc.handleWorkflowStageTimerFire(ctx, emitEvt)
	if err != nil {
		t.Fatalf("handle emit stage timer: %v", err)
	}
	if !recognized || !fired {
		t.Fatal("emit stage timer was not recognized")
	}
	emitInstance, ok, err := store.Load(ctx, "ent-emit")
	if err != nil || !ok {
		t.Fatalf("load emit instance ok=%v err=%v", ok, err)
	}
	if got := emitInstance.CurrentState; got != "awaiting_review" {
		t.Fatalf("emit timer state = %q, want awaiting_review", got)
	}
	assertWorkflowTimerState(t, emitInstance, "awaiting_review.review.sla_escalated", true)

	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      "ent-advance",
		StorageRef:      "ent-advance",
		WorkflowName:    "stage-timer-test",
		WorkflowVersion: "1.0.0",
		CurrentState:    "awaiting_review",
		TimerState: []WorkflowTimerState{{
			TimerID:   "awaiting_review.expired",
			EventType: runtimecontracts.WorkflowStageTimerInternalEvent,
			CreatedAt: now.Add(-3 * time.Hour),
			FiresAt:   now.Add(-2 * time.Hour),
			StartedBy: "state:awaiting_review",
		}},
	}); err != nil {
		t.Fatalf("seed advance workflow instance: %v", err)
	}
	advanceEvt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType(runtimecontracts.WorkflowStageTimerInternalEvent),
		"runtime",
		"awaiting_review.expired",
		[]byte(`{"entity_id":"ent-advance"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-advance"),
		now,
	)
	recognized, fired, err = pc.handleWorkflowStageTimerFire(ctx, advanceEvt)
	if err != nil {
		t.Fatalf("handle advance stage timer: %v", err)
	}
	if !recognized || !fired {
		t.Fatal("advance stage timer was not recognized")
	}
	advanceInstance, ok, err := store.Load(ctx, "ent-advance")
	if err != nil || !ok {
		t.Fatalf("load advance instance ok=%v err=%v", ok, err)
	}
	if got := advanceInstance.CurrentState; got != "expired" {
		t.Fatalf("advance timer state = %q, want expired", got)
	}
	assertWorkflowTimerState(t, advanceInstance, "awaiting_review.expired", true)

	if !workflowTimerLifecycleLogContains(bus.runtimeLogEntries(), "workflow_timer_fired_late") {
		t.Fatalf("runtime logs = %#v, want late-fire diagnostic", bus.runtimeLogEntries())
	}

	recognized, fired, err = pc.handleWorkflowStageTimerFire(ctx, emitEvt)
	if err != nil {
		t.Fatalf("handle duplicate emit stage timer: %v", err)
	}
	if !recognized || fired {
		t.Fatalf("duplicate emit stage timer recognized=%v fired=%v, want recognized stale no-op", recognized, fired)
	}
}

func TestHandleWorkflowStageTimerFireDeactivatesTerminalTemplateFlow(t *testing.T) {
	runID := uuid.NewString()
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	source := semanticview.Wrap(stageTimerTemplateLifecycleBundle())
	var deactivated FlowInstanceDeactivationRequest
	pc := &PipelineCoordinator{
		module:        &pipelineFixtureWorkflowModule{source: source},
		workflowStore: store,
		instanceDeactivator: func(_ context.Context, req FlowInstanceDeactivationRequest) error {
			deactivated = req
			return nil
		},
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	now := time.Now().UTC().Round(time.Microsecond)
	const flowPath = "review/inst-1"
	entityID := FlowInstanceEntityID(flowPath)

	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      flowPath,
		WorkflowName:    "review",
		WorkflowVersion: "1.0.0",
		CurrentState:    "awaiting_review",
		Metadata: map[string]any{
			"entity_id":   entityID,
			"instance_id": "inst-1",
			"flow_path":   flowPath,
		},
		TimerState: []WorkflowTimerState{{
			TimerID:   "review.awaiting_review.expired",
			EventType: runtimecontracts.WorkflowStageTimerInternalEvent,
			CreatedAt: now.Add(-3 * time.Hour),
			FiresAt:   now.Add(-2 * time.Hour),
			StartedBy: "state:awaiting_review",
		}},
	}); err != nil {
		t.Fatalf("seed template workflow instance: %v", err)
	}
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType(runtimecontracts.WorkflowStageTimerInternalEvent),
		"runtime",
		"review.awaiting_review.expired",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		now,
	)

	recognized, fired, err := pc.handleWorkflowStageTimerFire(ctx, evt)
	if err != nil {
		t.Fatalf("handle advance stage timer: %v", err)
	}
	if !recognized || !fired {
		t.Fatalf("recognized=%v fired=%v, want terminal timer fire", recognized, fired)
	}
	if deactivated.FinalState != "expired" {
		t.Fatalf("deactivated final state = %q, want expired; req=%#v", deactivated.FinalState, deactivated)
	}
	if deactivated.Instance.InstancePath != flowPath {
		t.Fatalf("deactivated instance path = %q, want %q", deactivated.Instance.InstancePath, flowPath)
	}
}

func TestPipelineEngineStateRepoSaveStateArmsInitialStageTimersWithSQLiteStore(t *testing.T) {
	runID := uuid.NewString()
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	schedules := &recordingSchedulePersistence{}
	source := semanticview.Wrap(stageTimerLifecycleBundle())
	pc := &PipelineCoordinator{
		module:             &pipelineFixtureWorkflowModule{source: source},
		workflowStore:      store,
		timerScheduleStore: schedules,
	}
	repo := pipelineEngineStateRepo{coordinator: pc}
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	entityID := identity.NormalizeEntityID("33333333-3333-3333-3333-333333333333")

	if err := repo.SaveState(ctx, entityID, testEngineStateMutation(nil, nil, nil)); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	instance, ok, err := store.Load(ctx, entityID.String())
	if err != nil || !ok {
		t.Fatalf("load materialized instance ok=%v err=%v", ok, err)
	}
	if got := instance.CurrentState; got != "awaiting_review" {
		t.Fatalf("materialized state = %q, want awaiting_review", got)
	}
	for _, id := range []string{"awaiting_review.review.sla_escalated", "awaiting_review.expired"} {
		assertWorkflowTimerState(t, instance, id, false)
	}
	if got := len(schedules.schedules); got != 2 {
		t.Fatalf("persisted schedules = %d, want 2: %#v", got, schedules.schedules)
	}
}

func TestWorkflowTimerRecurringSpecNormalizesCanonicalDayDelay(t *testing.T) {
	timer := runtimecontracts.WorkflowTimerContract{
		ID:        "daily_report",
		Owner:     "runtime",
		Event:     "timer.daily_report",
		Delay:     "7d",
		Recurring: true,
	}

	got, ok := workflowTimerRecurringSpec(timer, nil)
	if !ok {
		t.Fatal("expected canonical day delay recurring spec")
	}
	if want := "@every 168h0m0s"; got != want {
		t.Fatalf("workflowTimerRecurringSpec = %q, want %q", got, want)
	}
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

	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("timer.scheduled"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

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

	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("timer.scheduled"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

	seedPipelineNodeDeliveryAuthority(t, db, evt, "test-node")

	_, handled := pc.interceptPolicy(context.Background(), "timer.scheduled", evt)
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
	assertWorkflowTimerSchedulePostCommitClaimDropsPipelineTransaction(t, func(pc *PipelineCoordinator, ctx context.Context, sc Schedule) error {
		pc.registerWorkflowTimerSchedule(ctx, sc)
		return nil
	})
}

func TestPersistWorkflowTimerSchedule_PostCommitClaimDropsPipelineTransaction(t *testing.T) {
	assertWorkflowTimerSchedulePostCommitClaimDropsPipelineTransaction(t, func(pc *PipelineCoordinator, ctx context.Context, sc Schedule) error {
		return pc.persistWorkflowTimerSchedule(ctx, sc)
	})
}

func assertWorkflowTimerSchedulePostCommitClaimDropsPipelineTransaction(t *testing.T, schedule func(*PipelineCoordinator, context.Context, Schedule) error) {
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

	if err := schedule(pc, ctx, sc); err != nil {
		t.Fatalf("schedule: %v", err)
	}
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

	if err := pc.persistWorkflowTimerCancellation(testPipelineCoordinatorRunContext(t, pc), sc); err != nil {
		t.Fatalf("persistWorkflowTimerCancellation: %v", err)
	}

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

	if err := pc.persistWorkflowTimerCancellation(testPipelineCoordinatorRunContext(t, pc), sc); err != nil {
		t.Fatalf("persistWorkflowTimerCancellation: %v", err)
	}

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

	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("child/task.done"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

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
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("item.arrived"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001","item_id":"a"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		start,
	)

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
	evt := eventtest.RootIngress(
		"evt-item-a",
		events.EventType("component-scaffold/a/item.arrived"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001","item_id":"a"}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"), "component-scaffold/a"),
		start,
	)

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

func TestReconcileAccumulationTimeoutScheduleUsesFanInPinDerivedWindow(t *testing.T) {
	bundle := templatefanin.LoadBundle(t, templatefanin.Options{})
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
			Into:       "operating_reports",
			From:       "payload",
			Completion: runtimecontracts.ParseAccumulateCompletion("timeout"),
			TimeoutMS:  5000,
		},
	}
	wantBucket := timeridentity.NewAccumulatorWindowBucketRef(templatefanin.ReceiverNodeID, templatefanin.ReceiverEvent, "2026-Q1")
	stateBuckets := map[string]any{
		templatefanin.ReceiverNodeID: map[string]any{
			"handler_accumulators": map[string]any{
				wantBucket.Key(): map[string]any{
					"started_at": start.Format(time.RFC3339Nano),
				},
			},
		},
	}
	evt := eventtest.RootIngress(
		"evt-operating-2026-q1",
		events.EventType(templatefanin.ReceiverEvent),
		"cataloge2e",
		"",
		[]byte(`{"portfolio_id":"portfolio/default","report_id":"report-1","period_id":"2026-Q1","operating_id":"opco-a","revenue":42}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, templatefanin.ReceiverFlowInstance), templatefanin.ReceiverFlowInstance),
		start,
	)

	ctx := withPipelineFlowScope(testPipelineCoordinatorRunContext(t, pc), templatefanin.ReceiverFlowID)
	if err := pc.reconcileAccumulationTimeoutSchedule(ctx, templatefanin.ReceiverFlowInstance, templatefanin.ReceiverNodeID, handler, evt, templatefanin.ReceiverEvent, stateBuckets, true); err != nil {
		t.Fatalf("reconcileAccumulationTimeoutSchedule: %v", err)
	}
	if len(store.schedules) != 1 {
		t.Fatalf("registered schedules = %d, want 1 for pin-derived window bucket %s", len(store.schedules), wantBucket.Key())
	}
	got := store.schedules[0]
	wantHandle := timeridentity.AccumulationTimeoutHandle(wantBucket)
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
	if handle.Bucket.Key() != wantBucket.Key() {
		t.Fatalf("scheduled payload bucket = %#v, want %s", handle.Bucket, wantBucket.Key())
	}

	timeoutEvt := eventtest.RootIngress(
		"evt-timeout-2026-q1",
		events.EventType("accumulate.timeout"),
		runtimeWorkflowID,
		"",
		mustJSON(map[string]any{
			"entity_id":     templatefanin.ReceiverFlowInstance,
			"timer_handle":  handle.PayloadMetadata()["timer_handle"],
			"timeout_ms":    5000,
			"flow_instance": templatefanin.ReceiverFlowInstance,
		}),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, templatefanin.ReceiverFlowInstance), templatefanin.ReceiverFlowInstance),
		start.Add(5*time.Second),
	)
	if err := pc.reconcileAccumulationTimeoutSchedule(ctx, templatefanin.ReceiverFlowInstance, templatefanin.ReceiverNodeID, handler, timeoutEvt, templatefanin.ReceiverEvent, stateBuckets, false); err != nil {
		t.Fatalf("reconcileAccumulationTimeoutSchedule timeout cancel: %v", err)
	}
	if len(store.cancels) != 1 {
		t.Fatalf("cancelled schedules = %d, want 1 for pin-derived window bucket %s", len(store.cancels), wantBucket.Key())
	}
	if got := store.cancels[0].TaskID; got != wantHandle.TaskID() {
		t.Fatalf("cancelled task_id = %q, want %q", got, wantHandle.TaskID())
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

	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("item.arrived"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001","item_id":"a"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

	seedPipelineNodeDeliveryAuthority(t, db, evt, "test-node")
	if handled := pc.executeNodeHandlerPlan(ctx, "test-node", evt); !handled {
		t.Fatal("expected item.arrived handler to be handled")
	}

	timeoutEvt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("accumulate.timeout"),
		runtimeWorkflowID,
		"",
		mustJSON(map[string]any{
			"entity_id": "ent-001",
			"timer_handle": map[string]any{
				"kind": "accumulation_timeout",
				"bucket": map[string]any{
					"node_id":    "test-node",
					"event_type": "item.arrived",
				},
			},
		}),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

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

	trigger := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("work.requested"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

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
	completion := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("child/work.completed"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

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

	completion := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("child/work.completed"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

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

	completion := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("child/grandchild/micro.done"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+grandchildEntityID+`"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, grandchildEntityID),
		time.Now().UTC(),
	)

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
	if consume, handled := pc.workflowNodeInterceptPolicy(context.Background(), "child/grandchild/micro.done", eventtest.RootIngress(
		"",
		events.EventType("child/grandchild/micro.done"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, childRowID),
		time.Time{},
	)); !handled {
		t.Fatalf("workflowNodeInterceptPolicy handled = %v, consume = %v, want handled", handled, consume)
	}

	completion := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("child/grandchild/micro.done"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+childRowID+`"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, childRowID),
		time.Now().UTC(),
	)

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

	completion := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("child/grandchild/micro.done"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+childRowID+`"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, childRowID),
		time.Now().UTC(),
	)

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
