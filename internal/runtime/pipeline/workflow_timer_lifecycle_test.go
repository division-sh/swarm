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
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type recordingSchedulePersistence struct {
	schedules     []Schedule
	cancels       []Schedule
	releases      []Schedule
	upsertTx      []bool
	cancelTx      []bool
	claimTx       []bool
	claimCalls    int
	claimFailures int
	claimErr      error
	cancelExacts  int
	cancelOwned   int
	cancelErr     error
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
	s.claimCalls++
	if s.claimFailures > 0 {
		s.claimFailures--
		return false, s.claimErr
	}
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

func newTimerLifecycleCoordinator(t *testing.T, bus Bus, db *sql.DB, module WorkflowModule, store SchedulePersistence) *PipelineCoordinator {
	t.Helper()
	opts := PipelineCoordinatorOptions{
		Module: module,
	}
	if store != nil {
		opts.TimerScheduler = NewSchedulerWithWorkOwner(pipelineTestWorkOwner(t))
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
		Module: module,
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

	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("child/task.done"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

	configurePipelineTestDeliveryOwner(t, pc)
	route := seedPipelineNodeDeliveryAuthority(t, db, evt, "listener")
	deliveryCtx := withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), route)

	if handled := pc.executeNodeHandlerPlan(deliveryCtx, "dispatcher", evt); handled {
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

	if handled := pc.executeNodeHandlerPlan(deliveryCtx, "listener", evt); !handled {
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
		Module: module,
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

	trigger := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("work.requested"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

	configurePipelineTestDeliveryOwner(t, pc)
	triggerRoute := seedPipelineNodeDeliveryAuthority(t, db, trigger, "child-worker")

	if handled := pc.executeNodeHandlerPlan(withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), triggerRoute), "child-worker", trigger); !handled {
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
	completion := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("work.completed"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

	completionRoute := seedPipelineNodeDeliveryAuthority(t, db, completion, "parent-listener")
	handler, ok := pc.SemanticSource().NodeEventHandler("parent-listener", "work.completed")
	if !ok {
		t.Fatal("parent-listener handler missing for root-local work.completed")
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

	if handled := pc.executeNodeHandlerPlan(withWorkflowNodeDeliveryRoute(listenerCtx, completionRoute), "parent-listener", completion); !handled {
		t.Fatal("parent-listener should clear inherited child flow scope and handle root-local work.completed")
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
		Module: module,
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

	completion := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("work.completed"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"ent-001"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-001"),
		time.Now().UTC(),
	)

	configurePipelineTestDeliveryOwner(t, pc)
	route := seedPipelineNodeDeliveryAuthority(t, db, completion, "parent-listener")
	passThrough, emitted, err := pc.Intercept(withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), route), completion)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected root-local work.completed to remain visible downstream")
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
		Module: module,
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

	completion := eventtest.RunCreatingRootIngress(
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

	configurePipelineTestDeliveryOwner(t, pc)
	route := seedPipelineNodeDeliveryAuthority(t, db, completion, "root-collector")
	passThrough, emitted, err := pc.Intercept(withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), route), completion)
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

func TestPipelineCoordinatorIntercept_NestedPackageRootConnectDoesNotAuthorizeRootResult(t *testing.T) {
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
	if consume, handled, err := pc.workflowNodeInterceptPolicy(testAuthorActivityContext(t, context.Background()), "child/grandchild/micro.done", eventtest.RunCreatingRootIngress(
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
	)); err != nil || !handled {
		t.Fatalf("workflowNodeInterceptPolicy handled = %v, consume = %v, err = %v, want handled", handled, consume, err)
	}

	completion := eventtest.RunCreatingRootIngress(
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

	configurePipelineTestDeliveryOwner(t, pc)
	route := seedPipelineNodeDeliveryAuthority(t, db, completion, "root-collector")
	passThrough, emitted, err := pc.Intercept(withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), route), completion)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected nested descendant completion to remain visible downstream")
	}
	if len(emitted) != 0 {
		t.Fatalf("emitted = %#v, want none from an unauthorized repository-root handler", emitted)
	}
}

func TestPipelineCoordinatorIntercept_NestedPackageRootConnectInsideOuterSQLTxDoesNotAuthorizeRootResult(t *testing.T) {
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
	tx, err := db.BeginTx(testAuthorActivityContext(t, context.Background()), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	ctx := WithPipelineSQLTxContext(testPipelineCoordinatorRunContext(t, pc), tx)

	completion := eventtest.RunCreatingRootIngress(
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

	configurePipelineTestDeliveryOwner(t, pc)
	route := seedPipelineNodeDeliveryAuthority(t, db, completion, "root-collector")
	ctx, err = runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectPostgres)
	if err != nil {
		t.Fatalf("begin nested completion author activity story: %v", err)
	}
	passThrough, emitted, err := pc.Intercept(withWorkflowNodeDeliveryRoute(ctx, route), completion)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected nested descendant completion to remain visible downstream")
	}
	if len(emitted) != 0 {
		t.Fatalf("emitted = %#v, want none from an unauthorized repository-root handler", emitted)
	}
}
