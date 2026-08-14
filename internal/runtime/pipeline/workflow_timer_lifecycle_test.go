package pipeline

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type recordingGenericScheduleWakeupOwner struct {
	activationIDs []string
}

func (s *recordingGenericScheduleWakeupOwner) ReconcileWakeupWithRecovery(_ context.Context, activationID string) (bool, error) {
	s.activationIDs = append(s.activationIDs, activationID)
	return false, nil
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
	const entityID = "11111111-1111-1111-1111-111111111111"
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

	pc := newPostgresPipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	ctx := testPipelineCoordinatorRunContext(t, pc)
	runID := runtimecorrelation.RunIDFromContext(ctx)
	if err := pc.workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      runID,
		StorageRef:      runID,
		EntityID:        entityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Fields:          map[string]any{"entity_id": entityID, "flow_path": runID, "instance_id": runID},
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), runID)
	envelope = events.EnvelopeForSourceRoute(envelope, events.RouteIdentity{
		FlowID: "child", FlowInstance: "child", EntityID: entityID,
	})
	envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{
		FlowID: bundle.WorkflowName(), FlowInstance: runID, EntityID: entityID,
	})
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("child/task.done"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		testPipelineRunID,
		"",
		envelope,
		time.Now().UTC(),
	)

	configurePipelineTestDeliveryOwner(t, pc)
	route := workflowNodeStampedConnectRouteForHandlerEvent(t, pc.SemanticSource(), "task.done", "listener")
	route.Target = events.MustExistingEntityTarget(evt.TargetRoute())
	route = seedPipelineNodeDeliveryRouteAuthority(t, db, evt, route)
	deliveryCtx := withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), route)

	if handled := pc.executeNodeHandlerPlan(deliveryCtx, "dispatcher", evt); handled {
		t.Fatal("dispatcher should not handle child/task.done")
	}
	instance, ok, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), testWorkflowInstanceRoute(runID))
	if err != nil {
		t.Fatalf("load workflow instance after wrong node execution: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after wrong node execution")
	}
	if got := instance.CurrentState; got != "waiting" {
		t.Fatalf("state after wrong node execution = %q, want waiting", got)
	}

	if handled, err := pc.executeNodeHandlerPlanResult(deliveryCtx, "listener", evt); err != nil || !handled {
		t.Fatalf("listener should handle child/task.done: handled=%v err=%v", handled, err)
	}
	instance, ok, err = pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), testWorkflowInstanceRoute(runID))
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
	const entityID = "11111111-1111-1111-1111-111111111111"
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

	pc := newPostgresPipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      testPipelineRunID,
		StorageRef:      testPipelineRunID,
		EntityID:        entityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "ready",
		Fields:          map[string]any{"entity_id": entityID, "flow_path": testPipelineRunID, "instance_id": testPipelineRunID},
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	triggerEnvelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "child")
	triggerEnvelope = events.EnvelopeForTargetRoute(triggerEnvelope, events.RouteIdentity{
		FlowID: "child", FlowInstance: "child", EntityID: entityID,
	})
	trigger := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("work.requested"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		testPipelineRunID,
		"",
		triggerEnvelope,
		time.Now().UTC(),
	)

	configurePipelineTestDeliveryOwner(t, pc)
	triggerRoute := seedPipelineNodeDeliveryAuthority(t, db, trigger, "child-worker")

	if handled, err := pc.executeNodeHandlerPlanResult(withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), triggerRoute), "child-worker", trigger); err != nil || !handled {
		t.Fatalf("child-worker should handle work.requested through the input-pin alias: handled=%v err=%v", handled, err)
	}
	instance, ok, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), testWorkflowInstanceRoute(testPipelineRunID))
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
	completionEnvelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "child")
	completionEnvelope = events.EnvelopeForSourceRoute(completionEnvelope, events.RouteIdentity{
		FlowID: "child", FlowInstance: "child", EntityID: entityID,
	})
	completionEnvelope = events.EnvelopeForTargetRoute(completionEnvelope, events.RouteIdentity{
		FlowID: bundle.WorkflowName(), FlowInstance: testPipelineRunID, EntityID: entityID,
	})
	completion := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("work.completed"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		testPipelineRunID,
		"",
		completionEnvelope,
		time.Now().UTC(),
	)

	completionRoute := seedPipelineNodeDeliveryAuthority(t, db, completion, "parent-listener")
	handler, ok := pc.SemanticSource().NodeEventHandler("parent-listener", "work.completed")
	if !ok {
		t.Fatal("parent-listener handler missing for root-local work.completed")
	}
	result, err := pc.executeNodeContractHandler(withPipelineFlowScope(testPipelineCoordinatorRunContext(t, pc), "child"), "parent-listener", handler, workflowTriggerContext{
		Event: completion,
		State: mustCurrentWorkflowState(t, pc, withPipelineFlowScope(testPipelineCoordinatorRunContext(t, pc), ""), testWorkflowInstanceRoute(testPipelineRunID), entityID),
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if result.Outcome == nil || len(result.Outcome.Emits) != 0 {
		t.Fatalf("handler emits = %#v, want no retired dead output", result.Outcome)
	}

	if handled := pc.executeNodeHandlerPlan(withWorkflowNodeDeliveryRoute(listenerCtx, completionRoute), "parent-listener", completion); !handled {
		t.Fatal("parent-listener should clear inherited child flow scope and handle root-local work.completed")
	}
	instance, ok, err = pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), testWorkflowInstanceRoute(testPipelineRunID))
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
	const entityID = "11111111-1111-1111-1111-111111111111"
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
	pc := newPostgresPipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      testPipelineRunID,
		StorageRef:      testPipelineRunID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "ready",
		Fields:          map[string]any{"entity_id": entityID, "flow_path": testPipelineRunID, "instance_id": testPipelineRunID},
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	completion := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("work.completed"),
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForTargetRoute(
			handlerTestWorkflowEnvelope(bundle.WorkflowName(), testPipelineRunID, entityID),
			events.RouteIdentity{FlowID: bundle.WorkflowName(), FlowInstance: testPipelineRunID, EntityID: entityID},
		),
		time.Now().UTC(),
	)

	configurePipelineTestDeliveryOwner(t, pc)
	route := seedPipelineNodeDeliveryAuthority(t, db, completion, "parent-listener")
	passThrough, emitted, _, err := pc.Intercept(withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), route), completion)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passThrough {
		t.Fatal("expected root-local work.completed to remain visible downstream")
	}
	if len(emitted) != 0 {
		t.Fatalf("emitted = %#v, want no retired dead output", emitted)
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
	pc := newPostgresPipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if pc == nil {
		t.Fatal("expected coordinator")
	}
	const rootEntityID = "11111111-1111-1111-1111-111111111111"
	childEntityID := FlowInstanceEntityID("child/inst-1")
	grandchildEntityID := FlowInstanceEntityID("child/grandchild/inst-1")
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      rootEntityID,
		StorageRef:      bundle.WorkflowName(),
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "idle",
		Fields:          map[string]any{"entity_id": rootEntityID, "flow_path": bundle.WorkflowName(), "instance_id": bundle.WorkflowName()},
	})); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      childEntityID,
		StorageRef:      "child/inst-1",
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Fields: map[string]any{
			"entity_id":        childEntityID,
			"flow_path":        "child/inst-1",
			"parent_entity_id": rootEntityID,
		},
	})); err != nil {
		t.Fatalf("seed child instance: %v", err)
	}
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      grandchildEntityID,
		StorageRef:      "child/grandchild/inst-1",
		WorkflowName:    "grandchild",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "finished",
		Fields: map[string]any{
			"entity_id":        grandchildEntityID,
			"flow_path":        "child/grandchild/inst-1",
			"parent_entity_id": childEntityID,
		},
	})); err != nil {
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
		events.EnvelopeForTargetRoute(
			events.EnvelopeForEntityID(events.EventEnvelope{}, grandchildEntityID),
			events.RouteIdentity{FlowID: bundle.WorkflowName(), FlowInstance: bundle.WorkflowName(), EntityID: rootEntityID},
		),
		time.Now().UTC(),
	)

	configurePipelineTestDeliveryOwner(t, pc)
	route := seedPipelineNodeDeliveryAuthority(t, db, completion, "root-collector")
	passThrough, emitted, _, err := pc.Intercept(withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), route), completion)
	if err == nil || !strings.Contains(err.Error(), "stamped connect claim") {
		t.Fatalf("Intercept error = %v, want stamped connect claim", err)
	}
	if passThrough || len(emitted) != 0 {
		t.Fatalf("failed delivery result = passThrough:%v emitted:%#v, want no output", passThrough, emitted)
	}

	child, found, err := pc.workflowStore.Load(testPipelineCoordinatorRunContext(t, pc), testWorkflowInstanceRoute("child/inst-1"))
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
	pc := newPostgresPipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{
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
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "idle",
	})); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      childRowID,
		StorageRef:      childFlowPath,
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Fields: map[string]any{
			"entity_id":        childRowID,
			"flow_path":        childFlowPath,
			"parent_entity_id": rootEntityID,
		},
	})); err != nil {
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
		events.EnvelopeForTargetRoute(
			events.EnvelopeForEntityID(events.EventEnvelope{}, childRowID),
			events.RouteIdentity{FlowID: bundle.WorkflowName(), FlowInstance: bundle.WorkflowName(), EntityID: rootEntityID},
		),
		time.Time{},
	)); err != nil || handled || consume {
		t.Fatalf("workflowNodeInterceptPolicy handled = %v, consume = %v, err = %v, want no unstamped match", handled, consume, err)
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
		events.EnvelopeForTargetRoute(
			events.EnvelopeForEntityID(events.EventEnvelope{}, childRowID),
			events.RouteIdentity{FlowID: bundle.WorkflowName(), FlowInstance: bundle.WorkflowName(), EntityID: rootEntityID},
		),
		time.Now().UTC(),
	)

	configurePipelineTestDeliveryOwner(t, pc)
	route := seedPipelineNodeDeliveryAuthority(t, db, completion, "root-collector")
	passThrough, emitted, _, err := pc.Intercept(withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), route), completion)
	if err == nil || !strings.Contains(err.Error(), "stamped connect claim") {
		t.Fatalf("Intercept error = %v, want stamped connect claim", err)
	}
	if passThrough || len(emitted) != 0 {
		t.Fatalf("failed delivery result = passThrough:%v emitted:%#v, want no output", passThrough, emitted)
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
	pc := newPostgresPipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{
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
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      rootEntityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "idle",
	})); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      childRowID,
		StorageRef:      childFlowPath,
		WorkflowName:    "child",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "waiting",
		Fields: map[string]any{
			"entity_id":        childRowID,
			"flow_path":        childFlowPath,
			"parent_entity_id": rootEntityID,
		},
	})); err != nil {
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
		events.EnvelopeForTargetRoute(
			events.EnvelopeForEntityID(events.EventEnvelope{}, childRowID),
			events.RouteIdentity{FlowID: bundle.WorkflowName(), FlowInstance: bundle.WorkflowName(), EntityID: rootEntityID},
		),
		time.Now().UTC(),
	)

	configurePipelineTestDeliveryOwner(t, pc)
	route := seedPipelineNodeDeliveryAuthority(t, db, completion, "root-collector")
	ctx, err = authoractivityfixture.Begin(ctx, tx, authoractivityfixture.DialectPostgres)
	if err != nil {
		t.Fatalf("begin nested completion author activity story: %v", err)
	}
	passThrough, emitted, _, err := pc.Intercept(withWorkflowNodeDeliveryRoute(ctx, route), completion)
	if err == nil || !strings.Contains(err.Error(), "stamped connect claim") {
		t.Fatalf("Intercept error = %v, want stamped connect claim", err)
	}
	if passThrough || len(emitted) != 0 {
		t.Fatalf("failed delivery result = passThrough:%v emitted:%#v, want no output", passThrough, emitted)
	}
}
