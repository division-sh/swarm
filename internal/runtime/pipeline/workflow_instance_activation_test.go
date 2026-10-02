package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type workflowInitialMaterializationTestOwner struct {
	effects   int
	emissions int
}

func (o *workflowInitialMaterializationTestOwner) PrepareWorkflowLifecycleMutation(_ context.Context, _ runtimeflowidentity.RunScopedFlowInstance, _ *WorkflowInstance, _ []runtimeworkflowlifecycle.Effect, _ bool) (PreparedWorkflowLifecycleMutation, error) {
	return PreparedWorkflowLifecycleMutation{Emissions: make([]runtimeengine.EmitIntent, o.emissions)}, nil
}

func TestWorkflowInitialLifecyclePreparationRejectsUnownedEmissions(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	store.lifecycleOwner = &workflowInitialMaterializationTestOwner{emissions: 1}
	ctx := withLiveWorkflowInitialEntry(sqliteExactOnceRunContext(t, db))
	instance := WorkflowInstance{
		InstanceID: "inst-1", StorageRef: "review/inst-1", WorkflowName: "review",
		WorkflowVersion: "1.0.0", CurrentState: "pending",
		Fields:     map[string]any{},
		EntityType: "test_entity",
	}
	if _, _, _, err := store.prepareInitialEntryLifecycle(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef), instance, time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("initial materialization accepted lifecycle emissions outside its atomic commit")
	} else if !strings.Contains(err.Error(), "lifecycle emissions outside its atomic commit") {
		t.Fatalf("initial materialization error = %v", err)
	}
	if _, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef)); err != nil || found {
		t.Fatalf("rejected initial materialization persisted state: found=%v err=%v", found, err)
	}
}

func (o *workflowInitialMaterializationTestOwner) FinalizeWorkflowLifecycleMutation(_ context.Context, _ CommittedWorkflowLifecycleMutation) error {
	o.effects++
	return nil
}

func (*workflowInitialMaterializationTestOwner) ArmInitialEntryTimers(context.Context, runtimeflowidentity.RunScopedFlowInstance) error {
	return nil
}

func (*workflowInitialMaterializationTestOwner) ReconcileInitialEntryTimers(context.Context, runtimeflowidentity.RunScopedFlowInstance) error {
	return nil
}

func (*workflowInitialMaterializationTestOwner) RetireInitialEntryTimerWakeups(context.Context, runtimeflowidentity.RunScopedFlowInstance) error {
	return nil
}

func transitionWorkflowActivationRunForTest(
	t *testing.T,
	store *workflowInstanceStore,
	ctx context.Context,
	runID string,
	state runtimerunlifecycle.State,
) {
	t.Helper()
	if store == nil || store.testRuntimeMutation() == nil {
		t.Fatal("workflow activation run transition requires runtime mutation owner")
	}
	if err := store.testRuntimeMutation().RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		if state == runtimerunlifecycle.StateCancelled {
			_, _, err := store.runLifecycle.MarkTerminalRun(txctx, runtimerunlifecycle.TerminalRequest{
				RunID: runID, State: state, EndedAt: time.Now().UTC(),
			})
			return err
		}
		_, err := store.runLifecycle.TransitionActiveRun(txctx, runtimerunlifecycle.ActiveTransitionRequest{
			RunID: runID, State: state,
		})
		return err
	}); err != nil {
		t.Fatalf("transition workflow activation run %s to %s: %v", runID, state, err)
	}
}

func TestHandlerEmitEnvelope_KeepsLocalEntityAcrossOutputBoundaries(t *testing.T) {
	bundle := loadWorkflowFixtureBundle(t, "test-child-flow-local-events")
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	pc := &PipelineCoordinator{module: module}
	trigger := workflowTriggerContext{
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("child/child.start"),
			"",
			"",
			[]byte(`{"entity_id":"ent-child"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: WorkflowState{
			EntityID: "ent-child",
			Control: runtimeengine.StateControl{
				FlowPath:       "child/inst-1",
				ParentEntityID: "ent-parent",
			},
		},
	}

	internalPayload := pc.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(t, context.Background()), "child"), trigger, "child/child.internal")
	if got := asString(internalPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("internal payload entity_id = %q, want ent-child", got)
	}

	outputPayload := pc.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(t, context.Background()), "child"), trigger, "child/child.done")
	if got := asString(outputPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("output payload entity_id = %q, want ent-child", got)
	}

	pinBundle := loadWorkflowFixtureBundle(t, "test-child-flow-pin-wiring")
	pinModule, err := newPipelineFixtureWorkflowModule(pinBundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule(pin wiring): %v", err)
	}
	pinPC := &PipelineCoordinator{module: pinModule}
	pinPayload := pinPC.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(t, context.Background()), "child"), trigger, "child/work.completed")
	if got := asString(pinPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("pin output payload entity_id = %q, want ent-child", got)
	}
}

func TestHandlerEmitEnvelope_RootFlowOutputUsesLocalEntity(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"scoring/schema.yaml": `name: scoring
pins:
  outputs:
    events:
      - scoring.requested
`,
		"scoring/nodes.yaml": `scoring-node:
  execution_type: system_node
  event_handlers: {}
`,
	})
	pc := &PipelineCoordinator{module: staticSemanticWorkflowModule{source: source}}
	trigger := workflowTriggerContext{
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("vertical.discovered"),
			"",
			"",
			[]byte(`{"entity_id":"ent-root"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-root"),
			time.Time{},
		),

		State: WorkflowState{
			EntityID: "ent-child",
			Control: runtimeengine.StateControl{
				ParentEntityID: "ent-root",
			},
		},
	}

	payload := pc.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(t, context.Background()), "scoring"), trigger, "scoring/scoring.requested")
	if got := asString(payload["entity_id"]); got != "ent-child" {
		t.Fatalf("root flow output payload entity_id = %q, want ent-child", got)
	}
}

func TestTemplateInstanceSystemNodeDeliveryUsesExactLocalHandlerKey(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"operating/entities.yaml": "test_entity: {}\n",
		"operating/schema.yaml": `name: operating
stages:
  initializing: {initial: true}
  ready: {terminal: true}
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"operating/events.yaml": `opco.product_initialization_requested:
  entity_id: string
opco.ceo_ready:
  entity_id: string?
`,
		"operating/nodes.yaml": `lifecycle-orchestrator:
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [opco.ceo_ready]
  event_handlers:
    opco.product_initialization_requested:
      emit: opco.ceo_ready
`,
	})
	entityID := FlowInstanceEntityID("operating/inst-1")
	evt := handlerTestRootIngress(
		uuid.NewString(),
		events.EventType("opco.product_initialization_requested"),
		"",
		"",
		mustJSON(map[string]any{"entity_id": entityID}),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "operating/inst-1"),
		time.Time{},
	)
	evt = eventtest.TargetRouted(evt, events.RouteIdentity{FlowID: "operating", FlowInstance: "operating/inst-1", EntityID: "11111111-1111-1111-1111-111111111111"})

	node := pipelineNode(t, "operating", "lifecycle-orchestrator")
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, node, evt)
	if !resolved.Matched {
		t.Fatal("expected exact local event to resolve to lifecycle-orchestrator handler")
	}
	if got := resolved.HandlerEventKey; got != "opco.product_initialization_requested" {
		t.Fatalf("handler event key = %q, want opco.product_initialization_requested", got)
	}

	_, db, _ := testutil.StartPostgres(t)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		workflowStore:  newPostgresWorkflowInstanceStoreForTest(db),
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module:         staticSemanticWorkflowModule{source: source},
	}
	configurePipelineTestDeliveryOwner(t, pc)
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: "inst-1", StorageRef: "operating/inst-1", EntityID: "11111111-1111-1111-1111-111111111111",
		WorkflowName: "operating", WorkflowVersion: "1.0.0", CurrentState: "initializing", Fields: map[string]any{},
		EntityType: "test_entity",
	})); err != nil {
		t.Fatalf("seed exact selected template owner: %v", err)
	}
	route := seedPipelineNodeDeliveryAuthority(t, db, evt, pipelineNode(t, "operating", "lifecycle-orchestrator"))
	handled, err := pc.executeNodeHandlerPlanResult(withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), route), node, evt)
	if err != nil {
		t.Fatalf("executeNodeHandlerPlanResult: %v", err)
	}
	if !handled {
		t.Fatal("executeNodeHandlerPlanResult handled = false, want true")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type()); got != "operating/opco.ceo_ready" {
		t.Fatalf("published event type = %q, want operating/opco.ceo_ready", got)
	}
}

func loadWorkflowFixtureSource(t *testing.T, fixture string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadWorkflowFixtureBundle(t, fixture))
}

func loadWorkflowFixtureBundle(t *testing.T, fixture string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", fixture)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load fixture bundle: %v", err)
	}
	return bundle
}

func loadWorkflowTempSource(t *testing.T, files map[string]string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadWorkflowTempBundle(t, files))
}

func loadWorkflowTempBundle(t *testing.T, files map[string]string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("load temp bundle: %v", err)
	}
	return bundle
}

type staticSemanticWorkflowModule struct {
	source semanticview.Source
}

func (m staticSemanticWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (staticSemanticWorkflowModule) WorkflowNodes() []WorkflowNode         { return nil }
func (staticSemanticWorkflowModule) GuardRegistry() GuardRegistry          { return nil }
