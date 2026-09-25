package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func commitAccumulatorAppendForTest(ctx context.Context, pc *PipelineCoordinator, route runtimeflowidentity.Route, entityID, flowID, bucketID string, payload map[string]any) error {
	stateRepo := pipelineEngineStateRepo{coordinator: pc}
	address := runtimeengine.StateAddress{
		FlowID:       identity.NormalizeFlowID(flowID),
		FlowInstance: testRunScopedWorkflowRoute(ctx, route),
		EntityID:     identity.NormalizeEntityID(entityID),
	}
	state, ok, err := stateRepo.LoadState(ctx, address)
	if err != nil {
		return err
	}
	if !ok {
		state = runtimeengine.StateSnapshot{EntityID: address.EntityID}
	}
	mutation := runtimeengine.StateMutation{StateCarrier: runtimeengine.NewStateCarrierWithOwners(
		state.Fields, state.Bookkeeping, state.Control, state.Gates, state.StateBuckets,
	)}
	if mutation.StateBuckets == nil {
		mutation.StateBuckets = map[string]map[string]any{}
	}
	bucket := cloneStringAnyMap(mutation.StateBuckets["journal"])
	if bucket == nil {
		bucket = map[string]any{}
	}
	entries, _ := bucket[bucketID].([]any)
	bucket[bucketID] = append(entries, cloneStringAnyMap(payload))
	mutation.StateBuckets["journal"] = bucket
	owner := pipelineEngineMutationOwner{store: pc.workflowStore, state: stateRepo}
	_, err = owner.CommitEngineMutation(ctx, runtimeengine.EngineMutation{Address: address, State: mutation})
	return err
}

func testEngineStateMutation(metadata map[string]any, gates map[string]bool, buckets map[string]map[string]any) runtimeengine.StateMutation {
	return runtimeengine.StateMutation{
		StateCarrier: runtimeengine.NewStateCarrier(metadata, gates, buckets),
	}
}

func testEngineStateAddress(flowID, instancePath, entityID string) runtimeengine.StateAddress {
	return runtimeengine.StateAddress{
		FlowID:       identity.NormalizeFlowID(flowID),
		FlowInstance: testRunScopedWorkflowInstance(instancePath),
		EntityID:     identity.NormalizeEntityID(entityID),
	}
}

func applyMaterializedEngineStateMutationForTest(
	t *testing.T,
	instance *WorkflowInstance,
	mutation runtimeengine.StateMutation,
	allowedFields map[string]struct{},
	source semanticview.Source,
	flowID string,
) {
	t.Helper()
	if instance.EnteredStageAt.IsZero() {
		instance.EnteredStageAt = time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)
	}
	if instance.CurrentState == "" {
		instance.CurrentState = "pending"
	}
	if err := applyEngineStateMutation(instance, mutation, allowedFields, source, flowID); err != nil {
		t.Fatalf("apply materialized engine state mutation: %v", err)
	}
}

func assertEntityStateField(t *testing.T, db *sql.DB, entityID, field string, want any) {
	t.Helper()
	var gotRaw []byte
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT fields -> $3
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, testPipelineRunID, entityID, field).Scan(&gotRaw); err != nil {
		t.Fatalf("load entity_state fields for %s: %v", entityID, err)
	}
	wantRaw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal wanted entity_state field %s: %v", field, err)
	}
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("entity_state.fields[%q] = %s, want %s", field, gotRaw, wantRaw)
	}
}

func TestApplyEngineStateMutationMirrorsDataAccumulationIntoEntityProjection(t *testing.T) {
	instance := &WorkflowInstance{
		Fields:       map[string]any{"research_context": map[string]any{"summary": "done"}},
		StateBuckets: map[string]any{},
		EntityType:   "test_entity",
	}
	mutation := testEngineStateMutation(map[string]any{
		"research_context": map[string]any{"summary": "done"},
	}, nil, nil)
	mutation.StateCarrier.Bookkeeping = map[string]any{
		"last_data_accumulation_event":  "research.completed",
		"last_data_accumulation_source": "research.completed",
	}
	mutation.DataAccumulation = runtimecontracts.WorkflowDataAccumulation{
		Writes: []runtimecontracts.WorkflowDataWrite{
			{TargetField: "research_context", SourceField: "research_context"},
		},
	}
	applyMaterializedEngineStateMutationForTest(t, instance, mutation, map[string]struct{}{"research_context": {}}, nil, "")

	entityProjection, _ := workflowStateBucketObject(*instance, workflowStateBucketEntityProjection)
	got, ok := entityProjection["research_context"].(map[string]any)
	if !ok || got["summary"] != "done" {
		t.Fatalf("entity_projection research_context = %#v", entityProjection["research_context"])
	}
	if got := instance.Bookkeeping["last_data_accumulation_event"]; got != "research.completed" {
		t.Fatalf("last_data_accumulation_event = %#v", got)
	}
}

func TestApplyEngineStateMutationMergesGateDeltasIntoExistingMetadata(t *testing.T) {
	instance := &WorkflowInstance{
		Fields:     map[string]any{},
		Gates:      map[string]bool{"g_a": true, "g_b": true},
		EntityType: "test_entity",
	}
	mutation := testEngineStateMutation(nil, map[string]bool{"g_c": true}, nil)
	mutation.SetGate = "g_c"

	applyMaterializedEngineStateMutationForTest(t, instance, mutation, nil, nil, "")

	gates := instance.Gates
	want := map[string]bool{"g_a": true, "g_b": true, "g_c": true}
	if len(gates) != len(want) {
		t.Fatalf("gates len=%d want %d (%v)", len(gates), len(want), gates)
	}
	for key, value := range want {
		if gates[key] != value {
			t.Fatalf("gate %s=%v want %v (all=%v)", key, gates[key], value, gates)
		}
	}
}

func TestApplyEngineStateMutationScopesChildFlowGates(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Version: "v-test",
			FlowPrefix: map[string]string{
				"child": "child",
			},
		},
	})
	instance := &WorkflowInstance{
		Fields:     map[string]any{},
		EntityType: "test_entity",
	}
	mutation := testEngineStateMutation(nil, map[string]bool{"g_validated": true}, nil)
	mutation.SetGate = "g_validated"

	applyMaterializedEngineStateMutationForTest(t, instance, mutation, nil, source, "child")

	gates := instance.Gates
	if !gates["child/g_validated"] {
		t.Fatalf("scoped gates = %#v, want child/g_validated=true", gates)
	}
	if gates["g_validated"] {
		t.Fatalf("raw unscoped child gate leaked into metadata: %#v", gates)
	}
}

func TestPipelineEngineEvaluatorQueryEntitiesUsesExecutingFlowID(t *testing.T) {

	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": `
name: runtime-test
initial_state: ready
states: [ready]
`,
		"child/schema.yaml": `
name: child
mode: static
initial_state: queued
states: [queued]
`,
		"child/entities.yaml": `
child_entity:
  request_id: text
`,
		"child/events.yaml": `
request.received:
  request_id: text
`,
	})
	pc := newPostgresPipelineCoordinatorForTest(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &pipelineFixtureWorkflowModule{
			source: source,
		},
	})
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      "child/existing",
		EntityID:        entityID,
		WorkflowName:    "child",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Fields: map[string]any{
			"entity_id":  entityID,
			"request_id": "req-existing",
			"flow_path":  "child/existing",
		},
		EntityType: "test_entity",
	})); err != nil {
		t.Fatalf("seed child workflow instance: %v", err)
	}

	eval := pipelineEngineEvaluator{evaluator: pc.expressionEval, coordinator: pc}
	resolution := semanticview.ResolveEventSchema(source, "child", "request.received")
	if !resolution.HasStructural {
		t.Fatal("query fixture requires an admitted payload type")
	}
	ok, err := eval.EvalBool(`query_entities(request_id == payload.request_id).count == 1`, runtimeengine.BaseContext{
		FlowID:  "child",
		Event:   values.Wrap(map[string]any{"run_id": testPipelineRunID, "trigger_event_type": "request.received"}),
		Payload: values.Wrap(map[string]any{"request_id": "req-existing"}),
	}, workflowexpr.ValueExpressionOptions{PayloadType: &resolution.StructuralType})
	if err != nil {
		t.Fatalf("EvalBool query_entities: %v", err)
	}
	if !ok {
		t.Fatal("query_entities did not count the child-flow entity")
	}
}

func TestApplyEngineStateMutationPreservesExistingMetadataOnGateOnlyMutation(t *testing.T) {
	instance := &WorkflowInstance{
		Fields: map[string]any{
			"flow_path": "child/inst-1",
		},
		EntityType: "test_entity",
	}
	mutation := testEngineStateMutation(nil, map[string]bool{"g_ready": true}, nil)
	mutation.SetGate = "g_ready"

	applyMaterializedEngineStateMutationForTest(t, instance, mutation, nil, nil, "")

	if !instance.Gates["g_ready"] {
		t.Fatalf("gates = %#v, want g_ready=true", instance.Gates)
	}
}

func TestApplyEngineStateMutationPreservesTypedControlAcrossAuthoredCollisions(t *testing.T) {
	instance := &WorkflowInstance{
		StorageRef:         "review/inst-1",
		InstanceID:         "inst-1",
		EntityID:           "child-ent",
		WorkflowVersion:    "v1",
		TemplateVersion:    "tv1",
		InstanceKind:       "materialized",
		ParentFlowID:       "operating",
		ParentFlowInstance: "operating/root",
		ParentEntityID:     "parent-ent",
		Fields: map[string]any{
			"parent_flow_id":   "authored-old",
			"parent_entity_id": "authored-parent",
			"business_status":  "old",
		},
		EntityType: "test_entity",
	}
	mutation := testEngineStateMutation(map[string]any{
		"parent_flow_id":       "wrong",
		"parent_flow_instance": "wrong/root",
		"parent_entity_id":     "wrong-parent",
		"business_status":      "new",
	}, nil, nil)

	applyMaterializedEngineStateMutationForTest(t, instance, mutation, nil, nil, "")

	if instance.StorageRef != "review/inst-1" || instance.InstanceID != "inst-1" || instance.EntityID != "child-ent" {
		t.Fatalf("typed identity = %#v, want original identity", instance)
	}
	if instance.ParentFlowID != "operating" || instance.ParentFlowInstance != "operating/root" || instance.ParentEntityID != "parent-ent" {
		t.Fatalf("typed parent route = %q/%q/%q, want original route", instance.ParentFlowID, instance.ParentFlowInstance, instance.ParentEntityID)
	}
	if instance.WorkflowVersion != "v1" || instance.TemplateVersion != "tv1" || instance.InstanceKind != "materialized" {
		t.Fatalf("typed version/kind = %q/%q/%q, want original values", instance.WorkflowVersion, instance.TemplateVersion, instance.InstanceKind)
	}
	for key, want := range map[string]any{
		"parent_flow_id":       "wrong",
		"parent_flow_instance": "wrong/root",
		"parent_entity_id":     "wrong-parent",
	} {
		if got := instance.Fields[key]; got != want {
			t.Fatalf("authored field %s = %#v, want %#v", key, got, want)
		}
	}
	if got := instance.Fields["business_status"]; got != "new" {
		t.Fatalf("business_status = %#v, want new", got)
	}
}

func TestApplyEngineStateMutationDoesNotPromoteAuthoredParentRouteNames(t *testing.T) {
	instance := &WorkflowInstance{
		Fields: map[string]any{
			"business_status": "old",
		},
		EntityType: "test_entity",
	}
	mutation := testEngineStateMutation(map[string]any{
		"business_status":      "new",
		"parent_flow_id":       "root",
		"parent_flow_instance": "root/inst-1",
		"parent_entity_id":     "parent-ent",
	}, nil, nil)

	applyMaterializedEngineStateMutationForTest(t, instance, mutation, nil, nil, "")

	for key, want := range map[string]any{
		"parent_flow_id": "root", "parent_flow_instance": "root/inst-1", "parent_entity_id": "parent-ent",
	} {
		if got := instance.Fields[key]; got != want {
			t.Fatalf("authored field %s = %#v, want %#v", key, got, want)
		}
	}
	if instance.ParentFlowID != "" || instance.ParentFlowInstance != "" || instance.ParentEntityID != "" {
		t.Fatalf("typed parent route = %q/%q/%q, want empty", instance.ParentFlowID, instance.ParentFlowInstance, instance.ParentEntityID)
	}
}

func TestApplyEngineStateMutationKeepsTypedParentRouteIndependent(t *testing.T) {
	instance := &WorkflowInstance{
		ParentFlowID:       "typed-root",
		ParentFlowInstance: "typed-root/inst-1",
		ParentEntityID:     "typed-parent",
		Fields: map[string]any{
			"parent_entity_id": "legacy-parent",
			"business_status":  "old",
		},
		EntityType: "test_entity",
	}
	mutation := testEngineStateMutation(map[string]any{
		"business_status":      "new",
		"parent_flow_id":       "root",
		"parent_flow_instance": "root/inst-1",
		"parent_entity_id":     "wrong-parent",
	}, nil, nil)

	applyMaterializedEngineStateMutationForTest(t, instance, mutation, nil, nil, "")

	if got := instance.Fields["parent_flow_id"]; got != "root" {
		t.Fatalf("authored parent_flow_id = %#v, want root", got)
	}
	if got := instance.Fields["parent_flow_instance"]; got != "root/inst-1" {
		t.Fatalf("authored parent_flow_instance = %#v, want root/inst-1", got)
	}
	if got := instance.Fields["parent_entity_id"]; got != "wrong-parent" {
		t.Fatalf("authored parent_entity_id = %#v, want wrong-parent", got)
	}
	if got := instance.Fields["business_status"]; got != "new" {
		t.Fatalf("business_status = %#v, want new", got)
	}
	if instance.ParentFlowID != "typed-root" || instance.ParentFlowInstance != "typed-root/inst-1" || instance.ParentEntityID != "typed-parent" {
		t.Fatalf("typed parent route = %q/%q/%q, want original", instance.ParentFlowID, instance.ParentFlowInstance, instance.ParentEntityID)
	}
}

func mutationParentRoutePinOutputSource() semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			FlowPath: "child",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "child.done"}},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child.done": {},
		},
		Path: "child",
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{child},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child": &child,
			},
		},
	})
}

type preparedFlowDeactivationTest struct{}

func (*preparedFlowDeactivationTest) Commit() error { return nil }
func (*preparedFlowDeactivationTest) Abort() error  { return nil }

func TestPrepareTerminalFlowInstanceDeactivationIgnoresRootWorkflowEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:         "root",
			InitialStage: "pending",
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"root": {},
		},
	}
	deactivated := false
	pc := newPostgresPipelineCoordinatorForTest(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &pipelineFixtureWorkflowModule{
			source: semanticview.Wrap(bundle),
		},
		InstanceDeactivationPreparer: func(context.Context, FlowInstanceDeactivationRequest) (PreparedFlowInstanceDeactivation, error) {
			deactivated = true
			return &preparedFlowDeactivationTest{}, nil
		},
	})

	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "root",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Fields:          map[string]any{},
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}

	if prepared, err := pc.prepareTerminalFlowInstanceDeactivation(testPipelineCoordinatorRunContext(t, pc), testRunScopedWorkflowInstance("root"), identity.NormalizeEntityID(entityID), "done"); err != nil || prepared != nil {
		t.Fatalf("prepare terminal flow: prepared=%v err=%v", prepared, err)
	}
	if deactivated {
		t.Fatal("expected root workflow entity to skip flow-instance deactivation")
	}
}

func TestPrepareTerminalFlowInstanceDeactivationPassesTerminalState(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	bundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":          "name: root\n",
		"review/schema.yaml":   "name: review\nmode: template\nstages:\n  pending: {initial: true}\n  completed: {terminal: true}\n",
		"review/entities.yaml": "test_entity: {}\n",
	})
	var got FlowInstanceDeactivationRequest
	called := false
	pc := newPostgresPipelineCoordinatorForTest(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &pipelineFixtureWorkflowModule{
			source: semanticview.Wrap(bundle),
		},
		InstanceDeactivationPreparer: func(_ context.Context, req FlowInstanceDeactivationRequest) (PreparedFlowInstanceDeactivation, error) {
			called = true
			got = req
			return &preparedFlowDeactivationTest{}, nil
		},
	})

	const flowPath = "review/inst-1"
	entityID := FlowInstanceEntityID(flowPath)
	const parentEntityID = "22222222-2222-2222-2222-222222222222"
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      flowPath,
		WorkflowName:    "review",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Fields: map[string]any{
			"entity_id":        entityID,
			"instance_id":      "inst-1",
			"flow_path":        flowPath,
			"parent_entity_id": parentEntityID,
		},
		EntityType: "test_entity",
	})); err != nil {
		t.Fatalf("seed template instance: %v", err)
	}

	if prepared, err := pc.prepareTerminalFlowInstanceDeactivation(testPipelineCoordinatorRunContext(t, pc), testRunScopedWorkflowInstance(flowPath), identity.NormalizeEntityID(entityID), "completed"); err != nil || prepared == nil {
		t.Fatalf("prepare terminal flow: prepared=%v err=%v", prepared, err)
	}
	if !called {
		t.Fatal("expected template flow deactivation")
	}
	if got.FinalState != "completed" {
		t.Fatalf("FinalState = %q, want completed", got.FinalState)
	}
	if got.Instance.InstancePath != flowPath {
		t.Fatalf("InstancePath = %q, want %q", got.Instance.InstancePath, flowPath)
	}
}

func TestApplyEngineStateMutationRejectsMissingMaterializedEntryTime(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "empire",
			Version: "7.1.0",
		},
	})
	instance := &WorkflowInstance{}
	mutation := testEngineStateMutation(map[string]any{
		"name": "Test Vertical",
	}, nil, nil)
	mutation.DataAccumulation = runtimecontracts.WorkflowDataAccumulation{
		Writes: []runtimecontracts.WorkflowDataWrite{
			{TargetField: "name", Value: runtimecontracts.LiteralExpression("Test Vertical")},
		},
	}

	err := applyEngineStateMutation(instance, mutation, map[string]struct{}{"name": {}}, source, "scoring")
	if err == nil || !strings.Contains(err.Error(), "materialized entry time") {
		t.Fatalf("applyEngineStateMutation error = %v, want materialized entry time refusal", err)
	}
}

func TestWorkflowStateGatesForScopeLocalizesDeepScope(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")

	got := workflowStateGatesForScope(source, "child/grandchild", map[string]bool{
		"child/grandchild/g_ready": true,
	})

	if !got["child/grandchild/g_ready"] {
		t.Fatalf("scoped gate missing from result: %#v", got)
	}
	if !got["g_ready"] {
		t.Fatalf("local gate alias missing from deep scope result: %#v", got)
	}
}

func TestApplyEngineStateMutationMirrorsAllowedMetadataFieldsWithoutDataAccumulation(t *testing.T) {
	instance := &WorkflowInstance{
		Fields:       map[string]any{"composite_score": 0},
		Gates:        map[string]bool{"g_ready": true},
		StateBuckets: map[string]any{},
		EntityType:   "test_entity",
	}
	mutation := testEngineStateMutation(map[string]any{
		"composite_score": 71,
		"scoring_rubric":  "corpus_rubric",
	}, nil, nil)

	applyMaterializedEngineStateMutationForTest(t, instance, mutation, map[string]struct{}{
		"composite_score": {},
		"scoring_rubric":  {},
	}, nil, "")

	entityProjection, _ := workflowStateBucketObject(*instance, workflowStateBucketEntityProjection)
	if got := entityProjection["composite_score"]; got != 71 {
		t.Fatalf("entity_projection composite_score = %#v, want 71", got)
	}
	if got := entityProjection["scoring_rubric"]; got != "corpus_rubric" {
		t.Fatalf("entity_projection scoring_rubric = %#v", got)
	}
	if !instance.Gates["g_ready"] {
		t.Fatalf("field-only mutation dropped existing gates: %#v", instance.Gates)
	}
}

func TestApplyEngineStateMutationDoesNotCaptureSubjectIDFromMetadata(t *testing.T) {
	instance := &WorkflowInstance{EntityType: "test_entity"}
	mutation := testEngineStateMutation(map[string]any{}, nil, nil)

	applyMaterializedEngineStateMutationForTest(t, instance, mutation, nil, nil, "")

	if got := strings.TrimSpace(asString(instance.Fields["subject_id"])); got != "" {
		t.Fatalf("metadata subject_id = %q, want removed", got)
	}
}

func TestUpdateEntityState_ReturnsWorkflowStoreMutationError(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	pc := &PipelineCoordinator{
		workflowStore: newPostgresWorkflowInstanceStoreForTest(db),
		module: &previewWorkflowModule{
			bundle: &runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{
					Name:    "empire",
					Version: "1.0.0",
				},
			},
		},
	}

	const entityID = "11111111-1111-1111-1111-111111111111"
	ctx := testPipelineRunContextNoSeed(t)
	err := pc.persistWorkflowStateForTest(testWorkflowStateTransitionContext(ctx, testWorkflowInstanceRoute(entityID), entityID, "scoring/vertical.marginal"), testWorkflowInstanceRoute(entityID), entityID, "marginal_review", "scoring/vertical.marginal")
	if err == nil {
		t.Fatal("expected workflow state persistence to fail when workflow store mutate fails")
	}
}

func TestPipelineEngineMutationOwnerRejectsForeignFlowWrite(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := newPostgresWorkflowInstanceStoreForTest(db)
	entityID := "11111111-1111-1111-1111-111111111111"
	if err := store.upsert(testWorkflowStoreRunContext(t, store), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      "flow-a",
		EntityID:        entityID,
		WorkflowName:    "flow-a",
		WorkflowVersion: "1.6.0",
		CurrentState:    "pending",
		Fields:          map[string]any{},
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("upsert flow-a entity: %v", err)
	}

	repo := pipelineEngineStateRepo{
		coordinator: &PipelineCoordinator{
			workflowStore: store,
			module: &previewWorkflowModule{
				bundle: &runtimecontracts.WorkflowContractBundle{
					Semantics: runtimecontracts.WorkflowSemanticView{
						FlowPrefix: map[string]string{
							"flow-a": "flow-a",
							"flow-b": "flow-b",
						},
					},
				},
			},
		},
	}
	ctx := withPipelineFlowScope(testWorkflowStoreRunContext(t, store), "flow-b")
	_, err := (pipelineEngineMutationOwner{store: store, state: repo}).CommitEngineMutation(ctx, runtimeengine.EngineMutation{
		Address: testEngineStateAddress("flow-b", "flow-a", entityID),
		State:   testEngineStateMutation(map[string]any{"note": "bad write"}, nil, nil),
	})
	if err == nil || !strings.Contains(err.Error(), "cross_flow_write_forbidden") {
		t.Fatalf("expected cross_flow_write_forbidden, got %v", err)
	}
}

func TestPipelineEngineMutationOwnerRejectsWrongRunRootAddressBeforeMutationOnBothStores(t *testing.T) {
	const wrongRunID = "88888888-8888-8888-8888-888888888888"
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			source := handlerEntityRequirementExecutionSource()
			coordinator := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
				Module:              staticSemanticWorkflowModule{source: source},
				Persistence:         workflowPersistenceForTest(store),
				PipelineObligations: unavailablePipelineTestObligationOwner{},
			})
			var ctx context.Context
			if backend == "sqlite" {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}
			entityID := eventtest.UUID("wrong-run-root-engine-" + backend)
			address := testEngineStateAddress(".", wrongRunID, entityID)
			mutation := testEngineStateMutation(map[string]any{"marker": "must-not-persist"}, nil, nil)
			mutation.StateCarrier.Control.EntityType = "test_entity"
			mutation.TriggeredAt = time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

			_, err := (pipelineEngineMutationOwner{
				store: store,
				state: pipelineEngineStateRepo{coordinator: coordinator},
			}).CommitEngineMutation(ctx, runtimeengine.EngineMutation{Address: address, State: mutation})
			if err == nil || !strings.Contains(err.Error(), "disagrees with current root coordinate") {
				t.Fatalf("wrong-run root mutation error = %v", err)
			}
			for _, table := range []string{"entity_state", "flow_instances"} {
				var count int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("%s rows after wrong-run root mutation = %d, err=%v", table, count, err)
				}
			}
		})
	}
}

func TestWorkflowEngineMutationRejectsEntityContractDriftOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			source := handlerEntityRequirementExecutionSource()
			coordinator := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
				Module:              staticSemanticWorkflowModule{source: source},
				Persistence:         workflowPersistenceForTest(store),
				PipelineObligations: unavailablePipelineTestObligationOwner{},
			})
			var ctx context.Context
			if backend == "sqlite" {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}
			entityID := eventtest.UUID("entity-contract-drift-" + backend)
			instance := materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: testPipelineRunID, StorageRef: testPipelineRunID, EntityID: entityID,
				WorkflowName: ".", WorkflowVersion: "1", Mode: runtimecontracts.FlowModeStatic,
				CurrentState: "active", Fields: map[string]any{"marker": "unchanged"}, EntityType: "wrong_entity",
			})
			if err := store.upsert(ctx, instance); err != nil {
				t.Fatalf("seed contradictory entity contract: %v", err)
			}
			mutation := testEngineStateMutation(map[string]any{"marker": "mutated"}, nil, nil)
			mutation.NextState = "done"
			mutation.TriggeredAt = time.Date(2026, time.August, 23, 4, 0, 0, 0, time.UTC)
			_, err := (pipelineEngineMutationOwner{
				store: store, state: pipelineEngineStateRepo{coordinator: coordinator},
			}).CommitEngineMutation(ctx, runtimeengine.EngineMutation{
				Address: testEngineStateAddress(".", testPipelineRunID, entityID), State: mutation,
			})
			if err == nil || !strings.Contains(err.Error(), `entity_type "wrong_entity" disagrees with canonical contract "test_entity"`) {
				t.Fatalf("entity contract drift mutation error = %v", err)
			}
			stored, found, loadErr := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, testPipelineRunID))
			if loadErr != nil || !found || stored.EntityType != "wrong_entity" || stored.CurrentState != "active" || stored.Revision != 1 || stored.Fields["marker"] != "unchanged" {
				t.Fatalf("rejected entity contract drift changed state: found=%t err=%v state=%#v", found, loadErr, stored)
			}
		})
	}
}

func TestWorkflowEngineFirstMaterializationRejectsMissingOrContradictoryEntityContractOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, carriedType := range []string{"", "wrong_entity"} {
			label := "missing"
			if carriedType != "" {
				label = "contradictory"
			}
			t.Run(backend+"/"+label, func(t *testing.T) {
				db, store := openHandlerEntityRequirementStore(t, backend)
				source := handlerEntityRequirementExecutionSource()
				coordinator := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
					Module:              staticSemanticWorkflowModule{source: source},
					Persistence:         workflowPersistenceForTest(store),
					PipelineObligations: unavailablePipelineTestObligationOwner{},
				})
				var ctx context.Context
				if backend == "sqlite" {
					ctx = sqliteExactOnceRunContext(t, db)
				} else {
					ctx = testPipelineRunContext(t, db)
				}
				entityID := eventtest.UUID("first-materialization-contract-" + backend + "-" + label)
				mutation := testEngineStateMutation(map[string]any{"marker": "must-not-persist"}, nil, nil)
				mutation.NextState = "active"
				mutation.StateCarrier.Control = runtimeengine.StateControl{
					FlowPath: testPipelineRunID, StorageRef: testPipelineRunID, InstanceID: testPipelineRunID, EntityType: carriedType,
				}
				mutation.TriggeredAt = time.Date(2026, time.August, 23, 4, 5, 0, 0, time.UTC)
				_, err := (pipelineEngineMutationOwner{
					store: store, state: pipelineEngineStateRepo{coordinator: coordinator},
				}).CommitEngineMutation(ctx, runtimeengine.EngineMutation{
					Address: testEngineStateAddress(".", testPipelineRunID, entityID), State: mutation,
				})
				if err == nil || !strings.Contains(err.Error(), "workflow initial materialization carried entity_type") {
					t.Fatalf("%s entity contract materialization error = %v", label, err)
				}
				for _, table := range []string{"entity_state", "flow_instances", "entity_mutations"} {
					var count int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
						t.Fatalf("%s rows after rejected %s contract = %d, err=%v", table, label, count, err)
					}
				}
			})
		}
	}
}

func TestPipelineEngineStateRepoLoadStateMissingEntityDoesNotMaterializeDefaults(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "name: runtime-test\n",
		"review/schema.yaml": `
name: review
mode: static
initial_state: queued
states: [queued]
`,
		"review/entities.yaml": `
review_entity:
  status:
    type: text
    initial: pending
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	repo := pipelineEngineStateRepo{
		coordinator: &PipelineCoordinator{
			workflowStore: newPostgresWorkflowInstanceStoreForTest(db),
			module:        &previewWorkflowModule{bundle: bundle},
		},
	}

	loaded, ok, err := repo.LoadState(testWorkflowStoreRunContext(t, repo.coordinator.workflowStore), testEngineStateAddress("review", "review/inst-missing", FlowInstanceEntityID("review/inst-missing")))
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if ok {
		t.Fatalf("LoadState ok=true for missing entity, loaded fields=%#v", loaded.Fields)
	}
}

func TestPipelineEngineMutationOwnerRoundTripsTypedCarrier(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := newPostgresWorkflowInstanceStoreForTest(db)
	repo := pipelineEngineStateRepo{
		coordinator: &PipelineCoordinator{
			workflowStore: store,
			module:        &pipelineFixtureWorkflowModule{source: testRootEntityContractSource("root", "test_entity")},
		},
	}
	entityID := identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111")
	if err := store.upsert(testWorkflowStoreRunContext(t, store), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      entityID.String(),
		StorageRef:      testPipelineRunID,
		EntityID:        entityID.String(),
		WorkflowName:    ".",
		WorkflowVersion: "1.0.0",
		CurrentState:    "pending",
		Fields:          map[string]any{},
		StateBuckets:    map[string]any{},
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	mutation := testEngineStateMutation(
		map[string]any{"score": 91, "subject_id": "11111111-1111-1111-1111-111111111111"},
		map[string]bool{"ready": true},
		map[string]map[string]any{"evidence": {"count": 2}},
	)

	address := testEngineStateAddress(".", testPipelineRunID, entityID.String())
	if _, err := (pipelineEngineMutationOwner{store: store, state: repo}).CommitEngineMutation(
		testWorkflowStoreRunContext(t, repo.coordinator.workflowStore),
		runtimeengine.EngineMutation{Address: address, State: mutation},
	); err != nil {
		t.Fatalf("CommitEngineMutation: %v", err)
	}
	loaded, ok, err := repo.LoadState(testWorkflowStoreRunContext(t, repo.coordinator.workflowStore), address)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if !ok {
		t.Fatal("expected saved state to load")
	}
	if got := loaded.Fields["score"]; got != int64(91) {
		t.Fatalf("loaded metadata score = %#v, want 91", got)
	}
	if !loaded.Gates["ready"] {
		t.Fatalf("loaded gates = %#v, want ready=true", loaded.Gates)
	}
	if got := loaded.StateBuckets["evidence"]["count"]; got != int64(2) {
		t.Fatalf("loaded state bucket evidence.count = %#v, want 2", got)
	}
}

func TestPipelineEngineStateRepoLoadStateRejectsMalformedPersistedCarrier(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	t.Run("state_buckets", func(t *testing.T) {
		store := newPostgresWorkflowInstanceStoreForTest(db)
		if err := store.upsert(testWorkflowStoreRunContext(t, store), materializedWorkflowInstanceForTest(WorkflowInstance{
			InstanceID:      "22222222-2222-2222-2222-222222222222",
			StorageRef:      "root",
			EntityID:        "22222222-2222-2222-2222-222222222222",
			WorkflowName:    "root",
			WorkflowVersion: "1.0.0",
			CurrentState:    "pending",
			StateBuckets: map[string]any{
				"evidence": "bad",
			},
			EntityType: "test_entity",
		})); err != nil {
			t.Fatalf("upsert malformed state bucket instance: %v", err)
		}
		repo := pipelineEngineStateRepo{coordinator: &PipelineCoordinator{workflowStore: store}}
		_, _, err := repo.LoadState(testWorkflowStoreRunContext(t, repo.coordinator.workflowStore), testEngineStateAddress("root", "root", "22222222-2222-2222-2222-222222222222"))
		if err == nil || !strings.Contains(err.Error(), "invalid workflow state bucket") {
			t.Fatalf("LoadState error = %v, want invalid workflow state bucket", err)
		}
	})
}

func TestAccumulatorAppend_ReturnsWorkflowStoreMutationError(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	pc := &PipelineCoordinator{
		workflowStore: newPostgresWorkflowInstanceStoreForTest(db),
	}

	err := commitAccumulatorAppendForTest(testPipelineRunContextNoSeed(t), pc, testWorkflowInstanceRoute("11111111-1111-1111-1111-111111111111"), "11111111-1111-1111-1111-111111111111", "", "research", map[string]any{"summary": "done"})
	if err == nil {
		t.Fatal("expected accumulator append to fail when workflow store mutate fails")
	}
}

func mustStaticExecutionRoutingSource(route events.RouteIdentity) events.RoutingSource {
	source, err := events.NewStaticFlowRoutingSource(route)
	if err != nil {
		panic(err)
	}
	return source
}

func mustRootExecutionRoutingSource(entityID string) events.RoutingSource {
	source, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		panic(err)
	}
	return source
}

func TestPipelineEngineEvaluator_ExposesAccumulatedScopeForCEL(t *testing.T) {
	eval := pipelineEngineEvaluator{evaluator: newWorkflowExpressionEvaluator()}
	ok, err := eval.EvalBool(
		`accumulated.filter(d, d.score >= 70 && d.tier == 1).size() >= 2`,
		runtimeengine.BaseContext{
			Entity:  values.Wrap(map[string]any{}),
			Payload: values.Wrap(map[string]any{}),
			Policy:  values.Wrap(map[string]any{}),
			Accumulated: values.Wrap(map[string]any{
				"items": []any{
					map[string]any{"dimension": "build_complexity", "score": 74, "tier": 1},
					map[string]any{"dimension": "automation_completeness", "score": 72, "tier": 1},
					map[string]any{"dimension": "retention_architecture", "score": 68, "tier": 3},
				},
				"received_count": 3,
			}),
		},
		workflowexpr.ValueExpressionOptions{},
	)
	if err != nil {
		t.Fatalf("EvalBool error = %v", err)
	}
	if !ok {
		t.Fatal("expected CEL accumulated scope to expose the accumulated item list explicitly")
	}
}

func TestPipelineEngineQueryEntityCountRejectsRawNumericCarrierBeforeStoreLookup(t *testing.T) {
	eval := pipelineEngineEvaluator{}
	_, err := eval.queryEntityCount(workflowExpressionContext{
		Payload: map[string]any{"minimum": json.Number("9007199254740992")},
	}, `score >= payload.minimum`)
	var projection *workflowexpr.CELProjectionError
	if !errors.As(err, &projection) || projection.Path != "$.payload.minimum" {
		t.Fatalf("query_entities hostile operand error = %#v, want exact checked projection", err)
	}
}

func TestWorkflowStateGatesForScopeAddsLocalAliasesForChildFlow(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Version: "v-test",
			FlowPrefix: map[string]string{
				"child": "child",
			},
		},
	})
	got := workflowStateGatesForScope(source, "child", map[string]bool{
		"child/g_validated": true,
	})
	if !got["child/g_validated"] {
		t.Fatalf("scoped key missing from gates view: %#v", got)
	}
	if !got["g_validated"] {
		t.Fatalf("local alias missing from gates view: %#v", got)
	}
}

func TestPipelineEnginePayloadShaper_UsesParentEntityForCrossFlowOutputs(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-child-flow-local-events")
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	shaper := pipelineEnginePayloadShaper{
		coordinator: &PipelineCoordinator{
			module: &previewWorkflowModule{
				bundle: bundle,
			},
		},
	}

	req := runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID("ent-child"),
		Node:     pipelineNode(t, "child", "child-node"),
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("child/child.internal"),
			"",
			"",
			json.RawMessage(`{"entity_id":"ent-child","step":"done"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID("ent-child"),
			StateCarrier: runtimeengine.NewStateCarrier(map[string]any{"flow_path": "child/inst-1", "subject_id": "ent-parent", "parent_entity_id": "ent-parent"}, nil, nil),
		},
	}

	internal, err := shaper.ShapeEmitPayload(testAuthorActivityContext(t, context.Background()), req, "child/child.internal", map[string]any{"step": "done"})
	if err != nil {
		t.Fatalf("ShapeEmitPayload internal: %v", err)
	}
	if _, ok := internal["entity_id"]; ok {
		t.Fatalf("internal emit payload must not carry envelope entity_id: %#v", internal["entity_id"])
	}
	if got := internal["step"]; got != "done" {
		t.Fatalf("internal emit step = %#v, want done", got)
	}

	if _, err := shaper.ShapeEmitPayload(testAuthorActivityContext(t, context.Background()), req, "child/child.done", map[string]any{"step": "done"}); err == nil {
		t.Fatal("expected cross-flow undeclared field to fail closed")
	} else if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("ShapeEmitPayload output error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}
}

func TestPipelineEnginePayloadShaper_RejectsUndeclaredFieldsAcrossCrossFlowOutputBoundary(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-child-flow-local-events")
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	shaper := pipelineEnginePayloadShaper{
		coordinator: &PipelineCoordinator{
			module: &previewWorkflowModule{
				bundle: bundle,
			},
		},
	}

	req := runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID("ent-child"),
		Node:     pipelineNode(t, "child", "child-node"),
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("child/child.internal"),
			"",
			"",
			json.RawMessage(`{"entity_id":"ent-child"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID("ent-child"),
			StateCarrier: runtimeengine.NewStateCarrier(map[string]any{"flow_path": "child/inst-1", "subject_id": "ent-parent", "parent_entity_id": "ent-parent"}, nil, nil),
		},
	}

	_, err := shaper.ShapeEmitPayload(testAuthorActivityContext(t, context.Background()), req, "child/child.done", map[string]any{
		"vertical_id": "ent-child",
		"result":      "accepted",
	})
	if err == nil {
		t.Fatal("expected undeclared output fields to fail closed")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("ShapeEmitPayload output error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}
}

func TestPipelineEnginePayloadShaper_AllowsDeclaredPayloadOnDeclarativeSurface(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-child-flow-local-events")
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	shaper := pipelineEnginePayloadShaper{
		coordinator: &PipelineCoordinator{
			module: &previewWorkflowModule{
				bundle: bundle,
			},
		},
	}

	req := runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID("ent-child"),
		Node:     pipelineNode(t, "child", "child-node"),
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("child/child.internal"),
			"",
			"",
			json.RawMessage(`{"entity_id":"ent-child","step":"done"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID("ent-child"),
			StateCarrier: runtimeengine.NewStateCarrier(map[string]any{"flow_path": "child/inst-1", "subject_id": "ent-parent", "parent_entity_id": "ent-parent"}, nil, nil),
		},
	}

	emitCtx := runtimeengine.WithEmitSurface(testAuthorActivityContext(t, context.Background()), runtimeengine.EmitSurfaceDeclarative)
	payload, err := shaper.ShapeEmitPayload(emitCtx, req, "child/child.done", map[string]any{})
	if err != nil {
		t.Fatalf("ShapeEmitPayload declarative surface: %v", err)
	}
	if len(payload) != 0 {
		t.Fatalf("declarative payload = %#v, want declared business payload only", payload)
	}
}

func TestPipelineEnginePayloadShaper_RejectsMissingRequiredFieldsOnDeclarativeSurface(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n  outputs:\n    events: [parent.result]\n",
		"events.yaml": "parent.trigger:\n  entity_id: string\nparent.result:\n  entity_id: string\n",

		"child/schema.yaml": "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.internal]\n",
		"child/events.yaml": "child.start:\n  entity_id: string\nchild.internal:\n  entity_id: string\n  step: string\n",
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	shaper := pipelineEnginePayloadShaper{
		coordinator: &PipelineCoordinator{
			module: &previewWorkflowModule{
				bundle: bundle,
			},
		},
	}

	req := runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID("ent-child"),
		Node:     pipelineNode(t, "child", "child-node"),
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("child/child.start"),
			"",
			"",
			json.RawMessage(`{"entity_id":"ent-child"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID("ent-child"),
			StateCarrier: runtimeengine.NewStateCarrier(map[string]any{"flow_path": "child/inst-1", "subject_id": "ent-parent", "parent_entity_id": "ent-parent"}, nil, nil),
		},
	}

	emitCtx := runtimeengine.WithEmitSurface(testAuthorActivityContext(t, context.Background()), runtimeengine.EmitSurfaceDeclarative)
	_, err := shaper.ShapeEmitPayload(emitCtx, req, "child/child.internal", map[string]any{
		"entity_id": "ent-child",
	})
	if err == nil {
		t.Fatal("expected declarative surface missing required field to fail closed")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("ShapeEmitPayload declarative surface error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}
}

func TestPipelineEnginePayloadShaper_RejectsMissingRequiredFieldsForConcreteTemplateOutput(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n",
		"events.yaml": "parent.trigger:\n  entity_id: string\n",

		"child/schema.yaml": "name: child\nmode: template\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.done]\n",
		"child/events.yaml": "child.start:\n  entity_id: string\nchild.done:\n  step: string\n",
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	shaper := pipelineEnginePayloadShaper{
		coordinator: &PipelineCoordinator{
			module: &previewWorkflowModule{
				bundle: bundle,
			},
		},
	}

	req := runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID("ent-child"),
		Node:     pipelineNode(t, "child", "child-node"),
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("child/child.start"),
			"",
			"",
			json.RawMessage(`{"entity_id":"ent-child"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID("ent-child"),
			StateCarrier: runtimeengine.NewStateCarrier(map[string]any{"flow_path": "child/inst-1", "subject_id": "ent-parent", "parent_entity_id": "ent-parent"}, nil, nil),
		},
	}

	_, err := shaper.ShapeEmitPayload(testAuthorActivityContext(t, context.Background()), req, "child/inst-1/child.done", map[string]any{})
	if err == nil {
		t.Fatal("expected concrete template output missing required field to fail closed")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("ShapeEmitPayload concrete template output error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}

	if _, err := shaper.ShapeEmitPayload(testAuthorActivityContext(t, context.Background()), req, "child/inst-1/child.done", map[string]any{"step": "done"}); err != nil {
		t.Fatalf("ShapeEmitPayload concrete template output with required field: %v", err)
	}
}

func TestPipelineEnginePayloadShaper_RejectsEnvelopeOnlyRequiredFieldOnDeclarativeSurface(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n  outputs:\n    events: [parent.result]\n",
		"events.yaml": "parent.trigger:\n  entity_id: string\nparent.result:\n  entity_id: string\n",

		"child/schema.yaml": "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.internal]\n",
		"child/events.yaml": "child.start:\n  entity_id: string\nchild.internal:\n  entity_id: string\n",
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	shaper := pipelineEnginePayloadShaper{
		coordinator: &PipelineCoordinator{
			module: &previewWorkflowModule{
				bundle: bundle,
			},
		},
	}

	req := runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID("ent-child"),
		Node:     pipelineNode(t, "child", "child-node"),
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("child/child.start"),
			"",
			"",
			json.RawMessage(`{"entity_id":"ent-child"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID("ent-child"),
			StateCarrier: runtimeengine.NewStateCarrier(map[string]any{"flow_path": "child/inst-1", "subject_id": "ent-parent", "parent_entity_id": "ent-parent"}, nil, nil),
		},
	}

	emitCtx := runtimeengine.WithEmitSurface(testAuthorActivityContext(t, context.Background()), runtimeengine.EmitSurfaceDeclarative)
	_, err := shaper.ShapeEmitPayload(emitCtx, req, "child/child.internal", map[string]any{})
	if err == nil {
		t.Fatal("expected declarative surface envelope-only required field to fail closed")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("ShapeEmitPayload declarative surface error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}
}

func TestValidatePipelineEmitPayload_RejectsEnumViolationOnDeclarativeSurface(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n  outputs:\n    events: [parent.result]\n",
		"events.yaml": "parent.trigger:\n  entity_id: string\nparent.result:\n  entity_id: string\n",
		"types.yaml":  "enums:\n  Mode:\n    values: [fast, deep]\n    default: fast\n",

		"child/schema.yaml": "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.internal]\n",
		"child/events.yaml": "child.start:\n  entity_id: string\nchild.internal:\n  mode: Mode\n",
	})

	err := validatePipelineEmitPayload(source, "child", "child.internal", map[string]any{
		"mode": "invalid",
	}, nil, runtimeengine.EmitSurfaceDeclarative)
	if err == nil {
		t.Fatal("expected enum violation to fail closed on the declarative surface")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("validatePipelineEmitPayload error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}
	if !strings.Contains(err.Error(), "invalid enum value") {
		t.Fatalf("validatePipelineEmitPayload error = %v, want enum detail", err)
	}
	var contractErr *runtimeengine.EmitPayloadContractError
	if !errors.As(err, &contractErr) || contractErr.Event != "child/child.internal" || contractErr.Kind != runtimeengine.EmitPayloadSchemaMismatch || contractErr.Path != "$.mode" || contractErr.Constraint != "enum" || contractErr.Expected != "declared enum member" || contractErr.Actual != "invalid" {
		t.Fatalf("validatePipelineEmitPayload typed error = %#v", err)
	}
}

func TestPipelineEmitPayloadContractProducersReturnOneTypedFact(t *testing.T) {
	root := &runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Events: map[string]runtimecontracts.EventCatalogEntry{
		"company.registered": {
			Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{
					"gem_score":   {Type: "number"},
					"external_id": {Type: "uuid"},
				},
				Required: []string{"gem_score", "external_id"},
			},
		},
	}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{Root: root, ByID: map[string]*runtimecontracts.FlowContractView{".": root}},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)

	tests := []struct {
		name       string
		run        func() error
		event      string
		kind       runtimeengine.EmitPayloadContractKind
		path       string
		constraint string
		expected   string
		actual     string
	}{
		{
			name: "schema mismatch", event: "company.registered", kind: runtimeengine.EmitPayloadSchemaMismatch,
			path: "$.gem_score", constraint: "type", expected: "number", actual: "string",
			run: func() error {
				return validatePipelineEmitPayload(source, "", "company.registered", map[string]any{"gem_score": "7.2", "external_id": uuid.NewString()}, nil, runtimeengine.EmitSurfaceDeclarative)
			},
		},
		{
			name: "schema mismatch empty value", event: "company.registered", kind: runtimeengine.EmitPayloadSchemaMismatch,
			path: "$.external_id", constraint: "format", expected: "uuid", actual: "",
			run: func() error {
				return validatePipelineEmitPayload(source, "", "company.registered", map[string]any{"gem_score": 7.2, "external_id": ""}, nil, runtimeengine.EmitSurfaceDeclarative)
			},
		},
		{
			name: "authored envelope field", event: "company.registered", kind: runtimeengine.EmitPayloadEnvelopeField,
			path: "$", constraint: "platform_owned_envelope_fields", expected: "authored business payload only", actual: "entity_id",
			run: func() error {
				return rejectAuthoredEnvelopeFields("company.registered", map[string]any{"entity_id": "hostile"})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.run()
			var contractErr *runtimeengine.EmitPayloadContractError
			if !errors.As(err, &contractErr) || !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
				t.Fatalf("producer error = %#v, want typed emit contract error", err)
			}
			if contractErr.Event != test.event || contractErr.Kind != test.kind || contractErr.Path != test.path || contractErr.Constraint != test.constraint || contractErr.Expected != test.expected || contractErr.Actual != test.actual || strings.TrimSpace(contractErr.Detail) == "" {
				t.Fatalf("typed producer error = %#v", contractErr)
			}
		})
	}
	t.Run("unresolved schema rejected before execution", func(t *testing.T) {
		root.Events["company.unresolved"] = runtimecontracts.EventCatalogEntry{
			Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{"evidence": {Type: "NotDeclared"}},
				Required:   []string{"evidence"},
			},
		}
		if err := runtimecontracts.CompileWorkflowSemantics(bundle); err == nil || !strings.Contains(err.Error(), "NotDeclared") {
			t.Fatalf("unresolved schema escaped compiler admission: %v", err)
		}
	})
}

func TestPipelineEnginePayloadShaper_DoesNotBorrowRootSchemaForChildOutput(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  outputs:\n    events: [handoff.completed]\n",
		"types.yaml":  "types:\n  Evidence:\n    root_field: text\n",
		"events.yaml": "handoff.completed:\n  evidence: Evidence\n",

		"child/schema.yaml": "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  outputs:\n    events: [handoff.completed]\n",
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	shaper := pipelineEnginePayloadShaper{
		coordinator: &PipelineCoordinator{
			module: &previewWorkflowModule{
				bundle: bundle,
			},
		},
	}
	req := runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID("ent-child"),
		Node:     pipelineNode(t, "child", "child-node"),
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("child/child.internal"),
			"",
			"",
			json.RawMessage(`{"entity_id":"ent-child"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID("ent-child"),
			StateCarrier: runtimeengine.NewStateCarrier(map[string]any{"flow_path": "child/inst-1"}, nil, nil),
		},
	}

	for _, eventType := range []string{"handoff.completed", "child/handoff.completed"} {
		t.Run(eventType, func(t *testing.T) {
			_, err := shaper.ShapeEmitPayload(testAuthorActivityContext(t, context.Background()), req, eventType, map[string]any{
				"evidence": "not-a-root-Evidence-object",
			})
			if err != nil {
				t.Fatalf("ShapeEmitPayload borrowed the root schema for an untyped child event: %v", err)
			}
		})
	}
}

func TestPipelineEnginePayloadShaper_RejectsUndeclaredFieldsOnDeclarativeSurface(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-child-flow-local-events")
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	shaper := pipelineEnginePayloadShaper{
		coordinator: &PipelineCoordinator{
			module: &previewWorkflowModule{
				bundle: bundle,
			},
		},
	}

	req := runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID("ent-child"),
		Node:     pipelineNode(t, "child", "child-node"),
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("child/child.internal"),
			"",
			"",
			json.RawMessage(`{"entity_id":"ent-child","step":"done"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID("ent-child"),
			StateCarrier: runtimeengine.NewStateCarrier(map[string]any{"flow_path": "child/inst-1", "subject_id": "ent-parent", "parent_entity_id": "ent-parent"}, nil, nil),
		},
	}

	emitCtx := runtimeengine.WithEmitSurface(testAuthorActivityContext(t, context.Background()), runtimeengine.EmitSurfaceDeclarative)
	_, err := shaper.ShapeEmitPayload(emitCtx, req, "child/child.done", map[string]any{
		"entity_id":   "ent-child",
		"vertical_id": "ent-child",
		"result":      "accepted",
	})
	if err == nil {
		t.Fatal("expected undeclared declarative surface fields to fail closed")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("ShapeEmitPayload declarative surface error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}
}

func TestPipelineEmitPayloadProperties_UsesCanonicalFlowEventProofForLocalAndCanonicalRefs(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-child-flow-local-events")

	canonical := pipelineEmitPayloadProperties(source, "child", "child/child.internal")
	local := pipelineEmitPayloadProperties(source, "child", "child.internal")

	if len(canonical) == 0 {
		t.Fatalf("expected canonical child event schema properties, got %#v", canonical)
	}
	if len(local) == 0 {
		t.Fatalf("expected local child event schema properties, got %#v", local)
	}
	if !reflect.DeepEqual(canonical, local) {
		t.Fatalf("local/canonical payload properties drifted: canonical=%#v local=%#v", canonical, local)
	}
	if _, ok := canonical["step"]; !ok {
		t.Fatalf("expected step in canonical payload properties: %#v", canonical)
	}
	if _, ok := canonical["entity_id"]; ok {
		t.Fatalf("payload properties must not expose envelope entity_id: %#v", canonical)
	}
}

type pipelineTestEntityCollectionPersistenceReader struct {
	records []WorkflowEntityStatePersistenceRecord
	calls   int
}

func (r *pipelineTestEntityCollectionPersistenceReader) QueryWorkflowEntityCollection(_ context.Context, owner WorkflowEntityCollectionOwner) ([]WorkflowEntityStatePersistenceRecord, error) {
	r.calls++
	return FilterWorkflowEntityCollectionRecords(r.records, owner)
}

func TestPipelineEngineEntityCollectionReaderMaterializesDeclaredStateRows(t *testing.T) {
	runID := uuid.NewString()
	source := loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":   "name: work\ninitial_state: active\nstates: [active]\n",
		"entities.yaml": "items:\n  id: text\n  status: text\n",
	})
	persisted := &pipelineTestEntityCollectionPersistenceReader{records: []WorkflowEntityStatePersistenceRecord{
		{EntityID: uuid.NewString(), FlowInstance: runID, EntityType: "items", Fields: json.RawMessage(`{"id":"a","status":"queued","undeclared":"drop"}`)},
	}}
	reader := pipelineEngineEntityCollectionReader{coordinator: &PipelineCoordinator{
		module:        &pipelineFixtureWorkflowModule{source: source},
		workflowStore: &workflowInstanceStore{entityCollectionReader: persisted},
	}}
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
	rows, err := reader.QueryEntityCollection(ctx, runID, ".", "items")
	if err != nil {
		t.Fatalf("QueryEntityCollection: %v", err)
	}
	if persisted.calls != 1 || len(rows) != 1 || rows[0]["id"] != "a" || rows[0]["status"] != "queued" {
		t.Fatalf("rows = %#v", rows)
	}
	if _, err := reader.QueryEntityCollection(ctx, "", ".", "items"); err == nil || persisted.calls != 1 {
		t.Fatalf("missing explicit run read persistence: error=%v calls=%d", err, persisted.calls)
	}
	if _, survives := rows[0]["undeclared"]; survives {
		t.Fatalf("entity collection leaked undeclared persisted field: %#v", rows[0])
	}
	rows[0]["status"] = "mutated"
	if strings.Contains(string(persisted.records[0].Fields), "mutated") {
		t.Fatalf("query result aliases persisted fields: %s", persisted.records[0].Fields)
	}
}
