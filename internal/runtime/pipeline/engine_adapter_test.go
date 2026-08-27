package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type actionEmitIntentCollectorForTestKey struct{}

func withActionEmitIntentCollectorForTest(ctx context.Context, intents *[]runtimeengine.EmitIntent) context.Context {
	return context.WithValue(ctx, actionEmitIntentCollectorForTestKey{}, intents)
}

func (r pipelineEngineActionRunner) executeAction(ctx context.Context, action runtimecontracts.ActionSpec, entry runtimeregistry.ActionInstruction, execCtx runtimeengine.ExecutionContext) (bool, error) {
	result, err := r.ExecuteAction(ctx, action, entry, execCtx)
	if result.State != nil && r.coordinator != nil {
		state := pipelineEngineStateRepo{coordinator: r.coordinator}
		owner := pipelineEngineMutationOwner{store: r.coordinator.workflowStore, state: state}
		_, commitErr := owner.CommitEngineMutation(ctx, runtimeengine.EngineMutation{
			Address: execCtx.Request.StateAddress(),
			State:   *result.State,
		})
		if commitErr != nil {
			err = errors.Join(err, commitErr)
		}
	}
	if collector, ok := ctx.Value(actionEmitIntentCollectorForTestKey{}).(*[]runtimeengine.EmitIntent); ok && collector != nil {
		*collector = append(*collector, result.EmitIntents...)
	}
	return result.Handled, err
}

func commitProjectedWorkflowEvidenceForTest(ctx context.Context, pc *PipelineCoordinator, route runtimeflowidentity.Route, entityID, flowID, bucketID string, payload map[string]any) error {
	stateRepo := pipelineEngineStateRepo{coordinator: pc}
	address := runtimeengine.StateAddress{
		FlowID:   identity.NormalizeFlowID(flowID),
		Route:    route,
		EntityID: identity.NormalizeEntityID(entityID),
	}
	state, ok, err := stateRepo.LoadState(ctx, address)
	if err != nil {
		return err
	}
	if !ok {
		state = runtimeengine.StateSnapshot{EntityID: address.EntityID}
	}
	event := eventtest.RunCreatingRootIngress(
		uuid.NewString(), "workflow.evidence_recorded", "", "", mustJSON(payload), 0, "", "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), route.InstancePath),
		time.Now().UTC(),
	)
	mutation, err := pc.projectWorkflowEvidence(runtimeengine.ExecutionContext{Request: runtimeengine.ExecutionRequest{
		EntityID: address.EntityID,
		Node:     mustPipelineNode(string(address.FlowID), "evidence-node"),
		Route:    route,
		Event:    event,
		State:    state,
	}}, bucketID, payload)
	if err != nil {
		return err
	}
	owner := pipelineEngineMutationOwner{store: pc.workflowStore, state: stateRepo}
	_, err = owner.CommitEngineMutation(ctx, runtimeengine.EngineMutation{Address: address, State: *mutation})
	return err
}

func testEngineStateMutation(metadata map[string]any, gates map[string]bool, buckets map[string]map[string]any) runtimeengine.StateMutation {
	return runtimeengine.StateMutation{
		StateCarrier: runtimeengine.NewStateCarrier(metadata, gates, buckets),
	}
}

func testEngineStateAddress(flowID, instancePath, entityID string) runtimeengine.StateAddress {
	return runtimeengine.StateAddress{
		FlowID:   identity.NormalizeFlowID(flowID),
		Route:    runtimeflowidentity.RouteForInstancePath(instancePath),
		EntityID: identity.NormalizeEntityID(entityID),
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
	if err := applyEngineStateMutation(instance, mutation, allowedFields, source, flowID); err != nil {
		t.Fatalf("apply materialized engine state mutation: %v", err)
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
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
    mode: static
`,
		"schema.yaml": `
name: runtime-test
initial_state: ready
states: [ready]
`,
		"flows/child/schema.yaml": `
name: child
mode: static
initial_state: queued
states: [queued]
`,
		"flows/child/entities.yaml": `
child_entity:
  request_id: text
`,
	})
	pc := newPostgresPipelineCoordinatorForTest(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &pipelineFixtureWorkflowModule{
			source:   source,
			workflow: NewWorkflowDefinition("runtime-test", []WorkflowStage{{Name: "ready"}}, nil),
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
	ok, err := eval.EvalBool(`query_entities(request_id == payload.request_id).count == 1`, runtimeengine.BaseContext{
		FlowID:  "child",
		Event:   values.Wrap(map[string]any{"run_id": testPipelineRunID}),
		Payload: values.Wrap(map[string]any{"request_id": "req-existing"}),
	})
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

func TestMaybeDeactivateTerminalFlowInstance_IgnoresRootWorkflowEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:           "root",
			InitialStage:   "pending",
			TerminalStages: []string{"done"},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"root": {},
		},
	}
	deactivated := false
	pc := newPostgresPipelineCoordinatorForTest(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &pipelineFixtureWorkflowModule{
			source:   semanticview.Wrap(bundle),
			workflow: NewWorkflowDefinition("root", []WorkflowStage{{Name: "pending"}, {Name: "done", Terminal: true}}, nil),
		},
		InstanceDeactivator: func(context.Context, FlowInstanceDeactivationRequest) error {
			deactivated = true
			return nil
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

	if err := pc.maybeDeactivateTerminalFlowInstance(testPipelineCoordinatorRunContext(t, pc), testWorkflowInstanceRoute("root"), identity.NormalizeEntityID(entityID), "done"); err != nil {
		t.Fatalf("maybeDeactivateTerminalFlowInstance: %v", err)
	}
	if deactivated {
		t.Fatal("expected root workflow entity to skip flow-instance deactivation")
	}
}

func TestMaybeDeactivateTerminalFlowInstance_PassesTerminalStateToTemplateDeactivation(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:         "root",
			InitialStage: "pending",
			FlowTerminal: map[string][]string{
				"review": {"completed"},
			},
			FlowPrefix: map[string]string{
				"review": "review",
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"review": {Mode: "template"},
		},
	}
	var got FlowInstanceDeactivationRequest
	called := false
	pc := newPostgresPipelineCoordinatorForTest(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &pipelineFixtureWorkflowModule{
			source:   semanticview.Wrap(bundle),
			workflow: NewWorkflowDefinition("root", []WorkflowStage{{Name: "pending"}, {Name: "completed", Terminal: true}}, nil),
		},
		InstanceDeactivator: func(_ context.Context, req FlowInstanceDeactivationRequest) error {
			called = true
			got = req
			return nil
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

	if err := pc.maybeDeactivateTerminalFlowInstance(testPipelineCoordinatorRunContext(t, pc), testWorkflowInstanceRoute(flowPath), identity.NormalizeEntityID(entityID), "completed"); err != nil {
		t.Fatalf("maybeDeactivateTerminalFlowInstance: %v", err)
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

	got := workflowStateGatesForScope(source, "grandchild", map[string]bool{
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
			address := testEngineStateAddress("review", wrongRunID, entityID)
			mutation := testEngineStateMutation(map[string]any{"marker": "must-not-persist"}, nil, nil)
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
				WorkflowName: "review", WorkflowVersion: "1", Mode: runtimecontracts.FlowModeStatic,
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
				Address: testEngineStateAddress("review", testPipelineRunID, entityID), State: mutation,
			})
			if err == nil || !strings.Contains(err.Error(), `entity_type "wrong_entity" disagrees with canonical contract "test_entity"`) {
				t.Fatalf("entity contract drift mutation error = %v", err)
			}
			stored, found, loadErr := store.Load(ctx, testWorkflowInstanceRoute(testPipelineRunID))
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
					Address: testEngineStateAddress("review", testPipelineRunID, entityID), State: mutation,
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
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: review
    flow: review
    mode: static
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/review/schema.yaml": `
name: review
mode: static
initial_state: queued
states: [queued]
`,
		"flows/review/entities.yaml": `
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
			module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Semantics:    runtimecontracts.WorkflowSemanticView{Name: "root"},
				RootEntities: testEntityContractsForType("test_entity"),
			})},
		},
	}
	entityID := identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111")
	if err := store.upsert(testWorkflowStoreRunContext(t, store), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      entityID.String(),
		StorageRef:      testPipelineRunID,
		EntityID:        entityID.String(),
		WorkflowName:    "root",
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

	address := testEngineStateAddress("root", testPipelineRunID, entityID.String())
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
	if got := loaded.Fields["score"]; got != 91 && got != 91.0 {
		t.Fatalf("loaded metadata score = %#v, want 91", got)
	}
	if !loaded.Gates["ready"] {
		t.Fatalf("loaded gates = %#v, want ready=true", loaded.Gates)
	}
	if got := loaded.StateBuckets["evidence"]["count"]; got != 2 && got != 2.0 {
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

func TestRecordWorkflowEvidence_ReturnsWorkflowStoreMutationError(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	pc := &PipelineCoordinator{
		workflowStore: newPostgresWorkflowInstanceStoreForTest(db),
	}

	err := commitProjectedWorkflowEvidenceForTest(testPipelineRunContextNoSeed(t), pc, testWorkflowInstanceRoute("11111111-1111-1111-1111-111111111111"), "11111111-1111-1111-1111-111111111111", "", "research", map[string]any{"summary": "done"})
	if err == nil {
		t.Fatal("expected recordWorkflowEvidence to fail when workflow store mutate fails")
	}
}

func TestPipelineEngineActionRunner_RecordEvidenceReturnsMutationError(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	pc := &PipelineCoordinator{
		workflowStore: newPostgresWorkflowInstanceStoreForTest(db),
		module: &previewWorkflowModule{
			bundle: &runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{
					NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
						"node-a": {
							"research.completed": {
								Action:         runtimecontracts.ActionSpec{ID: "record_evidence"},
								EvidenceTarget: "research",
							},
						},
					},
				},
			},
		},
	}
	runner := pipelineEngineActionRunner{coordinator: pc}
	ok, err := runner.executeAction(testAuthorActivityContext(t, context.Background()), runtimecontracts.ActionSpec{ID: "record_evidence"}, runtimeregistry.ActionInstruction{Builtin: "record_evidence"}, runtimeengine.ExecutionContext{
		Request: runtimeengine.ExecutionRequest{
			EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
			Node:     pipelineNode(t, "", "node-a"),
			Event: eventtest.RunCreatingRootIngress(
				"",
				"research.completed",
				"",
				"",
				[]byte(`{"summary":"done"}`),
				0,
				"",
				"",
				events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111111"),
				time.Time{},
			),
			HandlerEventKey: "research.completed",
			Handler: runtimecontracts.SystemNodeEventHandler{
				Action:         runtimecontracts.ActionSpec{ID: "record_evidence"},
				EvidenceTarget: "research",
			},
		},
	})
	if !ok {
		t.Fatal("expected record_evidence action to be claimed")
	}
	if err == nil {
		t.Fatal("expected record_evidence action to return mutation error")
	}
}

func TestPipelineEngineActionRunner_RecordEvidenceUsesMatchedHandlerEvidenceTargetForConcreteEvents(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := newPostgresWorkflowInstanceStoreForTest(db)
	source := semanticview.Wrap(admitSyntheticEntityContractsForTest(t, &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "engine-action-test"},
	}, "", map[string]string{"operating": "test_entity"}))
	pc := &PipelineCoordinator{workflowStore: store, module: staticSemanticWorkflowModule{source: source}}
	runner := pipelineEngineActionRunner{coordinator: pc}

	tests := []struct {
		name            string
		entityID        string
		flowInstance    string
		concreteEvent   events.EventType
		handlerEventKey string
		action          runtimecontracts.ActionSpec
		handler         runtimecontracts.SystemNodeEventHandler
		wantBucket      string
		wantSummary     string
	}{
		{
			name:            "handler action",
			entityID:        "11111111-1111-1111-1111-111111111111",
			flowInstance:    "operating/instance-1",
			concreteEvent:   "operating/instance-1/build_progress",
			handlerEventKey: "build_progress",
			action:          runtimecontracts.ActionSpec{ID: "record_evidence"},
			handler: runtimecontracts.SystemNodeEventHandler{
				Action:         runtimecontracts.ActionSpec{ID: "record_evidence"},
				EvidenceTarget: "build_evidence",
			},
			wantBucket:  "build_evidence",
			wantSummary: "compile complete",
		},
		{
			name:            "selected rule action",
			entityID:        "22222222-2222-2222-2222-222222222222",
			flowInstance:    "operating/instance-2",
			concreteEvent:   "operating/instance-2/build_progress",
			handlerEventKey: "build_progress",
			action:          runtimecontracts.ActionSpec{ID: "record_evidence"},
			handler: runtimecontracts.SystemNodeEventHandler{
				Rules: []runtimecontracts.HandlerRuleEntry{{
					ID:     "capture-progress",
					Action: runtimecontracts.ActionSpec{ID: "record_evidence"},
				}},
				EvidenceTarget: "rule_evidence",
			},
			wantBucket:  "rule_evidence",
			wantSummary: "rule branch complete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testWorkflowStoreRunContext(t, store)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID:      runtimeflowidentity.LogicalInstanceID(tt.flowInstance),
				StorageRef:      tt.flowInstance,
				EntityID:        tt.entityID,
				WorkflowName:    "operating",
				WorkflowVersion: "1.0.0",
				CurrentState:    "initializing",
				Fields: map[string]any{
					"entity_id": tt.entityID, "flow_path": tt.flowInstance, "instance_id": runtimeflowidentity.LogicalInstanceID(tt.flowInstance),
				},
				StateBuckets: map[string]any{},
				EntityType:   "test_entity",
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			ok, err := runner.executeAction(ctx, tt.action, runtimeregistry.ActionInstruction{Builtin: "record_evidence"}, runtimeengine.ExecutionContext{
				Request: runtimeengine.ExecutionRequest{
					EntityID:        identity.NormalizeEntityID(tt.entityID),
					ExecutionFlowID: identity.NormalizeFlowID("operating"),
					Node:            pipelineNode(t, "operating", "build-orchestrator"),
					Route:           testWorkflowInstanceRoute(tt.flowInstance),
					ProducerSource: mustStaticExecutionRoutingSource(events.RouteIdentity{
						FlowID:       "operating",
						FlowInstance: tt.flowInstance,
						EntityID:     tt.entityID,
					}),
					Event: eventtest.RunCreatingRootIngress(
						uuid.NewString(),
						tt.concreteEvent,
						"",
						"",
						mustJSON(map[string]any{"summary": tt.wantSummary}),
						0,
						"",
						"",
						testWorkflowSourceEnvelope("operating", tt.flowInstance, tt.entityID),
						time.Now().UTC(),
					),
					HandlerEventKey: tt.handlerEventKey,
					Handler:         tt.handler,
				},
			})
			if !ok {
				t.Fatal("expected record_evidence action to be claimed")
			}
			if err != nil {
				t.Fatalf("ExecuteAction: %v", err)
			}

			instance, exists, err := store.Load(ctx, testWorkflowInstanceRoute(tt.flowInstance))
			if err != nil {
				t.Fatalf("load workflow instance: %v", err)
			}
			if !exists {
				t.Fatal("expected workflow instance to exist")
			}
			entries := workflowEvidenceEntries(t, instance, tt.wantBucket)
			if len(entries) != 1 {
				t.Fatalf("evidence entries = %d, want 1", len(entries))
			}
			if got := entries[0]["summary"]; got != tt.wantSummary {
				t.Fatalf("evidence summary = %#v, want %q", got, tt.wantSummary)
			}
		})
	}
}

func TestPipelineEngineActionRunner_CreateFlowInstanceUsesExecutionBaseContextForConfigFrom(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	runner := pipelineEngineActionRunner{coordinator: pc}
	evt := eventtest.ChildWithLineage(
		"evt-123",
		"spawn.requested",
		"",
		"",
		[]byte(`{"instance_id":"inst-42","name":"alpha","template_id":"application-basic-v1"}`),
		0,
		events.EventLineage{RunID: testPipelineRunID, ParentEventID: "source-evt-1", ExecutionMode: executionmode.Live},
		events.EventEnvelope{
			EntityID: "ent-1",
			Source: events.RouteIdentity{
				FlowID:       "parent-flow",
				FlowInstance: "parent-flow/source-1",
				EntityID:     "ent-parent",
			},
		},
		time.Time{},
	)

	base := values.NewContext()
	base.Event = values.Wrap(evt.ContextMap("ready"))
	base.Payload = values.Wrap(parsePayloadMap(evt.Payload()))
	base.PlatformEntity = values.Wrap(map[string]any{"id": "ent-1"})
	action := runtimecontracts.ActionSpec{
		ID:             "create_flow_instance",
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"source_event_id": "event.id",
				"event_type":      "event.type",
				"source_flow":     "event.source.flow_id",
				"correlation_id":  "event.source_event_id",
				"name":            "payload.name",
				"template_id":     "payload.template_id",
				"parent_entity":   "_entity.id",
			},
		},
	}

	ok, err := runner.executeAction(testAuthorActivityContext(t, context.Background()), action, runtimeregistry.ActionInstruction{Builtin: "create_flow_instance"}, runtimeengine.ExecutionContext{
		Base: base,
		Request: runtimeengine.ExecutionRequest{
			EntityID: identity.NormalizeEntityID("ent-1"),
			Node:     pipelineNode(t, "", "spawner"),
			Event:    evt,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteAction: %v", err)
	}
	if !ok {
		t.Fatal("expected create_flow_instance action to be claimed")
	}
	for key, want := range map[string]any{
		"source_event_id": "evt-123",
		"event_type":      "spawn.requested",
		"source_flow":     "parent-flow",
		"correlation_id":  "source-evt-1",
		"name":            "alpha",
		"template_id":     "application-basic-v1",
		"parent_entity":   "ent-1",
	} {
		if got := captured.Config[key]; got != want {
			t.Fatalf("config[%s] = %#v, want %#v", key, got, want)
		}
	}
}

func TestPipelineEngineActionRunner_MailboxWriteMaterializesIdempotentRow(t *testing.T) {
	materializer := &recordingMailboxWriteMaterializer{}
	pc := &PipelineCoordinator{mailboxMaterializer: materializer}
	runner := pipelineEngineActionRunner{coordinator: pc}
	ctx := testAuthorActivityContext(t, context.Background())
	eventID := "11111111-1111-1111-1111-111111111111"
	entityID := "22222222-2222-2222-2222-222222222222"
	action := runtimecontracts.ActionSpec{
		ID: "mailbox_write",
		Mailbox: &runtimecontracts.MailboxWriteSpec{
			ItemType:     runtimecontracts.LiteralExpression("review_request"),
			Severity:     runtimecontracts.LiteralExpression("urgent"),
			Summary:      runtimecontracts.LiteralExpression("Review validation package"),
			EntityID:     runtimecontracts.RefExpression("_entity.id"),
			FlowInstance: runtimecontracts.RefExpression("_entity.flow_instance"),
			Payload: map[string]runtimecontracts.ExpressionValue{
				"review_kind":   runtimecontracts.RefExpression("payload.review_kind"),
				"operator_hint": runtimecontracts.LiteralExpression("inspect_package"),
			},
		},
	}
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		"mailbox.review_requested",
		"",
		"",
		[]byte(`{"review_kind":"validation"}`),
		0,
		"",
		"",
		events.EventEnvelope{
			EntityID:     entityID,
			FlowInstance: "validation/case-1",
			Scope:        events.EventScopeEntity,
		},
		time.Time{},
	)

	base := values.NewContext()
	base.Event = values.Wrap(evt.ContextMap(""))
	base.Payload = values.Wrap(parsePayloadMap(evt.Payload()))
	base.PlatformEntity = values.Wrap(map[string]any{
		"id":            entityID,
		"flow_instance": "validation/case-1",
	})
	execCtx := runtimeengine.ExecutionContext{
		Base: base,
		Request: runtimeengine.ExecutionRequest{
			EntityID: identity.NormalizeEntityID(entityID),
			Node:     pipelineNode(t, "", "mailbox-node"),
			Event:    evt,
		},
	}
	for i := 0; i < 2; i++ {
		ok, err := runner.executeAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "mailbox_write"}, execCtx)
		if err != nil {
			t.Fatalf("ExecuteAction iteration %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("ExecuteAction iteration %d was not claimed", i)
		}
	}
	if materializer.calls != 2 {
		t.Fatalf("materializer calls = %d, want duplicate attempts to reach idempotent owner", materializer.calls)
	}
	rows := materializer.rows()
	if len(rows) != 1 {
		t.Fatalf("materialized rows = %d, want 1 idempotent row", len(rows))
	}
	got := rows[0]
	if got.ItemID != deterministicMailboxItemID(eventID, execCtx.Request.Node.Key()) {
		t.Fatalf("item_id = %q, want deterministic id", got.ItemID)
	}
	if got.SourceEventID != eventID || got.EntityID != entityID || got.FlowInstance != "validation/case-1" || got.Scope != "entity" {
		t.Fatalf("mailbox identity = source %q entity %q flow %q scope %q", got.SourceEventID, got.EntityID, got.FlowInstance, got.Scope)
	}
	if got.ItemType != "review_request" || got.Severity != "urgent" || got.Summary != "Review validation package" {
		t.Fatalf("mailbox fields type=%q severity=%q summary=%q", got.ItemType, got.Severity, got.Summary)
	}
	var payload map[string]any
	if err := json.Unmarshal(got.Payload, &payload); err != nil {
		t.Fatalf("decode materialized payload: %v", err)
	}
	if payload["review_kind"] != "validation" || payload["operator_hint"] != "inspect_package" {
		t.Fatalf("payload = %#v, want review_kind/operator_hint", payload)
	}
	if got.FromAgent != "system_node:"+execCtx.Request.Node.Key() {
		t.Fatalf("from_agent = %q", got.FromAgent)
	}
}

func TestPipelineEngineActionRunner_MailboxWriteFailsClosedOnMissingRequiredExpression(t *testing.T) {
	materializer := &recordingMailboxWriteMaterializer{}
	runner := pipelineEngineActionRunner{coordinator: &PipelineCoordinator{mailboxMaterializer: materializer}}
	ctx := testAuthorActivityContext(t, context.Background())
	eventID := "33333333-3333-3333-3333-333333333333"
	action := runtimecontracts.ActionSpec{
		ID: "mailbox_write",
		Mailbox: &runtimecontracts.MailboxWriteSpec{
			ItemType: runtimecontracts.LiteralExpression("review_request"),
			Summary:  runtimecontracts.RefExpression("payload.missing_summary"),
		},
	}
	evt := eventtest.RunCreatingRootIngress(eventID, "mailbox.review_requested", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{})
	base := values.NewContext()
	base.Event = values.Wrap(evt.ContextMap(""))
	base.Payload = values.Wrap(parsePayloadMap(evt.Payload()))

	ok, err := runner.executeAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "mailbox_write"}, runtimeengine.ExecutionContext{
		Base: base,
		Request: runtimeengine.ExecutionRequest{
			Node:  pipelineNode(t, "", "mailbox-node"),
			Event: evt,
		},
	})
	if !ok {
		t.Fatal("expected mailbox_write action to be claimed")
	}
	if err == nil || !strings.Contains(err.Error(), "mailbox.summary resolved empty") {
		t.Fatalf("ExecuteAction error = %v, want missing summary", err)
	}
	if materializer.calls != 0 {
		t.Fatalf("materializer calls = %d, want no persistence after validation failure", materializer.calls)
	}
}

type recordingMailboxWriteMaterializer struct {
	mu    sync.Mutex
	calls int
	byID  map[string]MailboxWriteMaterialization
}

func (m *recordingMailboxWriteMaterializer) MaterializeMailboxWrite(_ context.Context, item MailboxWriteMaterialization) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.byID == nil {
		m.byID = map[string]MailboxWriteMaterialization{}
	}
	if _, ok := m.byID[item.ItemID]; !ok {
		m.byID[item.ItemID] = item
	}
	return nil
}

func (m *recordingMailboxWriteMaterializer) rows() []MailboxWriteMaterialization {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]MailboxWriteMaterialization, 0, len(m.byID))
	for _, row := range m.byID {
		out = append(out, row)
	}
	return out
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitMaterializesLocalGitRef(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	artifactRoot := t.TempDir()
	pc := &PipelineCoordinator{workflowStore: store, artifactRoot: artifactRoot, module: &pipelineFixtureWorkflowModule{source: testRootEntityContractSource("artifact-repo", "test_entity")}}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "working",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\nrank: 2\n")
	ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok {
		t.Fatal("expected artifact_repo_commit action to be claimed")
	}
	if err != nil {
		t.Fatalf("ExecuteAction: %v", err)
	}

	instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil || !ok {
		t.Fatalf("load workflow instance ok=%v err=%v", ok, err)
	}
	if got := strings.TrimSpace(asString(instance.Fields["repo_url"])); got != "swarm-artifact://repos/"+initial["repo_id"].(string) {
		t.Fatalf("repo_url = %q", got)
	}
	if got := strings.TrimSpace(asString(instance.Fields["status"])); got != "committed" {
		t.Fatalf("artifact entity status = %q, want committed", got)
	}
	assertEntityStateField(t, db, entityID, "status", "committed")
	ref := strings.TrimSpace(asString(instance.Fields["current_ref"]))
	if len(ref) != 40 {
		t.Fatalf("current_ref length = %d ref=%q", len(ref), ref)
	}
	manifest, ok := instance.Fields["file_manifest"].(map[string]any)
	if !ok {
		t.Fatalf("file_manifest = %#v", instance.Fields["file_manifest"])
	}
	if got := strings.TrimSpace(asString(manifest["source_event_id"])); got != execCtx.Request.Event.ID() {
		t.Fatalf("manifest source_event_id = %q", got)
	}
	if _, exists := manifest["vertical_id"]; exists {
		t.Fatalf("manifest contains product vertical_id: %#v", manifest)
	}
	provenance, ok := manifest["provenance"].(map[string]any)
	if !ok {
		t.Fatalf("manifest provenance = %#v", manifest["provenance"])
	}
	if got := strings.TrimSpace(asString(provenance["source_record_id"])); got != initial["source_record_id"].(string) {
		t.Fatalf("manifest provenance source_record_id = %q", got)
	}
	files, ok := manifest["files"].([]any)
	if !ok || len(files) != 1 {
		t.Fatalf("manifest files = %#v", manifest["files"])
	}
	repoPath, err := artifactRepoPath(artifactRoot, initial["namespace"].(string), initial["repo_id"].(string))
	if err != nil {
		t.Fatalf("artifactRepoPath: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(repoPath, "specs", "mvp.yaml"))
	if err != nil {
		t.Fatalf("read artifact file: %v", err)
	}
	if got := string(raw); got != "name: Demo\nrank: 2\n" {
		t.Fatalf("artifact file content = %q", got)
	}

	replayCtx := execCtx
	replayCtx.Request.State.StateCarrier.Fields = cloneStringAnyMap(instance.Fields)
	ok, err = pipelineEngineActionRunner{coordinator: pc}.executeAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, replayCtx)
	if !ok || err != nil {
		t.Fatalf("replay ExecuteAction ok=%v err=%v", ok, err)
	}
	replayed, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load replayed workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(replayed.Fields["current_ref"])); got != ref {
		t.Fatalf("replay current_ref = %q, want %q", got, ref)
	}

	if err := os.MkdirAll(filepath.Join(repoPath, "notes"), 0o755); err != nil {
		t.Fatalf("create extra dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoPath, "notes", "extra.txt"), []byte("should not be committed\n"), 0o644); err != nil {
		t.Fatalf("write extra file: %v", err)
	}
	nextAction, nextCtx := testArtifactRepoActionAndContext(entityID, replayed.Fields, "55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666", "name: Demo\nrank: 3\n")
	ok, err = pipelineEngineActionRunner{coordinator: pc}.executeAction(ctx, nextAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, nextCtx)
	if !ok || err != nil {
		t.Fatalf("next ExecuteAction ok=%v err=%v", ok, err)
	}
	tree, err := runArtifactGit(ctx, repoPath, nil, "ls-tree", "-r", "--name-only", "HEAD")
	if err != nil {
		t.Fatalf("git ls-tree: %v", err)
	}
	if strings.Contains(tree, "notes/extra.txt") {
		t.Fatalf("non-allowlisted file was committed:\n%s", tree)
	}
}

func TestPipelineEngineActionRunner_MockArtifactRepoCommitStopsBeforeActionLaunchSQLitePostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			var cleanup func()
			if backend == "sqlite" {
				var err error
				db, err = sql.Open("sqlite", ":memory:")
				if err != nil {
					t.Fatalf("open sqlite: %v", err)
				}
				cleanup = func() { _ = db.Close() }
			} else {
				_, db, cleanup = testutil.StartPostgres(t)
			}
			defer cleanup()

			artifactRoot := filepath.Join(t.TempDir(), "must-not-exist")
			pc := &PipelineCoordinator{artifactRoot: artifactRoot}
			action, execCtx := testArtifactRepoActionAndContext(
				"22222222-2222-2222-2222-222222222222",
				testArtifactRepoEntityFields(),
				"33333333-3333-3333-3333-333333333333",
				"44444444-4444-4444-4444-444444444444",
				"name: Demo\n",
				executionmode.Mock,
			)
			launches := 0
			runner := pipelineEngineActionRunner{
				coordinator: pc,
				artifactRepoCommit: func(context.Context, runtimecontracts.ActionSpec, runtimeengine.ExecutionContext) (runtimeengine.ActionExecution, error) {
					launches++
					return runtimeengine.ActionExecution{}, nil
				},
			}
			ctx := runtimeeffects.WithExecutionMode(context.Background(), executionmode.Mock)
			claimed, err := runner.executeAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
			if !claimed || err == nil || !strings.Contains(err.Error(), "mock_artifact_repo_commit_forbidden") {
				t.Fatalf("ExecuteAction claimed=%v err=%v", claimed, err)
			}
			if launches != 0 {
				t.Fatalf("artifact action launches = %d, want zero", launches)
			}
			if _, err := os.Stat(artifactRoot); !os.IsNotExist(err) {
				t.Fatalf("artifact root mutation occurred: %v", err)
			}
		})
	}
}

func TestPipelineActionExecutionModeRejectsMissingAndConflictingAuthority(t *testing.T) {
	_, execCtx := testArtifactRepoActionAndContext(
		"22222222-2222-2222-2222-222222222222",
		testArtifactRepoEntityFields(),
		"33333333-3333-3333-3333-333333333333",
		"44444444-4444-4444-4444-444444444444",
		"name: Demo\n",
	)
	missingEventMode := execCtx
	var noEvent events.Event
	missingEventMode.Request.Event = noEvent
	if _, err := pipelineActionExecutionMode(context.Background(), missingEventMode); err == nil || !strings.Contains(err.Error(), "causal execution mode") {
		t.Fatalf("missing causal mode error = %v", err)
	}
	ctx := runtimeeffects.WithExecutionMode(context.Background(), executionmode.Mock)
	if _, err := pipelineActionExecutionMode(ctx, execCtx); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting context mode error = %v", err)
	}
}

func TestResolveArtifactRepoRootDefaultUsesRuntimePrivateRoot(t *testing.T) {
	t.Setenv("SWARM_ARTIFACT_ROOT", "")

	root, err := ResolveArtifactRepoRoot("")
	if err != nil {
		t.Fatalf("ResolveArtifactRepoRoot: %v", err)
	}
	if got, want := root, "/var/lib/swarm/artifacts"; got != want {
		t.Fatalf("default artifact root = %q, want %q", got, want)
	}
}

func TestResolveArtifactRepoRootExplicitOptionOverridesEnv(t *testing.T) {
	t.Setenv("SWARM_ARTIFACT_ROOT", "/data/swarm/artifacts")
	explicit := filepath.Join(t.TempDir(), "artifacts", "..", "repos")

	root, err := ResolveArtifactRepoRoot(explicit)
	if err != nil {
		t.Fatalf("ResolveArtifactRepoRoot: %v", err)
	}
	if got, want := root, filepath.Clean(explicit); got != want {
		t.Fatalf("explicit artifact root = %q, want %q", got, want)
	}
}

func TestResolveArtifactRepoRootRejectsUnsafeRoots(t *testing.T) {
	t.Setenv("SWARM_ARTIFACT_ROOT", "")
	for _, tc := range []struct {
		name string
		root string
		want string
	}{
		{name: "relative", root: "artifacts", want: "absolute runtime-private host path"},
		{name: "data", root: "/data/swarm/artifacts", want: "agent-visible mount /data"},
		{name: "workspace", root: "/workspace/artifacts", want: "agent-visible mount /workspace"},
		{name: "contracts", root: "/opt/swarm/contracts/artifacts", want: "agent-visible mount /opt/swarm/contracts"},
		{name: "prefix", root: "/database/swarm/artifacts", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveArtifactRepoRoot(tc.root)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("ResolveArtifactRepoRoot(%q): %v", tc.root, err)
				}
				if got != filepath.Clean(tc.root) {
					t.Fatalf("ResolveArtifactRepoRoot(%q) = %q", tc.root, got)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ResolveArtifactRepoRoot(%q) error = %v, want %q", tc.root, err, tc.want)
			}
		})
	}
}

func TestEnsureArtifactRepoRootWritableRejectsUnusableRoot(t *testing.T) {
	for _, tc := range []struct {
		name     string
		explicit bool
		source   string
	}{
		{name: "explicit option", explicit: true, source: "explicit runtime ArtifactRoot option"},
		{name: "environment", explicit: false, source: "SWARM_ARTIFACT_ROOT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rootFile := filepath.Join(t.TempDir(), "artifact-root")
			if err := os.WriteFile(rootFile, []byte("not a directory"), 0o644); err != nil {
				t.Fatalf("write root file: %v", err)
			}
			explicit := ""
			t.Setenv("SWARM_ARTIFACT_ROOT", "")
			if tc.explicit {
				explicit = rootFile
			} else {
				t.Setenv("SWARM_ARTIFACT_ROOT", rootFile)
			}

			resolution, err := EnsureArtifactRepoRootWritable(explicit)
			if err == nil {
				t.Fatal("EnsureArtifactRepoRootWritable returned nil error, want unusable root rejection")
			}
			if resolution.Source != tc.source {
				t.Fatalf("source = %q, want %q", resolution.Source, tc.source)
			}
			for _, want := range []string{rootFile, "not writable by the runtime process", "SWARM_ARTIFACT_ROOT=<writable runtime-private absolute path>"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %q, want %q", err.Error(), want)
				}
			}
		})
	}
}

func TestEnsureArtifactRepoRootWritableRejectsBlockedLocalGitStorageBase(t *testing.T) {
	t.Setenv("SWARM_ARTIFACT_ROOT", "")
	root := t.TempDir()
	reposFile := filepath.Join(root, "repos")
	if err := os.WriteFile(reposFile, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write repos file: %v", err)
	}

	resolution, err := EnsureArtifactRepoRootWritable(root)
	if err == nil {
		t.Fatal("EnsureArtifactRepoRootWritable returned nil error, want blocked repos rejection")
	}
	if resolution.Source != "explicit runtime ArtifactRoot option" {
		t.Fatalf("source = %q, want explicit runtime ArtifactRoot option", resolution.Source)
	}
	for _, want := range []string{root, reposFile, "not writable by the runtime process", "SWARM_ARTIFACT_ROOT=<writable runtime-private absolute path>"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want %q", err.Error(), want)
		}
	}
}

func TestSourceUsesArtifactRepoCommitDetectsSupportedActionSurfaces(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"handler": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"artifact.requested": {
						Action: runtimecontracts.ActionSpec{ID: "artifact_repo_commit"},
					},
				},
			},
			"rule": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"artifact.routed": {
						Rules: []runtimecontracts.HandlerRuleEntry{{
							ID:     "commit",
							Action: runtimecontracts.ActionSpec{ID: "artifact_repo_commit"},
						}},
					},
				},
			},
		},
	})
	if !SourceUsesArtifactRepoCommit(source) {
		t.Fatal("SourceUsesArtifactRepoCommit = false, want true for handler/rule action")
	}
	empty := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"handler": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.requested": {Action: runtimecontracts.ActionSpec{ID: "record_evidence"}},
				},
			},
		},
	})
	if SourceUsesArtifactRepoCommit(empty) {
		t.Fatal("SourceUsesArtifactRepoCommit = true, want false without artifact action")
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitRejectsAgentVisibleArtifactRoot(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{workflowStore: store, artifactRoot: "/data/swarm/artifacts", bus: bus, module: &pipelineFixtureWorkflowModule{source: testRootEntityContractSource("artifact-repo", "test_entity")}}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")

	var intents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(withActionEmitIntentCollectorForTest(ctx, &intents), action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	instance, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(instance.Fields["status"])); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	requireArtifactRepoFailure(t, instance.Fields["failure"], runtimefailures.ClassDependencyUnavailable, "artifact_repo_root_unavailable")
	if _, exists := instance.Fields["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted on invalid artifact root: %#v", instance.Fields["current_ref"])
	}
	assertArtifactRepoQueuedIntent(t, intents, 0, "artifact_repo.commit_failed")
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("fallback published event count = %d, want 0", got)
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitRejectsUnusableArtifactRoot(t *testing.T) {
	for _, tc := range []struct {
		name string
		root func(t *testing.T) (root string, wantDetail string)
	}{
		{
			name: "root file",
			root: func(t *testing.T) (string, string) {
				t.Helper()
				rootFile := filepath.Join(t.TempDir(), "artifact-root")
				if err := os.WriteFile(rootFile, []byte("not a directory"), 0o644); err != nil {
					t.Fatalf("write artifact root file: %v", err)
				}
				return rootFile, rootFile
			},
		},
		{
			name: "blocked local git storage base",
			root: func(t *testing.T) (string, string) {
				t.Helper()
				root := t.TempDir()
				reposFile := filepath.Join(root, "repos")
				if err := os.WriteFile(reposFile, []byte("not a directory"), 0o644); err != nil {
					t.Fatalf("write repos file: %v", err)
				}
				return root, reposFile
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			defer cleanup()
			store := newPostgresWorkflowInstanceStoreForTest(db)
			bus := &recordingPipelineBus{}
			artifactRoot, _ := tc.root(t)
			pc := &PipelineCoordinator{workflowStore: store, artifactRoot: artifactRoot, bus: bus, module: &pipelineFixtureWorkflowModule{source: testRootEntityContractSource("artifact-repo", "test_entity")}}
			ctx := testWorkflowStoreRunContext(t, store)
			entityID := "22222222-2222-2222-2222-222222222222"
			initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID:      artifactRepoFixtureRoute(initial),
				EntityID:        entityID,
				StorageRef:      artifactRepoFixtureRoute(initial),
				WorkflowName:    "artifact-repo",
				WorkflowVersion: "1.0.0",
				CurrentState:    "ready",
				Fields:          cloneStringAnyMap(initial),
				EntityType:      "test_entity",
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")

			var intents []runtimeengine.EmitIntent
			ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(withActionEmitIntentCollectorForTest(ctx, &intents), action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
			if !ok || err != nil {
				t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
			}
			instance, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
			if err != nil {
				t.Fatalf("load workflow instance: %v", err)
			}
			if got := strings.TrimSpace(asString(instance.Fields["status"])); got != "failed" {
				t.Fatalf("status = %q, want failed", got)
			}
			requireArtifactRepoFailure(t, instance.Fields["failure"], runtimefailures.ClassDependencyUnavailable, "artifact_repo_root_unavailable")
			assertArtifactRepoQueuedIntent(t, intents, 0, "artifact_repo.commit_failed")
			if got := bus.publishedCount(); got != 0 {
				t.Fatalf("fallback published event count = %d, want 0", got)
			}
		})
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitQueuesSuccessResultEvent(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	bus := &recordingPipelineBus{}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		workflowStore: store,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        handlerTestWorkflowModuleWithBundle(bundle, "artifact-repo", "artifact-node"),
		entityLocks:   map[string]*sync.Mutex{},
	}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.SuccessEvent = "artifact_repo.commit_completed"
	action.ArtifactRepo.SuccessPayload = map[string]runtimecontracts.ExpressionValue{
		"result_kind": runtimecontracts.LiteralExpression("ready"),
	}

	var intents []runtimeengine.EmitIntent
	actionCtx := withActionEmitIntentCollectorForTest(ctx, &intents)
	ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(actionCtx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v", ok, err)
	}
	resultEvent := assertArtifactRepoQueuedIntent(t, intents, 0, "artifact_repo.commit_completed")
	var payload map[string]any
	if err := json.Unmarshal(resultEvent.Payload(), &payload); err != nil {
		t.Fatalf("success event payload: %v", err)
	}
	if got := strings.TrimSpace(asString(payload["repo_id"])); got != initial["repo_id"].(string) {
		t.Fatalf("success payload repo_id = %q", got)
	}
	if got := strings.TrimSpace(asString(payload["result_kind"])); got != "ready" {
		t.Fatalf("success payload result_kind = %q", got)
	}
	if got := strings.TrimSpace(asString(payload["current_ref"])); len(got) != 40 {
		t.Fatalf("success payload current_ref = %q", got)
	}
	if _, ok := payload["file_manifest"].(map[string]any); !ok {
		t.Fatalf("success payload file_manifest = %#v", payload["file_manifest"])
	}
	if _, exists := payload["vertical_id"]; exists {
		t.Fatalf("success payload contains product vertical_id: %#v", payload)
	}
	committed, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load committed workflow instance: %v", err)
	}

	replayCtx := execCtx
	replayCtx.Request.State.StateCarrier.Fields = cloneStringAnyMap(committed.Fields)
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).executeAction(actionCtx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, replayCtx); err != nil {
		t.Fatalf("same-source replay ExecuteAction: %v", err)
	}
	if got := len(intents); got != 1 {
		t.Fatalf("same-source replay queued success event count = %d, want 1", got)
	}

	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("reset workflow instance to simulate DB/git split: %v", err)
	}
	repairAction, repairCtx := testArtifactRepoActionAndContext(entityID, initial, "55555555-5555-5555-5555-555555555555", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	repairAction.ArtifactRepo.SuccessEvent = action.ArtifactRepo.SuccessEvent
	repairAction.ArtifactRepo.SuccessPayload = action.ArtifactRepo.SuccessPayload
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).executeAction(actionCtx, repairAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, repairCtx); err != nil {
		t.Fatalf("history repair ExecuteAction: %v", err)
	}
	if got := len(intents); got != 2 {
		t.Fatalf("history repair queued success event count = %d, want 2", got)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("fallback published event count = %d, want 0", got)
	}
}

func TestExecuteNodeContractHandlerArtifactRepoCommitQueuesSuccessResultThroughOutbox(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	workflowStore := newPostgresWorkflowInstanceStoreForTest(db)
	bus := &recordingPipelineBus{}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		workflowStore: workflowStore,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        handlerTestWorkflowModuleWithBundle(bundle, "artifact-repo", "artifact-node"),
		entityLocks:   map[string]*sync.Mutex{},
	}
	ctx := testWorkflowStoreRunContext(t, workflowStore)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.SuccessEvent = "artifact_repo.commit_completed"
	action.ArtifactRepo.SuccessPayload = map[string]runtimecontracts.ExpressionValue{
		"result_kind": runtimecontracts.LiteralExpression("ready"),
	}
	sourceEvent := testProjectionEventWithSourceAgent(execCtx.Request.Event, "test")
	seedPipelineEventRecord(t, ctx, db, sourceEvent)

	result, err := pc.executeNodeContractHandler(ctx, pipelineSourceNode(t, pc.SemanticSource(), "artifact-repo", "artifact-node"), runtimecontracts.SystemNodeEventHandler{
		Action: action,
	}, workflowTriggerContext{
		Event: execCtx.Request.Event,
		State: WorkflowState{Stage: "working", Metadata: cloneStringAnyMap(initial)},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.outboxCount(); got != 1 {
		t.Fatalf("outbox result event count = %d, want 1 (published=%d actions=%v)", got, bus.publishedCount(), result.Outcome.ActionsExecuted)
	}
	if got := string(bus.outboxIntent(0).Event.Type()); got != "artifact-repo/artifact_repo.commit_completed" {
		t.Fatalf("outbox result event type = %q", got)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("post-commit published result event count = %d, want 1", got)
	}
	committed, _, err := workflowStore.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load committed workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(committed.Fields["last_source_event_id"])); got != execCtx.Request.Event.ID() {
		t.Fatalf("last_source_event_id = %q, want %q", got, execCtx.Request.Event.ID())
	}
}

func TestExecuteNodeContractHandlerArtifactRepoCommitQueuesFailureResultThroughOutbox(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	workflowStore := newPostgresWorkflowInstanceStoreForTest(db)
	bus := &recordingPipelineBus{}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		workflowStore: workflowStore,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        handlerTestWorkflowModuleWithBundle(bundle, "artifact-repo", "artifact-node"),
		entityLocks:   map[string]*sync.Mutex{},
	}
	ctx := testWorkflowStoreRunContext(t, workflowStore)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.Files[0].Path = runtimecontracts.LiteralExpression("../escape.yaml")
	sourceEvent := testProjectionEventWithSourceAgent(execCtx.Request.Event, "test")
	seedPipelineEventRecord(t, ctx, db, sourceEvent)

	result, err := pc.executeNodeContractHandler(ctx, pipelineSourceNode(t, pc.SemanticSource(), "artifact-repo", "artifact-node"), runtimecontracts.SystemNodeEventHandler{
		Action: action,
	}, workflowTriggerContext{
		Event: execCtx.Request.Event,
		State: WorkflowState{Stage: "working", Metadata: cloneStringAnyMap(initial)},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.outboxCount(); got != 1 {
		t.Fatalf("outbox result event count = %d, want 1 (published=%d actions=%v)", got, bus.publishedCount(), result.Outcome.ActionsExecuted)
	}
	if got := string(bus.outboxIntent(0).Event.Type()); got != "artifact-repo/artifact_repo.commit_failed" {
		t.Fatalf("outbox result event type = %q", got)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("post-commit published result event count = %d, want 1", got)
	}
	committed, _, err := workflowStore.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load committed workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(committed.Fields["status"])); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	requireArtifactRepoFailure(t, committed.Fields["failure"], runtimefailures.ClassSchemaInvalid, "artifact_repo_file_invalid")
	if got := strings.TrimSpace(asString(committed.Fields["last_source_event_id"])); got != execCtx.Request.Event.ID() {
		t.Fatalf("last_source_event_id = %q, want %q", got, execCtx.Request.Event.ID())
	}
	if _, exists := committed.Fields["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted on failed commit: %#v", committed.Fields["current_ref"])
	}
}

func TestExecuteNodeContractHandlerArtifactRepoCommitFailureResultOutboxFailureRollsBackState(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	workflowStore := newPostgresWorkflowInstanceStoreForTest(db)
	bus := &recordingPipelineBus{outboxErr: errors.New("outbox unavailable")}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		workflowStore: workflowStore,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        handlerTestWorkflowModuleWithBundle(bundle, "artifact-repo", "artifact-node"),
		entityLocks:   map[string]*sync.Mutex{},
	}
	ctx := testWorkflowStoreRunContext(t, workflowStore)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.Files[0].Path = runtimecontracts.LiteralExpression("../escape.yaml")
	sourceEvent := testProjectionEventWithSourceAgent(execCtx.Request.Event, "test")
	seedPipelineEventRecord(t, ctx, db, sourceEvent)

	_, err := pc.executeNodeContractHandler(ctx, pipelineSourceNode(t, pc.SemanticSource(), "artifact-repo", "artifact-node"), runtimecontracts.SystemNodeEventHandler{
		Action: action,
	}, workflowTriggerContext{
		Event: execCtx.Request.Event,
		State: WorkflowState{Stage: "working", Metadata: cloneStringAnyMap(initial)},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "outbox unavailable") {
		t.Fatalf("executeNodeContractHandler error = %v, want outbox unavailable", err)
	}
	if got := bus.outboxCount(); got != 0 {
		t.Fatalf("outbox result event count = %d, want 0", got)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("post-commit published result event count = %d, want 0", got)
	}
	rolledBack, _, err := workflowStore.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if _, exists := rolledBack.Fields["status"]; exists {
		t.Fatalf("status should roll back with failed outbox write: %#v", rolledBack.Fields["status"])
	}
	if _, exists := rolledBack.Fields["failure"]; exists {
		t.Fatalf("failure should roll back with failed outbox write: %#v", rolledBack.Fields["failure"])
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitReturnsExplicitResultEvent(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	bus := &recordingPipelineBus{publishErr: errors.New("direct publish must not be used")}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		workflowStore: store,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        &previewWorkflowModule{bundle: bundle},
	}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.SuccessEvent = "artifact_repo.commit_completed"
	action.ArtifactRepo.SuccessPayload = map[string]runtimecontracts.ExpressionValue{
		"result_kind": runtimecontracts.LiteralExpression("ready"),
	}

	result, err := (pipelineEngineActionRunner{coordinator: pc}).ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !result.Handled {
		t.Fatal("expected artifact_repo_commit action to be claimed")
	}
	if err != nil {
		t.Fatalf("ExecuteAction: %v", err)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("result emit intents = %d, want 1", len(result.EmitIntents))
	}
	if result.State == nil {
		t.Fatal("artifact action did not return projected state")
	}
	if got := strings.TrimSpace(asString(result.State.StateCarrier.Fields["status"])); got != "committed" {
		t.Fatalf("status = %q, want committed", got)
	}
	if _, exists := result.State.StateCarrier.Fields["current_ref"]; !exists {
		t.Fatal("current_ref was not projected")
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("fallback published event count = %d, want 0", got)
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitFailsClosedOnInvalidSuccessResultEvent(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	bus := &recordingPipelineBus{}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		workflowStore: store,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        &previewWorkflowModule{bundle: bundle},
	}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.SuccessEvent = "artifact_repo.commit_completed"

	var intents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(withActionEmitIntentCollectorForTest(ctx, &intents), action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	instance, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if _, exists := instance.Fields["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted on invalid success event: %#v", instance.Fields["current_ref"])
	}
	if got := strings.TrimSpace(asString(instance.Fields["status"])); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	requireArtifactRepoFailure(t, instance.Fields["failure"], runtimefailures.ClassSchemaInvalid, "artifact_repo_result_schema_invalid")
	assertArtifactRepoQueuedIntent(t, intents, 0, "artifact_repo.commit_failed")
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("fallback published event count = %d, want 0", got)
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitFailsClosedOnPathOutsideAllowlist(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{workflowStore: store, artifactRoot: t.TempDir(), bus: bus, module: &pipelineFixtureWorkflowModule{source: testRootEntityContractSource("artifact-repo", "test_entity")}}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.Files[0].Path = runtimecontracts.LiteralExpression("../escape.yaml")

	var intents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(withActionEmitIntentCollectorForTest(ctx, &intents), action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	instance, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if _, exists := instance.Fields["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted on failed commit: %#v", instance.Fields["current_ref"])
	}
	if got := strings.TrimSpace(asString(instance.Fields["status"])); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	requireArtifactRepoFailure(t, instance.Fields["failure"], runtimefailures.ClassSchemaInvalid, "artifact_repo_file_invalid")
	failureEvent := assertArtifactRepoQueuedIntent(t, intents, 0, "artifact_repo.commit_failed")
	var payload map[string]any
	if err := json.Unmarshal(failureEvent.Payload(), &payload); err != nil {
		t.Fatalf("failure event payload: %v", err)
	}
	requireArtifactRepoFailure(t, payload["failure"], runtimefailures.ClassSchemaInvalid, "artifact_repo_file_invalid")
	if got := strings.TrimSpace(asString(payload["namespace"])); got != initial["namespace"].(string) {
		t.Fatalf("failure payload namespace = %q", got)
	}
	if _, exists := payload["vertical_id"]; exists {
		t.Fatalf("failure payload contains product vertical_id: %#v", payload)
	}
	provenance, ok := payload["provenance"].(map[string]any)
	if !ok {
		t.Fatalf("failure payload provenance = %#v", payload["provenance"])
	}
	if got := strings.TrimSpace(asString(provenance["source_record_id"])); got != initial["source_record_id"].(string) {
		t.Fatalf("failure payload provenance source_record_id = %q", got)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("fallback published event count = %d, want 0", got)
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitFailsClosedOnYAMLSchemaMismatch(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{workflowStore: store, artifactRoot: t.TempDir(), bus: bus, module: &pipelineFixtureWorkflowModule{source: testRootEntityContractSource("artifact-repo", "test_entity")}}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "rank: 2\n")

	var intents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(withActionEmitIntentCollectorForTest(ctx, &intents), action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	instance, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(instance.Fields["status"])); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	if _, exists := instance.Fields["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted on failed commit: %#v", instance.Fields["current_ref"])
	}
	requireArtifactRepoFailure(t, instance.Fields["failure"], runtimefailures.ClassSchemaInvalid, "artifact_repo_file_invalid")
	assertArtifactRepoQueuedIntent(t, intents, 0, "artifact_repo.commit_failed")
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitRejectsRequestIDContentConflict(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	pc := &PipelineCoordinator{workflowStore: store, artifactRoot: t.TempDir(), module: &pipelineFixtureWorkflowModule{source: testRootEntityContractSource("artifact-repo", "test_entity")}}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).executeAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx); err != nil {
		t.Fatalf("initial ExecuteAction: %v", err)
	}
	instance, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}

	nextAction, nextCtx := testArtifactRepoActionAndContext(entityID, instance.Fields, "55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666", "name: Next\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).executeAction(ctx, nextAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, nextCtx); err != nil {
		t.Fatalf("next ExecuteAction: %v", err)
	}
	afterNext, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance after next request: %v", err)
	}
	// Display labels are cosmetic; request history must remain keyed by repo identity.
	afterNext.Fields["display_slug"] = "Renamed Artifact"

	conflictAction, conflictCtx := testArtifactRepoActionAndContext(entityID, afterNext.Fields, "77777777-7777-7777-7777-777777777777", "44444444-4444-4444-4444-444444444444", "name: Changed\n")
	var conflictIntents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(withActionEmitIntentCollectorForTest(ctx, &conflictIntents), conflictAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, conflictCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	assertArtifactRepoQueuedIntent(t, conflictIntents, 0, "artifact_repo.commit_failed")
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitRecordsNoDiffRequestHistory(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{workflowStore: store, artifactRoot: t.TempDir(), bus: bus, module: &pipelineFixtureWorkflowModule{source: testRootEntityContractSource("artifact-repo", "test_entity")}}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).executeAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx); err != nil {
		t.Fatalf("initial ExecuteAction: %v", err)
	}
	instance, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	initialRef := strings.TrimSpace(asString(instance.Fields["current_ref"]))

	sameAction, sameCtx := testArtifactRepoActionAndContext(entityID, instance.Fields, "55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666", "name: Demo\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).executeAction(ctx, sameAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, sameCtx); err != nil {
		t.Fatalf("same-tree ExecuteAction: %v", err)
	}
	afterSame, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance after same-tree request: %v", err)
	}
	sameRef := strings.TrimSpace(asString(afterSame.Fields["current_ref"]))
	if sameRef == "" || sameRef == initialRef {
		t.Fatalf("same-tree request current_ref = %q, initial ref = %q; want a durable operation commit", sameRef, initialRef)
	}
	artifactRoot, err := pc.artifactRepoRoot()
	if err != nil {
		t.Fatalf("artifactRepoRoot: %v", err)
	}
	repoPath, err := artifactRepoPath(artifactRoot, initial["namespace"].(string), initial["repo_id"].(string))
	if err != nil {
		t.Fatalf("artifactRepoPath: %v", err)
	}
	if _, found, err := artifactRepoRequestRecord(ctx, repoPath, "66666666-6666-6666-6666-666666666666"); err != nil || !found {
		t.Fatalf("artifactRepoRequestRecord found=%v err=%v, want recorded same-tree request", found, err)
	}

	nextAction, nextCtx := testArtifactRepoActionAndContext(entityID, afterSame.Fields, "77777777-7777-7777-7777-777777777777", "88888888-8888-8888-8888-888888888888", "name: Next\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).executeAction(ctx, nextAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, nextCtx); err != nil {
		t.Fatalf("next ExecuteAction: %v", err)
	}
	afterNext, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance after next request: %v", err)
	}

	conflictAction, conflictCtx := testArtifactRepoActionAndContext(entityID, afterNext.Fields, "99999999-9999-9999-9999-999999999999", "66666666-6666-6666-6666-666666666666", "name: Changed\n")
	var conflictIntents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(withActionEmitIntentCollectorForTest(ctx, &conflictIntents), conflictAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, conflictCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	assertArtifactRepoQueuedIntent(t, conflictIntents, 0, "artifact_repo.commit_failed")
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("fallback published event count = %d, want 0", got)
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitRepairsDBStateFromGitHistory(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	pc := &PipelineCoordinator{workflowStore: store, artifactRoot: t.TempDir(), module: &pipelineFixtureWorkflowModule{source: testRootEntityContractSource("artifact-repo", "test_entity")}}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).executeAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx); err != nil {
		t.Fatalf("initial ExecuteAction: %v", err)
	}
	committed, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load committed workflow instance: %v", err)
	}
	ref := strings.TrimSpace(asString(committed.Fields["current_ref"]))
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("reset workflow instance to simulate DB/git split: %v", err)
	}

	repairAction, repairCtx := testArtifactRepoActionAndContext(entityID, initial, "55555555-5555-5555-5555-555555555555", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(ctx, repairAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, repairCtx)
	if !ok || err != nil {
		t.Fatalf("repair ExecuteAction ok=%v err=%v", ok, err)
	}
	repaired, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load repaired workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(repaired.Fields["current_ref"])); got != ref {
		t.Fatalf("repaired current_ref = %q, want %q", got, ref)
	}
	if got := strings.TrimSpace(asString(repaired.Fields["status"])); got != "committed" {
		t.Fatalf("repaired status = %q, want committed", got)
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitEnforcesProjectedRepoSize(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	pc := &PipelineCoordinator{workflowStore: store, artifactRoot: t.TempDir(), module: &pipelineFixtureWorkflowModule{source: testRootEntityContractSource("artifact-repo", "test_entity")}}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFieldsForSource(pc.SemanticSource(), entityID)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      artifactRepoFixtureRoute(initial),
		EntityID:        entityID,
		StorageRef:      artifactRepoFixtureRoute(initial),
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Fields:          cloneStringAnyMap(initial),
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.Limits.MaxRepoBytes = 1024
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).executeAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx); err != nil {
		t.Fatalf("initial ExecuteAction: %v", err)
	}
	instance, _, err := store.Load(ctx, testWorkflowInstanceRoute(artifactRepoFixtureRoute(initial)))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}

	nextAction, nextCtx := testArtifactRepoActionAndContext(entityID, instance.Fields, "55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666", "name: unused\n")
	nextAction.ArtifactRepo.AllowedPaths = append(nextAction.ArtifactRepo.AllowedPaths, "artifacts/extra.txt")
	nextAction.ArtifactRepo.Files = []runtimecontracts.ArtifactRepoFileSpec{{
		Path:        runtimecontracts.LiteralExpression("artifacts/extra.txt"),
		Content:     runtimecontracts.LiteralExpression("xxxxxxxxxxxxxxxxxxxxxxxxx"),
		ContentType: "text",
	}}
	nextAction.ArtifactRepo.Limits.MaxRepoBytes = 30
	var intents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.executeAction(withActionEmitIntentCollectorForTest(ctx, &intents), nextAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, nextCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	assertArtifactRepoQueuedIntent(t, intents, 0, "artifact_repo.commit_failed")
}

func TestWriteArtifactRepoFilesRejectsSymlinkEscape(t *testing.T) {
	repoPath := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(repoPath, "specs")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	err := writeArtifactRepoFiles(repoPath, []artifactRepoPreparedFile{{
		Path:    "specs/mvp.yaml",
		Content: []byte("name: Demo\n"),
	}})

	if err == nil || !strings.Contains(err.Error(), "escaped repo root through symlink") {
		t.Fatalf("writeArtifactRepoFiles error = %v, want symlink escape", err)
	}
}

func TestArtifactRepoPathRejectsUnsafeGenericSegments(t *testing.T) {
	repoID := "11111111-1111-1111-1111-111111111111"
	_, err := artifactRepoPath(t.TempDir(), "../escape", repoID)
	if err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("artifactRepoPath error = %v, want namespace", err)
	}
}

func testProjectionEventWithSourceAgent(evt events.Event, sourceAgent string) events.Event {
	return eventtest.RunCreatingRootIngress(
		evt.ID(),
		evt.Type(),
		sourceAgent,
		evt.TaskID(),
		evt.Payload(),
		evt.ChainDepth(),
		evt.RunID(),
		evt.ParentEventID(),
		evt.Envelope(),
		evt.CreatedAt())

}

func assertArtifactRepoQueuedIntent(t *testing.T, intents []runtimeengine.EmitIntent, index int, eventType string) events.Event {
	t.Helper()
	if len(intents) <= index {
		t.Fatalf("queued artifact result intents = %d, want index %d for %s", len(intents), index, eventType)
	}
	evt := intents[index].Event
	if got := strings.TrimSpace(string(evt.Type())); got != eventType {
		t.Fatalf("queued artifact result event type = %q, want %q", got, eventType)
	}
	if got := strings.TrimSpace(intents[index].ParentEventID); got == "" {
		t.Fatalf("queued artifact result parent_event_id is empty for %s", eventType)
	}
	return evt
}

func testArtifactRepoResultEventSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadWorkflowTempSource(t, map[string]string{
		"package.yaml":  "name: artifact-repo\nversion: 1.0.0\ndescription: artifact result event fixture\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n",
		"schema.yaml":   "initial_state: ready\nterminal_states: [ready]\nstates: [ready]\n",
		"entities.yaml": "test_entity: {}\n",
		"nodes.yaml":    "artifact-node:\n  id: artifact-node\n  execution_type: system_node\n",
		"types.yaml": `types:
  ArtifactProvenance:
    artifact_type: text
    source_record_id: text
  ArtifactManifestFile:
    path: text
    content_type: text
    sha256: text
    size_bytes: integer
  ArtifactManifest:
    provider: text
    repo_id: text
    namespace: text
    partition_key: text
    display_slug: text
    request_id: text
    source_event_id: text
    repo_url: text
    ref: text
    tree_hash: text
    files: [ArtifactManifestFile]
    provenance: ArtifactProvenance
`,
		"events.yaml": `artifact_repo.commit_requested:
  request_id: string
  mvp_yaml: string
artifact_repo.commit_completed:
  repo_id: string
  namespace: string
  partition_key: string?
  display_slug: string?
  request_id: string
  source_event_id: string
  repo_url: string
  current_ref: string
  file_manifest: ArtifactManifest
  provenance: ArtifactProvenance
  result_kind: string
artifact_repo.commit_failed:
  repo_id: string
  namespace: string
  partition_key: string?
  display_slug: string?
  request_id: string
  source_event_id: string
  failure: platform.failure/v1 envelope
  provenance: ArtifactProvenance
  request_copy: string?
`,
	})
}

func testArtifactRepoEntityFields(entityIDs ...string) map[string]any {
	fields := map[string]any{
		"repo_id":          "11111111-1111-1111-1111-111111111111",
		"namespace":        "tenant-alpha",
		"partition_key":    "project-42",
		"display_slug":     "Demo Artifact",
		"source_record_id": "record-123",
	}
	if len(entityIDs) > 0 {
		fields["entity_id"] = strings.TrimSpace(entityIDs[0])
		fields["flow_path"] = "artifact-repo"
		fields["instance_id"] = "artifact-repo"
	}
	return fields
}

func testArtifactRepoEntityFieldsForSource(source semanticview.Source, entityID string) map[string]any {
	fields := testArtifactRepoEntityFields(entityID)
	if source != nil && strings.TrimSpace(source.WorkflowName()) == "artifact-repo" {
		fields["flow_path"] = testPipelineRunID
		fields["instance_id"] = testPipelineRunID
	}
	return fields
}

func artifactRepoFixtureRoute(entity map[string]any) string {
	if route := strings.Trim(strings.TrimSpace(asString(entity["flow_path"])), "/"); route != "" {
		return route
	}
	return "artifact-repo"
}

func requireArtifactRepoFailure(t testing.TB, value any, class runtimefailures.Class, code string) runtimefailures.Envelope {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal artifact failure: %v", err)
	}
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("decode artifact failure: %v (value=%#v)", err, value)
	}
	if failure.Class != class || failure.Detail.Code != code {
		t.Fatalf("artifact failure = %s/%s, want %s/%s", failure.Class, failure.Detail.Code, class, code)
	}
	return failure
}

func testArtifactRepoActionAndContext(entityID string, entity map[string]any, eventID, requestID, content string, modes ...executionmode.Mode) (runtimecontracts.ActionSpec, runtimeengine.ExecutionContext) {
	mode := executionmode.Live
	if len(modes) > 0 {
		mode = modes[0]
	}
	payload := map[string]any{
		"request_id": requestID,
		"mvp_yaml":   content,
	}
	route := artifactRepoFixtureRoute(entity)
	payloadBytes, _ := json.Marshal(payload)
	evt := eventtest.RunCreatingRootIngressWithMode(
		eventID,
		"artifact_repo.commit_requested",
		"",
		"",
		payloadBytes,
		0,
		testPipelineRunID,
		"",
		handlerTestWorkflowEnvelope("artifact-repo", route, entityID),
		time.Unix(1_700_000_000, 0).UTC(),
		mode,
	)

	base := values.NewContext()
	base.Event = values.Wrap(evt.ContextMap("ready"))
	base.Payload = values.Wrap(payload)
	base.Entity = values.Wrap(entity)
	stateMetadata := cloneStringAnyMap(entity)
	return runtimecontracts.ActionSpec{
			ID: "artifact_repo_commit",
			ArtifactRepo: &runtimecontracts.ArtifactRepoSpec{
				Provider:     "local_git",
				RepoID:       runtimecontracts.RefExpression("entity.repo_id"),
				Namespace:    runtimecontracts.RefExpression("entity.namespace"),
				PartitionKey: runtimecontracts.RefExpression("entity.partition_key"),
				DisplaySlug:  runtimecontracts.RefExpression("entity.display_slug"),
				RequestID:    runtimecontracts.RefExpression("payload.request_id"),
				Author:       runtimecontracts.LiteralExpression("artifact-writer"),
				Provenance: map[string]runtimecontracts.ExpressionValue{
					"artifact_type":    runtimecontracts.LiteralExpression("fixture"),
					"source_record_id": runtimecontracts.RefExpression("entity.source_record_id"),
				},
				AllowedPaths: []string{"specs/mvp.yaml"},
				Files: []runtimecontracts.ArtifactRepoFileSpec{{
					Path:        runtimecontracts.LiteralExpression("specs/mvp.yaml"),
					Content:     runtimecontracts.RefExpression("payload.mvp_yaml"),
					ContentType: "yaml",
					Schema: runtimecontracts.ArtifactRepoSchemaSpec{
						Type:           "object",
						RequiredFields: []string{"name"},
					},
					MaxBytes: 4096,
				}},
				Output: runtimecontracts.ArtifactRepoOutputSpec{
					RepoURL:           "repo_url",
					CurrentRef:        "current_ref",
					FileManifest:      "file_manifest",
					Status:            "status",
					Failure:           "failure",
					LastRequestID:     "last_request_id",
					LastSourceEventID: "last_source_event_id",
				},
				Limits: runtimecontracts.ArtifactRepoLimitsSpec{
					MaxYAMLBytes: 4096,
					MaxRepoBytes: 1048576,
				},
				FailureEvent: "artifact_repo.commit_failed",
				FailurePayload: map[string]runtimecontracts.ExpressionValue{
					"request_copy": runtimecontracts.RefExpression("payload.request_id"),
				},
			},
		}, runtimeengine.ExecutionContext{
			Base: base,
			Request: runtimeengine.ExecutionRequest{
				EntityID: identity.NormalizeEntityID(entityID),
				Node:     mustPipelineNode("", "artifact-node"),
				Route:    runtimeflowidentity.RouteForInstancePath(route),
				Event:    evt,
				ProducerSource: mustStaticExecutionRoutingSource(events.RouteIdentity{
					FlowID:       "artifact-repo",
					FlowInstance: route,
					EntityID:     entityID,
				}),
				State: runtimeengine.StateSnapshot{
					EntityID:        identity.NormalizeEntityID(entityID),
					WorkflowName:    "artifact-repo",
					WorkflowVersion: "1.0.0",
					CurrentState:    "ready",
					StateCarrier:    runtimeengine.NewStateCarrier(stateMetadata, nil, nil),
				},
			},
		}
}

func mustStaticExecutionRoutingSource(route events.RouteIdentity) events.RoutingSource {
	source, err := events.NewStaticFlowRoutingSource(route)
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
	)
	if err != nil {
		t.Fatalf("EvalBool error = %v", err)
	}
	if !ok {
		t.Fatal("expected CEL accumulated scope to expose the accumulated item list explicitly")
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

func TestPipelineEnginePayloadShaper_AllowsDeclaredPayloadOnActionSurface(t *testing.T) {
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

	actionCtx := runtimeengine.WithEmitSurface(testAuthorActivityContext(t, context.Background()), runtimeengine.EmitSurfaceAction)
	payload, err := shaper.ShapeEmitPayload(actionCtx, req, "child/child.done", map[string]any{})
	if err != nil {
		t.Fatalf("ShapeEmitPayload action surface: %v", err)
	}
	if len(payload) != 0 {
		t.Fatalf("action payload = %#v, want declared business payload only", payload)
	}
}

func TestPipelineEnginePayloadShaper_RejectsMissingRequiredFieldsOnActionSurface(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml":             "name: action-emit-required\nversion: 1.0.0\ndescription: Action emit required-field proof.\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n- id: child\n  flow: child\n  mode: static\n",
		"schema.yaml":              "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n  outputs:\n    events: [parent.result]\n",
		"events.yaml":              "parent.trigger:\n  entity_id: string\nparent.result:\n  entity_id: string\n",
		"flows/child/package.yaml": "name: child\nversion: 1.0.0\ndescription: child flow\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n",
		"flows/child/schema.yaml":  "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.internal]\n",
		"flows/child/events.yaml":  "child.start:\n  entity_id: string\nchild.internal:\n  entity_id: string\n  step: string\n",
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

	actionCtx := runtimeengine.WithEmitSurface(testAuthorActivityContext(t, context.Background()), runtimeengine.EmitSurfaceAction)
	_, err := shaper.ShapeEmitPayload(actionCtx, req, "child/child.internal", map[string]any{
		"entity_id": "ent-child",
	})
	if err == nil {
		t.Fatal("expected action surface missing required field to fail closed")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("ShapeEmitPayload action surface error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}
}

func TestPipelineEnginePayloadShaper_RejectsMissingRequiredFieldsForConcreteTemplateOutput(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml":             "name: template-output-required\nversion: 1.0.0\ndescription: Template output required-field proof.\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n- id: child\n  flow: child\n  mode: template\n",
		"schema.yaml":              "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n",
		"events.yaml":              "parent.trigger:\n  entity_id: string\n",
		"flows/child/package.yaml": "name: child\nversion: 1.0.0\ndescription: child flow\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n",
		"flows/child/schema.yaml":  "name: child\nmode: template\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.done]\n",
		"flows/child/events.yaml":  "child.start:\n  entity_id: string\nchild.done:\n  step: string\n",
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

func TestPipelineEnginePayloadShaper_RejectsEnvelopeOnlyRequiredFieldOnActionSurface(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml":             "name: action-emit-envelope-required\nversion: 1.0.0\ndescription: Action emit envelope-required proof.\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n- id: child\n  flow: child\n  mode: static\n",
		"schema.yaml":              "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n  outputs:\n    events: [parent.result]\n",
		"events.yaml":              "parent.trigger:\n  entity_id: string\nparent.result:\n  entity_id: string\n",
		"flows/child/package.yaml": "name: child\nversion: 1.0.0\ndescription: child flow\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n",
		"flows/child/schema.yaml":  "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.internal]\n",
		"flows/child/events.yaml":  "child.start:\n  entity_id: string\nchild.internal:\n  entity_id: string\n",
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

	actionCtx := runtimeengine.WithEmitSurface(testAuthorActivityContext(t, context.Background()), runtimeengine.EmitSurfaceAction)
	_, err := shaper.ShapeEmitPayload(actionCtx, req, "child/child.internal", map[string]any{})
	if err == nil {
		t.Fatal("expected action surface envelope-only required field to fail closed")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("ShapeEmitPayload action surface error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}
}

func TestValidatePipelineEmitPayload_RejectsEnumViolationOnActionSurface(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml":             "name: action-emit-enum\nversion: 1.0.0\ndescription: Action emit enum proof.\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n- id: child\n  flow: child\n  mode: static\n",
		"schema.yaml":              "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n  outputs:\n    events: [parent.result]\n",
		"events.yaml":              "parent.trigger:\n  entity_id: string\nparent.result:\n  entity_id: string\n",
		"types.yaml":               "enums:\n  Mode:\n    values: [fast, deep]\n    default: fast\n",
		"flows/child/package.yaml": "name: child\nversion: 1.0.0\ndescription: child flow\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n",
		"flows/child/schema.yaml":  "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.internal]\n",
		"flows/child/events.yaml":  "child.start:\n  entity_id: string\nchild.internal:\n  mode: Mode\n",
	})

	err := validatePipelineEmitPayload(source, "child", "child.internal", map[string]any{
		"mode": "invalid",
	}, nil, runtimeengine.EmitSurfaceAction)
	if err == nil {
		t.Fatal("expected enum violation to fail closed on the action surface")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("validatePipelineEmitPayload error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}
	if !strings.Contains(err.Error(), "invalid enum value") {
		t.Fatalf("validatePipelineEmitPayload error = %v, want enum detail", err)
	}
}

func TestPipelineEnginePayloadShaper_UsesRootNamedTypeSchemaForChildOutput(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml":             "name: child-output-named-type\nversion: 1.0.0\ndescription: child output named type proof\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n- id: child\n  flow: child\n  mode: static\n",
		"schema.yaml":              "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  outputs:\n    events: [handoff.completed]\n",
		"types.yaml":               "types:\n  Evidence:\n    root_field: text\n",
		"events.yaml":              "handoff.completed:\n  evidence: Evidence\n",
		"flows/child/package.yaml": "name: child\nversion: 1.0.0\ndescription: child flow\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n",
		"flows/child/schema.yaml":  "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  outputs:\n    events: [handoff.completed]\n",
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
			payload, err := shaper.ShapeEmitPayload(testAuthorActivityContext(t, context.Background()), req, eventType, map[string]any{
				"evidence": map[string]any{"root_field": "ok"},
			})
			if err != nil {
				t.Fatalf("ShapeEmitPayload valid root named type: %v", err)
			}
			evidence, _ := payload["evidence"].(map[string]any)
			if _, ok := evidence["root_field"]; !ok {
				t.Fatalf("payload = %#v, want root_field evidence", payload)
			}

			_, err = shaper.ShapeEmitPayload(testAuthorActivityContext(t, context.Background()), req, eventType, map[string]any{
				"evidence": map[string]any{"child_field": "wrong catalog"},
			})
			if err == nil {
				t.Fatal("expected child Evidence override to fail for root-declared output event")
			}
			if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
				t.Fatalf("ShapeEmitPayload invalid catalog error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
			}
			if !strings.Contains(err.Error(), "$.evidence.root_field is required") {
				t.Fatalf("ShapeEmitPayload error = %v, want root_field required proof", err)
			}
		})
	}
}

func TestPipelineEnginePayloadShaper_RejectsUndeclaredFieldsOnActionSurface(t *testing.T) {
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

	actionCtx := runtimeengine.WithEmitSurface(testAuthorActivityContext(t, context.Background()), runtimeengine.EmitSurfaceAction)
	_, err := shaper.ShapeEmitPayload(actionCtx, req, "child/child.done", map[string]any{
		"entity_id":   "ent-child",
		"vertical_id": "ent-child",
		"result":      "accepted",
	})
	if err == nil {
		t.Fatal("expected undeclared action surface fields to fail closed")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("ShapeEmitPayload action surface error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
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

func TestPipelineEngineActionRegistry_SynthesizesSupportedBuiltinActions(t *testing.T) {
	registry := pipelineEngineActionRegistry{}
	for _, builtin := range []string{"create_flow_instance", "mailbox_write"} {
		id := identity.NormalizeActionKey(builtin)
		if !registry.HasAction(id) {
			t.Fatalf("expected builtin action %s to be discoverable without explicit registry entry", builtin)
		}
		if !registry.IsExecutable(id) {
			t.Fatalf("expected builtin action %s to be executable without explicit registry entry", builtin)
		}
		instruction, ok := registry.Action(id)
		if !ok {
			t.Fatalf("expected builtin action %s instruction", builtin)
		}
		if got := instruction.Builtin; got != builtin {
			t.Fatalf("Builtin = %q, want %q", got, builtin)
		}
	}
}

func TestPipelineEngineActionRegistry_DoesNotSynthesizeRemovedBuiltinActions(t *testing.T) {
	registry := pipelineEngineActionRegistry{}
	id := identity.NormalizeActionKey("increment_revision_count")

	if registry.HasAction(id) {
		t.Fatal("did not expect removed builtin action to be discoverable")
	}
	if registry.IsExecutable(id) {
		t.Fatal("did not expect removed builtin action to be executable")
	}
	if _, ok := registry.Action(id); ok {
		t.Fatal("did not expect removed builtin action instruction")
	}
}
