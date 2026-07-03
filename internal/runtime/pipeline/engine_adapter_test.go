package pipeline

import (
	"context"
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
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
)

func testEngineStateMutation(metadata map[string]any, gates map[string]bool, buckets map[string]map[string]any) runtimeengine.StateMutation {
	return runtimeengine.StateMutation{
		StateCarrier: runtimeengine.NewStateCarrier(metadata, gates, buckets),
	}
}

func testEngineStateSnapshot(metadata map[string]any, gates map[string]bool, buckets map[string]map[string]any) runtimeengine.StateSnapshot {
	return runtimeengine.StateSnapshot{
		StateCarrier: runtimeengine.NewStateCarrier(metadata, gates, buckets),
	}
}

func TestApplyEngineStateMutationMirrorsDataAccumulationIntoEntityProjection(t *testing.T) {
	instance := &WorkflowInstance{
		Metadata:     map[string]any{"research_context": map[string]any{"summary": "done"}},
		StateBuckets: map[string]any{},
	}
	mutation := testEngineStateMutation(map[string]any{
		"research_context":              map[string]any{"summary": "done"},
		"last_data_accumulation_event":  "research.completed",
		"last_data_accumulation_source": "research.completed",
	}, nil, nil)
	mutation.DataAccumulation = runtimecontracts.WorkflowDataAccumulation{
		Writes: []runtimecontracts.WorkflowDataWrite{
			{TargetField: "research_context", SourceField: "research_context"},
		},
	}
	applyEngineStateMutation(instance, mutation, map[string]struct{}{"research_context": {}}, nil, "")

	entityProjection, _ := workflowStateBucketObject(*instance, workflowStateBucketEntityProjection)
	got, ok := entityProjection["research_context"].(map[string]any)
	if !ok || got["summary"] != "done" {
		t.Fatalf("entity_projection research_context = %#v", entityProjection["research_context"])
	}
	if got := instance.Metadata["last_data_accumulation_event"]; got != "research.completed" {
		t.Fatalf("last_data_accumulation_event = %#v", got)
	}
}

func TestApplyEngineStateMutationMergesGateDeltasIntoExistingMetadata(t *testing.T) {
	instance := &WorkflowInstance{
		Metadata: map[string]any{
			"gates": map[string]any{
				"g_a": true,
				"g_b": true,
			},
		},
	}
	mutation := testEngineStateMutation(nil, map[string]bool{"g_c": true}, nil)
	mutation.SetGate = "g_c"

	applyEngineStateMutation(instance, mutation, nil, nil, "")

	gates := workflowStateGatesAsBools(instance.Metadata)
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
		Metadata: map[string]any{},
	}
	mutation := testEngineStateMutation(nil, map[string]bool{"g_validated": true}, nil)
	mutation.SetGate = "g_validated"

	applyEngineStateMutation(instance, mutation, nil, source, "child")

	gates := workflowStateGatesAsBools(instance.Metadata)
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
platform_version: ">=1.0.0"
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
		"flows/child/nodes.yaml": "{}\n",
	})
	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &pipelineFixtureWorkflowModule{
			source:   source,
			workflow: NewWorkflowDefinition("runtime-test", []WorkflowStage{{Name: "ready"}}, nil),
		},
	})
	const entityID = "11111111-1111-1111-1111-111111111111"
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      "child/existing",
		WorkflowName:    "child",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Metadata: map[string]any{
			"entity_id":  entityID,
			"request_id": "req-existing",
			"flow_path":  "child/existing",
		},
	}); err != nil {
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
		Metadata: map[string]any{
			"flow_path": "child/inst-1",
		},
	}
	mutation := testEngineStateMutation(nil, map[string]bool{"g_ready": true}, nil)
	mutation.SetGate = "g_ready"

	applyEngineStateMutation(instance, mutation, nil, nil, "")

	if !workflowStateGatesAsBools(instance.Metadata)["g_ready"] {
		t.Fatalf("gates = %#v, want g_ready=true", instance.Metadata["gates"])
	}
}

func TestApplyEngineStateMutationPreservesRuntimeControlMetadata(t *testing.T) {
	instance := &WorkflowInstance{
		Metadata: map[string]any{
			"storage_ref":          "review/inst-1",
			"instance_id":          "inst-1",
			"flow_path":            "review/inst-1",
			"entity_id":            "child-ent",
			"workflow_version":     "v1",
			"template_version":     "tv1",
			"instance_kind":        "materialized",
			"parent_flow_id":       "operating",
			"parent_flow_instance": "operating/root",
			"parent_entity_id":     "parent-ent",
			"business_status":      "old",
		},
	}
	mutation := testEngineStateMutation(map[string]any{
		"parent_flow_id":       "wrong",
		"parent_flow_instance": "wrong/root",
		"parent_entity_id":     "wrong-parent",
		"business_status":      "new",
	}, nil, nil)

	applyEngineStateMutation(instance, mutation, nil, nil, "")

	for key, want := range map[string]any{
		"storage_ref":          "review/inst-1",
		"instance_id":          "inst-1",
		"flow_path":            "review/inst-1",
		"entity_id":            "child-ent",
		"workflow_version":     "v1",
		"template_version":     "tv1",
		"instance_kind":        "materialized",
		"parent_flow_id":       "operating",
		"parent_flow_instance": "operating/root",
		"parent_entity_id":     "parent-ent",
	} {
		if got := instance.Metadata[key]; got != want {
			t.Fatalf("metadata[%s] = %#v, want %#v", key, got, want)
		}
	}
	if got := instance.Metadata["business_status"]; got != "new" {
		t.Fatalf("business_status = %#v, want new", got)
	}
}

func TestApplyEngineStateMutationRejectsAuthoredParentRouteInsertion(t *testing.T) {
	instance := &WorkflowInstance{
		Metadata: map[string]any{
			"business_status": "old",
		},
	}
	mutation := testEngineStateMutation(map[string]any{
		"business_status":      "new",
		"parent_flow_id":       "root",
		"parent_flow_instance": "root/inst-1",
		"parent_entity_id":     "parent-ent",
	}, nil, nil)

	applyEngineStateMutation(instance, mutation, nil, nil, "")

	for _, key := range []string{"parent_flow_id", "parent_flow_instance", "parent_entity_id"} {
		if _, ok := instance.Metadata[key]; ok {
			t.Fatalf("metadata[%s] = %#v, want absent", key, instance.Metadata[key])
		}
	}
	if got := instance.Metadata["business_status"]; got != "new" {
		t.Fatalf("business_status = %#v, want new", got)
	}
	assertMutationParentRoutePinOutputFailure(t, instance.Metadata, runtimepinrouting.FailureTargetRequiredMissing)
}

func TestApplyEngineStateMutationRejectsAuthoredParentRouteCompletion(t *testing.T) {
	instance := &WorkflowInstance{
		Metadata: map[string]any{
			"parent_entity_id": "legacy-parent",
			"business_status":  "old",
		},
	}
	mutation := testEngineStateMutation(map[string]any{
		"business_status":      "new",
		"parent_flow_id":       "root",
		"parent_flow_instance": "root/inst-1",
		"parent_entity_id":     "wrong-parent",
	}, nil, nil)

	applyEngineStateMutation(instance, mutation, nil, nil, "")

	if _, ok := instance.Metadata["parent_flow_id"]; ok {
		t.Fatalf("parent_flow_id = %#v, want absent", instance.Metadata["parent_flow_id"])
	}
	if _, ok := instance.Metadata["parent_flow_instance"]; ok {
		t.Fatalf("parent_flow_instance = %#v, want absent", instance.Metadata["parent_flow_instance"])
	}
	if got := instance.Metadata["parent_entity_id"]; got != "legacy-parent" {
		t.Fatalf("parent_entity_id = %#v, want legacy-parent", got)
	}
	if got := instance.Metadata["business_status"]; got != "new" {
		t.Fatalf("business_status = %#v, want new", got)
	}
	assertMutationParentRoutePinOutputFailure(t, instance.Metadata, runtimepinrouting.FailureParentRouteIncomplete)
}

func assertMutationParentRoutePinOutputFailure(t *testing.T, metadata map[string]any, want runtimepinrouting.TargetFailure) {
	t.Helper()
	route := runtimeflowidentity.ParentRouteFromMetadata(metadata).Normalized()
	result := runtimepinrouting.Resolve(runtimepinrouting.ResolutionInput{
		Source:    mutationParentRoutePinOutputSource(),
		FlowID:    "child",
		EventType: "child.done",
		Emit:      runtimecontracts.EmitSpec{Event: "child.done"},
		ParentRoute: events.RouteIdentity{
			FlowID:       route.FlowID,
			FlowInstance: route.FlowInstance,
			EntityID:     route.EntityID,
		},
	}, eventtest.RootIngress("", events.EventType("child.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if result.Failure != want {
		t.Fatalf("pin output failure = %q, want %q (metadata=%#v)", result.Failure, want, metadata)
	}
}

func mutationParentRoutePinOutputSource() semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "child",
			Flow: "child",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"child.done"},
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
	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
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
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "root",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Metadata:        map[string]any{},
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}

	if err := pc.maybeDeactivateTerminalFlowInstance(testPipelineCoordinatorRunContext(t, pc), entityID, "done"); err != nil {
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
	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
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
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      flowPath,
		WorkflowName:    "review",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Metadata: map[string]any{
			"entity_id":        entityID,
			"instance_id":      "inst-1",
			"flow_path":        flowPath,
			"parent_entity_id": parentEntityID,
		},
	}); err != nil {
		t.Fatalf("seed template instance: %v", err)
	}

	if err := pc.maybeDeactivateTerminalFlowInstance(testPipelineCoordinatorRunContext(t, pc), entityID, "completed"); err != nil {
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

func TestApplyEngineStateMutationInitializesWorkflowInstanceDefaults(t *testing.T) {
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

	applyEngineStateMutation(instance, mutation, map[string]struct{}{"name": {}}, source, "scoring")

	if got := instance.WorkflowName; got != "scoring" {
		t.Fatalf("WorkflowName = %q, want scoring", got)
	}
	if got := instance.WorkflowVersion; got != "7.1.0" {
		t.Fatalf("WorkflowVersion = %q, want 7.1.0", got)
	}
	if got := instance.CurrentState; got != "pending" {
		t.Fatalf("CurrentState = %q, want pending", got)
	}
	if instance.EnteredStageAt.IsZero() {
		t.Fatal("expected EnteredStageAt to be initialized")
	}
}

func TestWorkflowStateGatesForScopeLocalizesDeepScope(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")

	got := workflowStateGatesForScope(source, "grandchild", map[string]any{
		"gates": map[string]any{
			"child/grandchild/g_ready": true,
		},
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
		Metadata: map[string]any{
			"composite_score": 0,
			"gates": map[string]any{
				"g_ready": true,
			},
		},
		StateBuckets: map[string]any{},
	}
	mutation := testEngineStateMutation(map[string]any{
		"composite_score": 71,
		"scoring_rubric":  "corpus_rubric",
	}, nil, nil)

	applyEngineStateMutation(instance, mutation, map[string]struct{}{
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
	if !workflowStateGatesAsBools(instance.Metadata)["g_ready"] {
		t.Fatalf("metadata-only mutation dropped existing gates: %#v", instance.Metadata["gates"])
	}
}

func TestApplyEngineStateMutationDoesNotCaptureSubjectIDFromMetadata(t *testing.T) {
	instance := &WorkflowInstance{}
	mutation := testEngineStateMutation(map[string]any{}, nil, nil)

	applyEngineStateMutation(instance, mutation, nil, nil, "")

	if got := strings.TrimSpace(asString(instance.Metadata["subject_id"])); got != "" {
		t.Fatalf("metadata subject_id = %q, want removed", got)
	}
}

func TestUpdateEntityState_ReturnsWorkflowStoreMutationError(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	pc := &PipelineCoordinator{
		workflowStore: NewWorkflowInstanceStore(db),
		module: &previewWorkflowModule{
			bundle: &runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{
					Name:    "empire",
					Version: "1.0.0",
				},
			},
		},
	}

	err := pc.updateEntityState(testPipelineRunContextNoSeed(), "11111111-1111-1111-1111-111111111111", "marginal_review", "scoring/vertical.marginal")
	if err == nil {
		t.Fatal("expected updateEntityState to fail when workflow store mutate fails")
	}
}

func TestPipelineEngineStateRepoSaveStateRejectsForeignFlowWrite(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	entityID := "11111111-1111-1111-1111-111111111111"
	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "flow-a",
		WorkflowVersion: "1.6.0",
		CurrentState:    "pending",
		Metadata:        map[string]any{},
	}); err != nil {
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
	err := repo.SaveState(ctx, identity.NormalizeEntityID(entityID), testEngineStateMutation(map[string]any{"note": "bad write"}, nil, nil))
	if err == nil || !strings.Contains(err.Error(), "cross_flow_write_forbidden") {
		t.Fatalf("expected cross_flow_write_forbidden, got %v", err)
	}
}

func TestPipelineEngineStateRepoLoadStateMissingEntityDoesNotMaterializeDefaults(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=1.0.0"
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
			workflowStore: NewWorkflowInstanceStore(db),
			module:        &previewWorkflowModule{bundle: bundle},
		},
	}

	loaded, ok, err := repo.LoadState(withPipelineFlowScope(testWorkflowStoreRunContext(t, repo.coordinator.workflowStore), "review"), identity.NormalizeEntityID(FlowInstanceEntityID("review/inst-missing")))
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if ok {
		t.Fatalf("LoadState ok=true for missing entity, loaded metadata=%#v", loaded.Metadata)
	}
}

func TestPipelineEngineStateRepoRoundTripsTypedCarrier(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := NewWorkflowInstanceStore(db)
	repo := pipelineEngineStateRepo{
		coordinator: &PipelineCoordinator{
			workflowStore: store,
		},
	}
	entityID := identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111")
	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      entityID.String(),
		StorageRef:      entityID.String(),
		WorkflowName:    "root",
		WorkflowVersion: "1.0.0",
		CurrentState:    "pending",
		Metadata:        map[string]any{},
		StateBuckets:    map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	mutation := testEngineStateMutation(
		map[string]any{"score": 91, "subject_id": "11111111-1111-1111-1111-111111111111"},
		map[string]bool{"ready": true},
		map[string]map[string]any{"evidence": {"count": 2}},
	)

	if err := repo.SaveState(testWorkflowStoreRunContext(t, repo.coordinator.workflowStore), entityID, mutation); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	loaded, ok, err := repo.LoadState(testWorkflowStoreRunContext(t, repo.coordinator.workflowStore), entityID)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if !ok {
		t.Fatal("expected saved state to load")
	}
	if got := loaded.Metadata["score"]; got != 91 && got != 91.0 {
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
		store := NewWorkflowInstanceStore(db)
		if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
			InstanceID:      "22222222-2222-2222-2222-222222222222",
			StorageRef:      "22222222-2222-2222-2222-222222222222",
			WorkflowName:    "root",
			WorkflowVersion: "1.0.0",
			CurrentState:    "pending",
			StateBuckets: map[string]any{
				"evidence": "bad",
			},
		}); err != nil {
			t.Fatalf("upsert malformed state bucket instance: %v", err)
		}
		repo := pipelineEngineStateRepo{coordinator: &PipelineCoordinator{workflowStore: store}}
		_, _, err := repo.LoadState(testWorkflowStoreRunContext(t, repo.coordinator.workflowStore), identity.NormalizeEntityID("22222222-2222-2222-2222-222222222222"))
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
		workflowStore: NewWorkflowInstanceStore(db),
	}

	err := pc.recordWorkflowEvidence(testPipelineRunContextNoSeed(), "11111111-1111-1111-1111-111111111111", "", "research", map[string]any{"summary": "done"})
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
		workflowStore: NewWorkflowInstanceStore(db),
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
	ok, err := runner.ExecuteAction(context.Background(), runtimecontracts.ActionSpec{ID: "record_evidence"}, runtimeregistry.ActionInstruction{Builtin: "record_evidence"}, runtimeengine.ExecutionContext{
		Request: runtimeengine.ExecutionRequest{
			EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
			NodeID:   identity.NormalizeNodeID("node-a"),
			Event: eventtest.RootIngress(
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

	store := NewWorkflowInstanceStore(db)
	pc := &PipelineCoordinator{workflowStore: store}
	runner := pipelineEngineActionRunner{coordinator: pc}

	tests := []struct {
		name            string
		entityID        string
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
			if err := store.Upsert(ctx, WorkflowInstance{
				InstanceID:      tt.entityID,
				StorageRef:      tt.entityID,
				WorkflowName:    "operating",
				WorkflowVersion: "1.0.0",
				CurrentState:    "initializing",
				Metadata:        map[string]any{},
				StateBuckets:    map[string]any{},
			}); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			ok, err := runner.ExecuteAction(ctx, tt.action, runtimeregistry.ActionInstruction{Builtin: "record_evidence"}, runtimeengine.ExecutionContext{
				Request: runtimeengine.ExecutionRequest{
					EntityID: identity.NormalizeEntityID(tt.entityID),
					NodeID:   identity.NormalizeNodeID("build-orchestrator"),
					Event: eventtest.RootIngress(
						"",
						tt.concreteEvent,
						"",
						"",
						mustJSON(map[string]any{"summary": tt.wantSummary}),
						0,
						"",
						"",
						events.EnvelopeForEntityID(events.EventEnvelope{}, tt.entityID),
						time.Time{},
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

			instance, exists, err := store.Load(ctx, tt.entityID)
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
	evt := eventtest.RootIngress(
		"evt-123",
		"spawn.requested",
		"",
		"",
		[]byte(`{"instance_id":"inst-42","name":"alpha","template_id":"application-basic-v1"}`),
		0,
		"",
		"source-evt-1",
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

	ok, err := runner.ExecuteAction(context.Background(), action, runtimeregistry.ActionInstruction{Builtin: "create_flow_instance"}, runtimeengine.ExecutionContext{
		Base: base,
		Request: runtimeengine.ExecutionRequest{
			EntityID: identity.NormalizeEntityID("ent-1"),
			NodeID:   identity.NormalizeNodeID("spawner"),
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
	ctx := context.Background()
	eventID := "11111111-1111-1111-1111-111111111111"
	entityID := "22222222-2222-2222-2222-222222222222"
	action := runtimecontracts.ActionSpec{
		ID: "mailbox_write",
		Mailbox: &runtimecontracts.MailboxWriteSpec{
			ItemType:     runtimecontracts.LiteralExpression("review_request"),
			Severity:     runtimecontracts.LiteralExpression("urgent"),
			Summary:      runtimecontracts.LiteralExpression("Review validation package"),
			EntityID:     runtimecontracts.RefExpression("event.entity_id"),
			FlowInstance: runtimecontracts.RefExpression("event.flow_instance"),
			Payload: map[string]runtimecontracts.ExpressionValue{
				"review_kind":   runtimecontracts.RefExpression("payload.review_kind"),
				"operator_hint": runtimecontracts.LiteralExpression("inspect_package"),
			},
		},
	}
	evt := eventtest.RootIngress(
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
	execCtx := runtimeengine.ExecutionContext{
		Base: base,
		Request: runtimeengine.ExecutionRequest{
			EntityID: identity.NormalizeEntityID(entityID),
			NodeID:   identity.NormalizeNodeID("mailbox-node"),
			Event:    evt,
		},
	}
	for i := 0; i < 2; i++ {
		ok, err := runner.ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "mailbox_write"}, execCtx)
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
	if got.ItemID != deterministicMailboxItemID(eventID, "mailbox-node") {
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
	if got.FromAgent != "system_node:mailbox-node" {
		t.Fatalf("from_agent = %q", got.FromAgent)
	}
}

func TestPipelineEngineActionRunner_MailboxWriteFailsClosedOnMissingRequiredExpression(t *testing.T) {
	materializer := &recordingMailboxWriteMaterializer{}
	runner := pipelineEngineActionRunner{coordinator: &PipelineCoordinator{mailboxMaterializer: materializer}}
	ctx := context.Background()
	eventID := "33333333-3333-3333-3333-333333333333"
	action := runtimecontracts.ActionSpec{
		ID: "mailbox_write",
		Mailbox: &runtimecontracts.MailboxWriteSpec{
			ItemType: runtimecontracts.LiteralExpression("review_request"),
			Summary:  runtimecontracts.RefExpression("payload.missing_summary"),
		},
	}
	evt := eventtest.RootIngress(eventID, "mailbox.review_requested", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{})
	base := values.NewContext()
	base.Event = values.Wrap(evt.ContextMap(""))
	base.Payload = values.Wrap(parsePayloadMap(evt.Payload()))

	ok, err := runner.ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "mailbox_write"}, runtimeengine.ExecutionContext{
		Base: base,
		Request: runtimeengine.ExecutionRequest{
			NodeID: identity.NormalizeNodeID("mailbox-node"),
			Event:  evt,
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
	store := NewWorkflowInstanceStore(db)
	artifactRoot := t.TempDir()
	pc := &PipelineCoordinator{db: db, workflowStore: store, artifactRoot: artifactRoot}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "working",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\nrank: 2\n")
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok {
		t.Fatal("expected artifact_repo_commit action to be claimed")
	}
	if err != nil {
		t.Fatalf("ExecuteAction: %v", err)
	}

	instance, ok, err := store.Load(ctx, entityID)
	if err != nil || !ok {
		t.Fatalf("load workflow instance ok=%v err=%v", ok, err)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["repo_url"])); got != "swarm-artifact://repos/"+initial["repo_id"].(string) {
		t.Fatalf("repo_url = %q", got)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["status"])); got != "committed" {
		t.Fatalf("artifact entity status = %q, want committed", got)
	}
	assertEntityStateField(t, db, entityID, "status", "committed")
	ref := strings.TrimSpace(asString(instance.Metadata["current_ref"]))
	if len(ref) != 40 {
		t.Fatalf("current_ref length = %d ref=%q", len(ref), ref)
	}
	manifest, ok := instance.Metadata["file_manifest"].(map[string]any)
	if !ok {
		t.Fatalf("file_manifest = %#v", instance.Metadata["file_manifest"])
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
	replayCtx.Request.State.StateCarrier.Metadata = cloneStringAnyMap(instance.Metadata)
	ok, err = pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, replayCtx)
	if !ok || err != nil {
		t.Fatalf("replay ExecuteAction ok=%v err=%v", ok, err)
	}
	replayed, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load replayed workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(replayed.Metadata["current_ref"])); got != ref {
		t.Fatalf("replay current_ref = %q, want %q", got, ref)
	}

	if err := os.MkdirAll(filepath.Join(repoPath, "notes"), 0o755); err != nil {
		t.Fatalf("create extra dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoPath, "notes", "extra.txt"), []byte("should not be committed\n"), 0o644); err != nil {
		t.Fatalf("write extra file: %v", err)
	}
	nextAction, nextCtx := testArtifactRepoActionAndContext(entityID, replayed.Metadata, "55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666", "name: Demo\nrank: 3\n")
	ok, err = pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(ctx, nextAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, nextCtx)
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
	store := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{db: db, workflowStore: store, artifactRoot: "/data/swarm/artifacts", bus: bus}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")

	var intents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(runtimeengine.WithActionEmitIntentCollector(ctx, &intents), action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	instance, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["status"])); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["failure_reason"])); !strings.Contains(got, "agent-visible mount /data") {
		t.Fatalf("failure_reason = %q, want invalid root detail", got)
	}
	if _, exists := instance.Metadata["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted on invalid artifact root: %#v", instance.Metadata["current_ref"])
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
			store := NewWorkflowInstanceStore(db)
			bus := &recordingPipelineBus{}
			artifactRoot, wantDetail := tc.root(t)
			pc := &PipelineCoordinator{db: db, workflowStore: store, artifactRoot: artifactRoot, bus: bus}
			ctx := testWorkflowStoreRunContext(t, store)
			entityID := "22222222-2222-2222-2222-222222222222"
			initial := testArtifactRepoEntityFields()
			if err := store.Upsert(ctx, WorkflowInstance{
				InstanceID:      entityID,
				StorageRef:      entityID,
				WorkflowName:    "artifact-repo",
				WorkflowVersion: "1.0.0",
				CurrentState:    "ready",
				Metadata:        cloneStringAnyMap(initial),
			}); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")

			var intents []runtimeengine.EmitIntent
			ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(runtimeengine.WithActionEmitIntentCollector(ctx, &intents), action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
			if !ok || err != nil {
				t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
			}
			instance, _, err := store.Load(ctx, entityID)
			if err != nil {
				t.Fatalf("load workflow instance: %v", err)
			}
			if got := strings.TrimSpace(asString(instance.Metadata["status"])); got != "failed" {
				t.Fatalf("status = %q, want failed", got)
			}
			if got := strings.TrimSpace(asString(instance.Metadata["failure_reason"])); !strings.Contains(got, wantDetail) || !strings.Contains(got, "not writable by the runtime process") {
				t.Fatalf("failure_reason = %q, want unusable root detail %q", got, wantDetail)
			}
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
	store := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		db:            db,
		workflowStore: store,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        &previewWorkflowModule{bundle: bundle},
		entityLocks:   map[string]*sync.Mutex{},
	}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.SuccessEvent = "artifact_repo.commit_completed"
	action.ArtifactRepo.SuccessPayload = map[string]runtimecontracts.ExpressionValue{
		"result_kind": runtimecontracts.LiteralExpression("ready"),
	}

	var intents []runtimeengine.EmitIntent
	actionCtx := runtimeengine.WithActionEmitIntentCollector(ctx, &intents)
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(actionCtx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
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
	committed, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load committed workflow instance: %v", err)
	}

	replayCtx := execCtx
	replayCtx.Request.State.StateCarrier.Metadata = cloneStringAnyMap(committed.Metadata)
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).ExecuteAction(actionCtx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, replayCtx); err != nil {
		t.Fatalf("same-source replay ExecuteAction: %v", err)
	}
	if got := len(intents); got != 1 {
		t.Fatalf("same-source replay queued success event count = %d, want 1", got)
	}

	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("reset workflow instance to simulate DB/git split: %v", err)
	}
	repairAction, repairCtx := testArtifactRepoActionAndContext(entityID, initial, "55555555-5555-5555-5555-555555555555", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	repairAction.ArtifactRepo.SuccessEvent = action.ArtifactRepo.SuccessEvent
	repairAction.ArtifactRepo.SuccessPayload = action.ArtifactRepo.SuccessPayload
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).ExecuteAction(actionCtx, repairAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, repairCtx); err != nil {
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
	workflowStore := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		db:            db,
		workflowStore: workflowStore,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        &previewWorkflowModule{bundle: bundle},
		entityLocks:   map[string]*sync.Mutex{},
	}
	ctx := testWorkflowStoreRunContext(t, workflowStore)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.SuccessEvent = "artifact_repo.commit_completed"
	action.ArtifactRepo.SuccessPayload = map[string]runtimecontracts.ExpressionValue{
		"result_kind": runtimecontracts.LiteralExpression("ready"),
	}
	sourceEvent := testProjectionEventWithSourceAgent(execCtx.Request.Event, "test")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload,
			produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4::uuid, '', 'entity', $5::jsonb, $6, 'agent', $7)
	`, testPipelineRunID, sourceEvent.ID(), string(sourceEvent.Type()), entityID, string(sourceEvent.Payload()), sourceEvent.SourceAgent(), sourceEvent.CreatedAt()); err != nil {
		t.Fatalf("seed source event: %v", err)
	}

	result, err := pc.executeNodeContractHandler(ctx, "artifact-node", runtimecontracts.SystemNodeEventHandler{
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
	if got := string(bus.outboxIntent(0).Event.Type()); got != "artifact_repo.commit_completed" {
		t.Fatalf("outbox result event type = %q", got)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("post-commit published result event count = %d, want 1", got)
	}
	committed, _, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load committed workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(committed.Metadata["last_source_event_id"])); got != execCtx.Request.Event.ID() {
		t.Fatalf("last_source_event_id = %q, want %q", got, execCtx.Request.Event.ID())
	}
}

func TestExecuteNodeContractHandlerArtifactRepoCommitQueuesFailureResultThroughOutbox(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	workflowStore := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		db:            db,
		workflowStore: workflowStore,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        &previewWorkflowModule{bundle: bundle},
		entityLocks:   map[string]*sync.Mutex{},
	}
	ctx := testWorkflowStoreRunContext(t, workflowStore)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.Files[0].Path = runtimecontracts.LiteralExpression("../escape.yaml")
	sourceEvent := testProjectionEventWithSourceAgent(execCtx.Request.Event, "test")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload,
			produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4::uuid, '', 'entity', $5::jsonb, $6, 'agent', $7)
	`, testPipelineRunID, sourceEvent.ID(), string(sourceEvent.Type()), entityID, string(sourceEvent.Payload()), sourceEvent.SourceAgent(), sourceEvent.CreatedAt()); err != nil {
		t.Fatalf("seed source event: %v", err)
	}

	result, err := pc.executeNodeContractHandler(ctx, "artifact-node", runtimecontracts.SystemNodeEventHandler{
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
	if got := string(bus.outboxIntent(0).Event.Type()); got != "artifact_repo.commit_failed" {
		t.Fatalf("outbox result event type = %q", got)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("post-commit published result event count = %d, want 1", got)
	}
	committed, _, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load committed workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(committed.Metadata["status"])); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	if got := strings.TrimSpace(asString(committed.Metadata["failure_reason"])); !strings.Contains(got, "path traversal is not allowed") {
		t.Fatalf("failure_reason = %q, want path traversal detail", got)
	}
	if got := strings.TrimSpace(asString(committed.Metadata["last_source_event_id"])); got != execCtx.Request.Event.ID() {
		t.Fatalf("last_source_event_id = %q, want %q", got, execCtx.Request.Event.ID())
	}
	if _, exists := committed.Metadata["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted on failed commit: %#v", committed.Metadata["current_ref"])
	}
}

func TestExecuteNodeContractHandlerArtifactRepoCommitFailureResultOutboxFailureRollsBackState(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	workflowStore := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{outboxErr: errors.New("outbox unavailable")}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		db:            db,
		workflowStore: workflowStore,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        &previewWorkflowModule{bundle: bundle},
		entityLocks:   map[string]*sync.Mutex{},
	}
	ctx := testWorkflowStoreRunContext(t, workflowStore)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.Files[0].Path = runtimecontracts.LiteralExpression("../escape.yaml")
	sourceEvent := testProjectionEventWithSourceAgent(execCtx.Request.Event, "test")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload,
			produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4::uuid, '', 'entity', $5::jsonb, $6, 'agent', $7)
	`, testPipelineRunID, sourceEvent.ID(), string(sourceEvent.Type()), entityID, string(sourceEvent.Payload()), sourceEvent.SourceAgent(), sourceEvent.CreatedAt()); err != nil {
		t.Fatalf("seed source event: %v", err)
	}

	_, err := pc.executeNodeContractHandler(ctx, "artifact-node", runtimecontracts.SystemNodeEventHandler{
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
	rolledBack, _, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if _, exists := rolledBack.Metadata["status"]; exists {
		t.Fatalf("status should roll back with failed outbox write: %#v", rolledBack.Metadata["status"])
	}
	if _, exists := rolledBack.Metadata["failure_reason"]; exists {
		t.Fatalf("failure_reason should roll back with failed outbox write: %#v", rolledBack.Metadata["failure_reason"])
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitFailsClosedWithoutResultEventCollector(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{publishErr: errors.New("direct publish must not be used")}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		db:            db,
		workflowStore: store,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        &previewWorkflowModule{bundle: bundle},
	}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.SuccessEvent = "artifact_repo.commit_completed"
	action.ArtifactRepo.SuccessPayload = map[string]runtimecontracts.ExpressionValue{
		"result_kind": runtimecontracts.LiteralExpression("ready"),
	}

	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok {
		t.Fatal("expected artifact_repo_commit action to be claimed")
	}
	if err == nil || !strings.Contains(err.Error(), errArtifactRepoResultEmitCollectorMissing.Error()) {
		t.Fatalf("ExecuteAction error = %v, want missing result collector", err)
	}
	instance, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["status"])); got != "" {
		t.Fatalf("status = %q, want unchanged without result collector", got)
	}
	if _, exists := instance.Metadata["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted without result collector: %#v", instance.Metadata["current_ref"])
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("fallback published event count = %d, want 0", got)
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitFailsClosedOnInvalidSuccessResultEvent(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{}
	source := testArtifactRepoResultEventSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	pc := &PipelineCoordinator{
		db:            db,
		workflowStore: store,
		artifactRoot:  t.TempDir(),
		bus:           bus,
		module:        &previewWorkflowModule{bundle: bundle},
	}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.SuccessEvent = "artifact_repo.commit_completed"

	var intents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(runtimeengine.WithActionEmitIntentCollector(ctx, &intents), action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	instance, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if _, exists := instance.Metadata["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted on invalid success event: %#v", instance.Metadata["current_ref"])
	}
	if got := strings.TrimSpace(asString(instance.Metadata["status"])); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["failure_reason"])); !strings.Contains(got, "payload violates schema") {
		t.Fatalf("failure_reason = %q, want schema violation detail", got)
	}
	assertArtifactRepoQueuedIntent(t, intents, 0, "artifact_repo.commit_failed")
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("fallback published event count = %d, want 0", got)
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitFailsClosedOnPathOutsideAllowlist(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{db: db, workflowStore: store, artifactRoot: t.TempDir(), bus: bus}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.Files[0].Path = runtimecontracts.LiteralExpression("../escape.yaml")

	var intents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(runtimeengine.WithActionEmitIntentCollector(ctx, &intents), action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	instance, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if _, exists := instance.Metadata["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted on failed commit: %#v", instance.Metadata["current_ref"])
	}
	if got := strings.TrimSpace(asString(instance.Metadata["status"])); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["failure_reason"])); !strings.Contains(got, "path traversal is not allowed") {
		t.Fatalf("failure_reason = %q, want traversal detail", got)
	}
	failureEvent := assertArtifactRepoQueuedIntent(t, intents, 0, "artifact_repo.commit_failed")
	var payload map[string]any
	if err := json.Unmarshal(failureEvent.Payload(), &payload); err != nil {
		t.Fatalf("failure event payload: %v", err)
	}
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
	store := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{db: db, workflowStore: store, artifactRoot: t.TempDir(), bus: bus}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "rank: 2\n")

	var intents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(runtimeengine.WithActionEmitIntentCollector(ctx, &intents), action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	instance, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["status"])); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	if _, exists := instance.Metadata["current_ref"]; exists {
		t.Fatalf("current_ref should not be persisted on failed commit: %#v", instance.Metadata["current_ref"])
	}
	if got := strings.TrimSpace(asString(instance.Metadata["failure_reason"])); !strings.Contains(got, "missing required field name") {
		t.Fatalf("failure_reason = %q, want schema mismatch detail", got)
	}
	assertArtifactRepoQueuedIntent(t, intents, 0, "artifact_repo.commit_failed")
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitRejectsRequestIDContentConflict(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := NewWorkflowInstanceStore(db)
	pc := &PipelineCoordinator{db: db, workflowStore: store, artifactRoot: t.TempDir()}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx); err != nil {
		t.Fatalf("initial ExecuteAction: %v", err)
	}
	instance, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}

	nextAction, nextCtx := testArtifactRepoActionAndContext(entityID, instance.Metadata, "55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666", "name: Next\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).ExecuteAction(ctx, nextAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, nextCtx); err != nil {
		t.Fatalf("next ExecuteAction: %v", err)
	}
	afterNext, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance after next request: %v", err)
	}
	// Display labels are cosmetic; request history must remain keyed by repo identity.
	afterNext.Metadata["display_slug"] = "Renamed Artifact"

	conflictAction, conflictCtx := testArtifactRepoActionAndContext(entityID, afterNext.Metadata, "77777777-7777-7777-7777-777777777777", "44444444-4444-4444-4444-444444444444", "name: Changed\n")
	var conflictIntents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(runtimeengine.WithActionEmitIntentCollector(ctx, &conflictIntents), conflictAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, conflictCtx)
	if !ok || err != nil {
		t.Fatalf("ExecuteAction ok=%v err=%v, want handled failure result", ok, err)
	}
	assertArtifactRepoQueuedIntent(t, conflictIntents, 0, "artifact_repo.commit_failed")
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitRecordsNoDiffRequestHistory(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := NewWorkflowInstanceStore(db)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{db: db, workflowStore: store, artifactRoot: t.TempDir(), bus: bus}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx); err != nil {
		t.Fatalf("initial ExecuteAction: %v", err)
	}
	instance, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	initialRef := strings.TrimSpace(asString(instance.Metadata["current_ref"]))

	sameAction, sameCtx := testArtifactRepoActionAndContext(entityID, instance.Metadata, "55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666", "name: Demo\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).ExecuteAction(ctx, sameAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, sameCtx); err != nil {
		t.Fatalf("same-tree ExecuteAction: %v", err)
	}
	afterSame, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance after same-tree request: %v", err)
	}
	sameRef := strings.TrimSpace(asString(afterSame.Metadata["current_ref"]))
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

	nextAction, nextCtx := testArtifactRepoActionAndContext(entityID, afterSame.Metadata, "77777777-7777-7777-7777-777777777777", "88888888-8888-8888-8888-888888888888", "name: Next\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).ExecuteAction(ctx, nextAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, nextCtx); err != nil {
		t.Fatalf("next ExecuteAction: %v", err)
	}
	afterNext, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance after next request: %v", err)
	}

	conflictAction, conflictCtx := testArtifactRepoActionAndContext(entityID, afterNext.Metadata, "99999999-9999-9999-9999-999999999999", "66666666-6666-6666-6666-666666666666", "name: Changed\n")
	var conflictIntents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(runtimeengine.WithActionEmitIntentCollector(ctx, &conflictIntents), conflictAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, conflictCtx)
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
	store := NewWorkflowInstanceStore(db)
	pc := &PipelineCoordinator{db: db, workflowStore: store, artifactRoot: t.TempDir()}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx); err != nil {
		t.Fatalf("initial ExecuteAction: %v", err)
	}
	committed, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load committed workflow instance: %v", err)
	}
	ref := strings.TrimSpace(asString(committed.Metadata["current_ref"]))
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("reset workflow instance to simulate DB/git split: %v", err)
	}

	repairAction, repairCtx := testArtifactRepoActionAndContext(entityID, initial, "55555555-5555-5555-5555-555555555555", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(ctx, repairAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, repairCtx)
	if !ok || err != nil {
		t.Fatalf("repair ExecuteAction ok=%v err=%v", ok, err)
	}
	repaired, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load repaired workflow instance: %v", err)
	}
	if got := strings.TrimSpace(asString(repaired.Metadata["current_ref"])); got != ref {
		t.Fatalf("repaired current_ref = %q, want %q", got, ref)
	}
	if got := strings.TrimSpace(asString(repaired.Metadata["status"])); got != "committed" {
		t.Fatalf("repaired status = %q, want committed", got)
	}
}

func TestPipelineEngineActionRunner_ArtifactRepoCommitEnforcesProjectedRepoSize(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	store := NewWorkflowInstanceStore(db)
	pc := &PipelineCoordinator{db: db, workflowStore: store, artifactRoot: t.TempDir()}
	ctx := testWorkflowStoreRunContext(t, store)
	entityID := "22222222-2222-2222-2222-222222222222"
	initial := testArtifactRepoEntityFields()
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "artifact-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.Limits.MaxRepoBytes = 1024
	if _, err := (pipelineEngineActionRunner{coordinator: pc}).ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx); err != nil {
		t.Fatalf("initial ExecuteAction: %v", err)
	}
	instance, _, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}

	nextAction, nextCtx := testArtifactRepoActionAndContext(entityID, instance.Metadata, "55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666", "name: unused\n")
	nextAction.ArtifactRepo.AllowedPaths = append(nextAction.ArtifactRepo.AllowedPaths, "artifacts/extra.txt")
	nextAction.ArtifactRepo.Files = []runtimecontracts.ArtifactRepoFileSpec{{
		Path:        runtimecontracts.LiteralExpression("artifacts/extra.txt"),
		Content:     runtimecontracts.LiteralExpression("xxxxxxxxxxxxxxxxxxxxxxxxx"),
		ContentType: "text",
	}}
	nextAction.ArtifactRepo.Limits.MaxRepoBytes = 30
	var intents []runtimeengine.EmitIntent
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(runtimeengine.WithActionEmitIntentCollector(ctx, &intents), nextAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, nextCtx)
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
	return eventtest.RootIngress(
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
		"package.yaml": "name: artifact-result-events\nversion: 1.0.0\ndescription: artifact result event fixture\nplatform_version: \">=1.1.0\"\nflows: []\n",
		"schema.yaml":  "initial_state: ready\nterminal_states: [ready]\nstates: [ready]\n",
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
  partition_key: string
  display_slug: string
  request_id: string
  source_event_id: string
  repo_url: string
  current_ref: string
  file_manifest: ArtifactManifest
  provenance: ArtifactProvenance
  result_kind: string
  required: [repo_id, namespace, request_id, source_event_id, repo_url, current_ref, file_manifest, provenance, result_kind]
artifact_repo.commit_failed:
  repo_id: string
  namespace: string
  partition_key: string
  display_slug: string
  request_id: string
  source_event_id: string
  failure_reason: string
  provenance: ArtifactProvenance
  request_copy: string
  required: [repo_id, namespace, request_id, source_event_id, failure_reason, provenance]
`,
	})
}

func testArtifactRepoEntityFields() map[string]any {
	return map[string]any{
		"repo_id":          "11111111-1111-1111-1111-111111111111",
		"namespace":        "tenant-alpha",
		"partition_key":    "project-42",
		"display_slug":     "Demo Artifact",
		"source_record_id": "record-123",
	}
}

func testArtifactRepoActionAndContext(entityID string, entity map[string]any, eventID, requestID, content string) (runtimecontracts.ActionSpec, runtimeengine.ExecutionContext) {
	payload := map[string]any{
		"request_id": requestID,
		"mvp_yaml":   content,
	}
	payloadBytes, _ := json.Marshal(payload)
	evt := eventtest.RootIngress(
		eventID,
		"artifact_repo.commit_requested",
		"",
		"",
		payloadBytes,
		0,
		testPipelineRunID,
		"",
		events.EventEnvelope{EntityID: entityID},
		time.Unix(1_700_000_000, 0).UTC(),
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
					FailureReason:     "failure_reason",
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
				FlowID:   identity.NormalizeFlowID("artifact-repo"),
				NodeID:   identity.NormalizeNodeID("artifact-node"),
				Event:    evt,
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
	got := workflowStateGatesForScope(source, "child", map[string]any{
		"gates": map[string]any{
			"child/g_validated": true,
		},
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
		FlowID:   identity.NormalizeFlowID("child"),
		Event: eventtest.RootIngress(
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

	internal, err := shaper.ShapeEmitPayload(context.Background(), req, "child/child.internal", map[string]any{"step": "done"})
	if err != nil {
		t.Fatalf("ShapeEmitPayload internal: %v", err)
	}
	if _, ok := internal["entity_id"]; ok {
		t.Fatalf("internal emit payload must not carry envelope entity_id: %#v", internal["entity_id"])
	}
	if got := internal["step"]; got != "done" {
		t.Fatalf("internal emit step = %#v, want done", got)
	}

	if _, err := shaper.ShapeEmitPayload(context.Background(), req, "child/child.done", map[string]any{"step": "done"}); err == nil {
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
		FlowID:   identity.NormalizeFlowID("child"),
		Event: eventtest.RootIngress(
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

	_, err := shaper.ShapeEmitPayload(context.Background(), req, "child/child.done", map[string]any{
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
		FlowID:   identity.NormalizeFlowID("child"),
		Event: eventtest.RootIngress(
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

	actionCtx := runtimeengine.WithEmitSurface(context.Background(), runtimeengine.EmitSurfaceAction)
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
		"package.yaml":             "name: action-emit-required\nversion: 1.0.0\ndescription: Action emit required-field proof.\nplatform_version: \">=1.1.0\"\nflows:\n- id: child\n  flow: child\n  mode: static\n",
		"schema.yaml":              "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n  outputs:\n    events: [parent.result]\n",
		"events.yaml":              "parent.trigger:\n  entity_id: string\nparent.result:\n  entity_id: string\n",
		"flows/child/package.yaml": "name: child\nversion: 1.0.0\ndescription: child flow\nplatform_version: \">=1.1.0\"\nflows: []\n",
		"flows/child/schema.yaml":  "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.internal]\n",
		"flows/child/events.yaml":  "child.start:\n  entity_id: string\nchild.internal:\n  entity_id: string\n  step: string\n  required: [entity_id, step]\n",
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
		FlowID:   identity.NormalizeFlowID("child"),
		Event: eventtest.RootIngress(
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

	actionCtx := runtimeengine.WithEmitSurface(context.Background(), runtimeengine.EmitSurfaceAction)
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
		"package.yaml":             "name: template-output-required\nversion: 1.0.0\ndescription: Template output required-field proof.\nplatform_version: \">=1.1.0\"\nflows:\n- id: child\n  flow: child\n  mode: template\n",
		"schema.yaml":              "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n",
		"events.yaml":              "parent.trigger:\n  entity_id: string\n",
		"flows/child/package.yaml": "name: child\nversion: 1.0.0\ndescription: child flow\nplatform_version: \">=1.1.0\"\nflows: []\n",
		"flows/child/schema.yaml":  "name: child\nmode: template\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.done]\n",
		"flows/child/events.yaml":  "child.start:\n  entity_id: string\nchild.done:\n  step: string\n  required: [step]\n",
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
		FlowID:   identity.NormalizeFlowID("child"),
		Event: eventtest.RootIngress(
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

	_, err := shaper.ShapeEmitPayload(context.Background(), req, "child/inst-1/child.done", map[string]any{})
	if err == nil {
		t.Fatal("expected concrete template output missing required field to fail closed")
	}
	if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
		t.Fatalf("ShapeEmitPayload concrete template output error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
	}

	if _, err := shaper.ShapeEmitPayload(context.Background(), req, "child/inst-1/child.done", map[string]any{"step": "done"}); err != nil {
		t.Fatalf("ShapeEmitPayload concrete template output with required field: %v", err)
	}
}

func TestPipelineEnginePayloadShaper_RejectsEnvelopeOnlyRequiredFieldOnActionSurface(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml":             "name: action-emit-envelope-required\nversion: 1.0.0\ndescription: Action emit envelope-required proof.\nplatform_version: \">=1.1.0\"\nflows:\n- id: child\n  flow: child\n  mode: static\n",
		"schema.yaml":              "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n  outputs:\n    events: [parent.result]\n",
		"events.yaml":              "parent.trigger:\n  entity_id: string\nparent.result:\n  entity_id: string\n",
		"flows/child/package.yaml": "name: child\nversion: 1.0.0\ndescription: child flow\nplatform_version: \">=1.1.0\"\nflows: []\n",
		"flows/child/schema.yaml":  "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.internal]\n",
		"flows/child/events.yaml":  "child.start:\n  entity_id: string\nchild.internal:\n  entity_id: string\n  required: [entity_id]\n",
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
		FlowID:   identity.NormalizeFlowID("child"),
		Event: eventtest.RootIngress(
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

	actionCtx := runtimeengine.WithEmitSurface(context.Background(), runtimeengine.EmitSurfaceAction)
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
		"package.yaml":             "name: action-emit-enum\nversion: 1.0.0\ndescription: Action emit enum proof.\nplatform_version: \">=1.1.0\"\nflows:\n- id: child\n  flow: child\n  mode: static\n",
		"schema.yaml":              "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: [parent.trigger]\n  outputs:\n    events: [parent.result]\n",
		"events.yaml":              "parent.trigger:\n  entity_id: string\nparent.result:\n  entity_id: string\n",
		"types.yaml":               "enums:\n  Mode: [fast, deep]\n",
		"flows/child/package.yaml": "name: child\nversion: 1.0.0\ndescription: child flow\nplatform_version: \">=1.1.0\"\nflows: []\n",
		"flows/child/schema.yaml":  "name: child\ninitial_state: waiting\nterminal_states: [processed]\nstates: [waiting, processed]\npins:\n  inputs:\n    events: [child.start]\n  outputs:\n    events: [child.internal]\n",
		"flows/child/events.yaml":  "child.start:\n  entity_id: string\nchild.internal:\n  mode: Mode\n  required: [mode]\n",
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
		"package.yaml":             "name: child-output-named-type\nversion: 1.0.0\ndescription: child output named type proof\nplatform_version: \">=1.1.0\"\nflows:\n- id: child\n  flow: child\n  mode: static\n",
		"schema.yaml":              "initial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  outputs:\n    events: [handoff.completed]\n",
		"types.yaml":               "types:\n  Evidence:\n    root_field: text\n",
		"events.yaml":              "handoff.completed:\n  evidence: Evidence\n  required: [evidence]\n",
		"flows/child/package.yaml": "name: child\nversion: 1.0.0\ndescription: child flow\nplatform_version: \">=1.1.0\"\nflows: []\n",
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
		FlowID:   identity.NormalizeFlowID("child"),
		Event: eventtest.RootIngress(
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
			payload, err := shaper.ShapeEmitPayload(context.Background(), req, eventType, map[string]any{
				"evidence": map[string]any{"root_field": "ok"},
			})
			if err != nil {
				t.Fatalf("ShapeEmitPayload valid root named type: %v", err)
			}
			evidence, _ := payload["evidence"].(map[string]any)
			if _, ok := evidence["root_field"]; !ok {
				t.Fatalf("payload = %#v, want root_field evidence", payload)
			}

			_, err = shaper.ShapeEmitPayload(context.Background(), req, eventType, map[string]any{
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
		FlowID:   identity.NormalizeFlowID("child"),
		Event: eventtest.RootIngress(
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

	actionCtx := runtimeengine.WithEmitSurface(context.Background(), runtimeengine.EmitSurfaceAction)
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

type recordingScheduleStore struct {
	upserts []Schedule
	cancels []Schedule
}

func (s *recordingScheduleStore) UpsertSchedule(_ context.Context, sc Schedule) error {
	s.upserts = append(s.upserts, sc)
	return nil
}
func (s *recordingScheduleStore) CancelSchedule(context.Context, string, string) error { return nil }
func (s *recordingScheduleStore) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}
func (*recordingScheduleStore) ClaimSchedule(context.Context, Schedule) (bool, error) {
	return true, nil
}
func (*recordingScheduleStore) ReleaseSchedule(context.Context, Schedule) error     { return nil }
func (*recordingScheduleStore) ReleaseScheduleClaims(context.Context) error         { return nil }
func (s *recordingScheduleStore) MarkScheduleFired(context.Context, Schedule) error { return nil }
func (s *recordingScheduleStore) CancelScheduleExact(_ context.Context, sc Schedule) error {
	s.cancels = append(s.cancels, sc)
	return nil
}
func (s *recordingScheduleStore) CancelScheduleExactTerminal(_ context.Context, sc Schedule) error {
	s.cancels = append(s.cancels, sc)
	return nil
}
func (s *recordingScheduleStore) MarkScheduleFiredExact(context.Context, Schedule) error { return nil }
func (s *recordingScheduleStore) CompleteScheduleFireExact(context.Context, Schedule) error {
	return nil
}

func TestPipelineEngineTimerApplierPersistsTimersAndDefersSchedulerToPostCommit(t *testing.T) {
	store := &recordingScheduleStore{}
	scheduler := NewScheduler()
	defer scheduler.Stop()
	pc := &PipelineCoordinator{
		timerScheduler:     scheduler,
		timerScheduleStore: store,
	}
	actions := make([]func(), 0, 2)
	ctx := withPipelinePostCommitActions(context.Background(), &actions)
	sc := Schedule{
		AgentID:   "owner",
		EventType: "timer.review",
		Mode:      "once",
		At:        time.Now().Add(time.Hour),
		EntityID:  "ent-1",
		TaskID:    "timer-1",
	}

	pc.persistWorkflowTimerSchedule(ctx, sc)
	if got := len(store.upserts); got != 1 {
		t.Fatalf("persisted schedules = %d, want 1", got)
	}
	if got := len(actions); got != 1 {
		t.Fatalf("post-commit actions = %d, want 1", got)
	}
	if got := len(scheduler.tasks); got != 0 {
		t.Fatalf("scheduler tasks before flush = %d, want 0", got)
	}
	flushPipelinePostCommitActions(actions)
	if got := len(scheduler.tasks); got != 1 {
		t.Fatalf("scheduler tasks after flush = %d, want 1", got)
	}

	cancelActions := make([]func(), 0, 1)
	cancelCtx := withPipelinePostCommitActions(context.Background(), &cancelActions)
	pc.persistWorkflowTimerCancellation(cancelCtx, sc)
	if got := len(store.cancels); got != 1 {
		t.Fatalf("persisted cancels = %d, want 1", got)
	}
	if got := len(cancelActions); got != 1 {
		t.Fatalf("cancel post-commit actions = %d, want 1", got)
	}
	flushPipelinePostCommitActions(cancelActions)
	if got := len(scheduler.tasks); got != 0 {
		t.Fatalf("scheduler tasks after cancel flush = %d, want 0", got)
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
