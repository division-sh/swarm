package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/identity"
	runtimeregistry "swarm/internal/runtime/core/registry"
	"swarm/internal/runtime/core/values"
	runtimeengine "swarm/internal/runtime/engine"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/testutil"
)

func TestApplyEngineStateMutationMirrorsDataAccumulationIntoEntityProjection(t *testing.T) {
	instance := &WorkflowInstance{
		Metadata:     map[string]any{"research_context": map[string]any{"summary": "done"}},
		StateBuckets: map[string]any{},
	}
	mutation := runtimeengine.StateMutation{
		Metadata: map[string]any{
			"research_context":              map[string]any{"summary": "done"},
			"last_data_accumulation_event":  "research.completed",
			"last_data_accumulation_source": "research.completed",
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "research_context", SourceField: "research_context"},
			},
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
	mutation := runtimeengine.StateMutation{
		SetGate: "g_c",
		Gates: map[string]bool{
			"g_c": true,
		},
	}

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
	mutation := runtimeengine.StateMutation{
		SetGate: "g_validated",
		Gates: map[string]bool{
			"g_validated": true,
		},
	}

	applyEngineStateMutation(instance, mutation, nil, source, "child")

	gates := workflowStateGatesAsBools(instance.Metadata)
	if !gates["child/g_validated"] {
		t.Fatalf("scoped gates = %#v, want child/g_validated=true", gates)
	}
	if gates["g_validated"] {
		t.Fatalf("raw unscoped child gate leaked into metadata: %#v", gates)
	}
}

func TestProjectWorkflowSubjectGatesProjectsScopedChildGatesToSubjectEntity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	subjectID := "11111111-1111-1111-1111-111111111111"
	childStorageRef := "child"
	childID := FlowInstanceEntityID(childStorageRef)
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      subjectID,
		SubjectID:       subjectID,
		StorageRef:      subjectID,
		WorkflowName:    "root",
		WorkflowVersion: "1.6.0",
		CurrentState:    "idle",
		Metadata: map[string]any{
			"subject_id": subjectID,
			"gates": map[string]any{
				"root_ready": true,
			},
		},
	}); err != nil {
		t.Fatalf("upsert subject: %v", err)
	}
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      childID,
		SubjectID:       subjectID,
		StorageRef:      childStorageRef,
		WorkflowName:    "child",
		WorkflowVersion: "1.6.0",
		CurrentState:    "done",
		Metadata: map[string]any{
			"subject_id": subjectID,
			"flow_path":  "child",
			"gates": map[string]any{
				"child/g_validated": true,
			},
		},
	}); err != nil {
		t.Fatalf("upsert child: %v", err)
	}

	pc := &PipelineCoordinator{workflowStore: store}
	child, ok, err := store.Load(context.Background(), childStorageRef)
	if err != nil {
		t.Fatalf("load child: %v", err)
	}
	if !ok {
		t.Fatal("child entity missing before projection")
	}
	if got := strings.TrimSpace(asString(child.Metadata["flow_path"])); got != "child" {
		t.Fatalf("child flow_path = %#v, want child (metadata=%#v)", child.Metadata["flow_path"], child.Metadata)
	}
	if gates := workflowStateGatesAsBools(child.Metadata); !gates["child/g_validated"] {
		t.Fatalf("child gates before projection = %#v", gates)
	}
	if err := pc.projectWorkflowSubjectGates(context.Background(), childID); err != nil {
		t.Fatalf("projectWorkflowSubjectGates: %v", err)
	}

	subject, ok, err := store.Load(context.Background(), subjectID)
	if err != nil {
		t.Fatalf("load subject: %v", err)
	}
	if !ok {
		t.Fatal("subject entity missing after projection")
	}
	gates := workflowStateGatesAsBools(subject.Metadata)
	if !gates["root_ready"] {
		t.Fatalf("subject gates lost existing root gate: %#v", gates)
	}
	if !gates["child/g_validated"] {
		t.Fatalf("subject gates missing projected child gate: %#v", gates)
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
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "root",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Metadata: map[string]any{
			"subject_id": entityID,
		},
	}); err != nil {
		t.Fatalf("seed root instance: %v", err)
	}

	if err := pc.maybeDeactivateTerminalFlowInstance(context.Background(), entityID, "done"); err != nil {
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
	const subjectID = "11111111-1111-1111-1111-111111111111"
	const parentEntityID = "22222222-2222-2222-2222-222222222222"
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      "inst-1",
		SubjectID:       subjectID,
		StorageRef:      flowPath,
		WorkflowName:    "review",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Metadata: map[string]any{
			"entity_id":        entityID,
			"instance_id":      "inst-1",
			"flow_path":        flowPath,
			"subject_id":       subjectID,
			"parent_entity_id": parentEntityID,
		},
	}); err != nil {
		t.Fatalf("seed template instance: %v", err)
	}

	if err := pc.maybeDeactivateTerminalFlowInstance(context.Background(), entityID, "completed"); err != nil {
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

func TestProjectWorkflowSubjectGatesRequiresCanonicalEntityID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	subjectID := "11111111-1111-1111-1111-111111111111"
	logicalChildID := "22222222-2222-2222-2222-222222222222"
	childStorageRef := "child"
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      subjectID,
		SubjectID:       subjectID,
		StorageRef:      subjectID,
		WorkflowName:    "root",
		WorkflowVersion: "1.6.0",
		CurrentState:    "idle",
		Metadata: map[string]any{
			"subject_id": subjectID,
		},
	}); err != nil {
		t.Fatalf("upsert subject: %v", err)
	}
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      logicalChildID,
		SubjectID:       subjectID,
		StorageRef:      childStorageRef,
		WorkflowName:    "child",
		WorkflowVersion: "1.6.0",
		CurrentState:    "done",
		Metadata: map[string]any{
			"subject_id": subjectID,
			"flow_path":  "child",
			"gates": map[string]any{
				"child/g_validated": true,
			},
		},
	}); err != nil {
		t.Fatalf("upsert child: %v", err)
	}

	pc := &PipelineCoordinator{
		workflowStore: store,
		module: &previewWorkflowModule{
			bundle: &runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{
					FlowPrefix: map[string]string{
						"child": "child",
					},
				},
			},
		},
	}
	if err := pc.projectWorkflowSubjectGates(context.Background(), logicalChildID); err != nil {
		t.Fatalf("projectWorkflowSubjectGates: %v", err)
	}

	subject, ok, err := store.Load(context.Background(), subjectID)
	if err != nil {
		t.Fatalf("load subject: %v", err)
	}
	if !ok {
		t.Fatal("subject entity missing after projection")
	}
	gates := workflowStateGatesAsBools(subject.Metadata)
	if gates["child/g_validated"] {
		t.Fatalf("subject gates unexpectedly projected from logical instance id lookup: %#v", gates)
	}
}

func TestProjectWorkflowSubjectGatesFallsBackToFlowScopeWhenChildMetadataHasNoFlowPath(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	subjectID := "11111111-1111-1111-1111-111111111111"
	childEntityID := "22222222-2222-2222-2222-222222222222"
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      subjectID,
		SubjectID:       subjectID,
		StorageRef:      subjectID,
		WorkflowName:    "root",
		WorkflowVersion: "1.6.0",
		CurrentState:    "idle",
		Metadata: map[string]any{
			"subject_id": subjectID,
		},
	}); err != nil {
		t.Fatalf("upsert subject: %v", err)
	}
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      childEntityID,
		SubjectID:       subjectID,
		StorageRef:      childEntityID,
		WorkflowName:    "child",
		WorkflowVersion: "1.6.0",
		CurrentState:    "done",
		Metadata: map[string]any{
			"subject_id": subjectID,
			"gates": map[string]any{
				"child/g_validated": true,
			},
		},
	}); err != nil {
		t.Fatalf("upsert child: %v", err)
	}

	pc := &PipelineCoordinator{
		workflowStore: store,
		module: &previewWorkflowModule{
			bundle: &runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{
					FlowPrefix: map[string]string{
						"child": "child",
					},
				},
			},
		},
	}
	ctx := withPipelineFlowScope(context.Background(), "child")
	if err := pc.projectWorkflowSubjectGates(ctx, childEntityID); err != nil {
		t.Fatalf("projectWorkflowSubjectGates: %v", err)
	}

	subject, ok, err := store.Load(context.Background(), subjectID)
	if err != nil {
		t.Fatalf("load subject: %v", err)
	}
	if !ok {
		t.Fatal("subject entity missing after projection")
	}
	gates := workflowStateGatesAsBools(subject.Metadata)
	if !gates["child/g_validated"] {
		t.Fatalf("subject gates missing projected child gate via flow-scope fallback: %#v", gates)
	}
}

func TestProjectWorkflowSubjectGatesUsesFlowScopeKeyForInstancedChildPaths(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	subjectID := "11111111-1111-1111-1111-111111111111"
	childEntityID := FlowInstanceEntityID("child/inst-1")
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      subjectID,
		SubjectID:       subjectID,
		StorageRef:      subjectID,
		WorkflowName:    "root",
		WorkflowVersion: "1.6.0",
		CurrentState:    "idle",
		Metadata: map[string]any{
			"subject_id": subjectID,
		},
	}); err != nil {
		t.Fatalf("upsert subject: %v", err)
	}
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      childEntityID,
		SubjectID:       subjectID,
		StorageRef:      "child/inst-1",
		WorkflowName:    "child",
		WorkflowVersion: "1.6.0",
		CurrentState:    "done",
		Metadata: map[string]any{
			"subject_id": subjectID,
			"flow_path":  "child/inst-1",
			"gates": map[string]any{
				"child/g_validated": true,
			},
		},
	}); err != nil {
		t.Fatalf("upsert child: %v", err)
	}

	pc := &PipelineCoordinator{
		workflowStore: store,
		module: &previewWorkflowModule{
			bundle: &runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{
					FlowPrefix: map[string]string{
						"child": "child",
					},
				},
			},
		},
	}
	if err := pc.projectWorkflowSubjectGates(context.Background(), childEntityID); err != nil {
		t.Fatalf("projectWorkflowSubjectGates: %v", err)
	}

	subject, ok, err := store.Load(context.Background(), subjectID)
	if err != nil {
		t.Fatalf("load subject: %v", err)
	}
	if !ok {
		t.Fatal("subject entity missing after projection")
	}
	gates := workflowStateGatesAsBools(subject.Metadata)
	if !gates["child/g_validated"] {
		t.Fatalf("subject gates missing projected child gate from instanced flow path: %#v", gates)
	}
}

func TestProjectWorkflowSubjectGatesUsesDeepFlowScopeKeyForInstancedPaths(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected workflow fixture bundle")
	}
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	subjectID := "11111111-1111-1111-1111-111111111111"
	flowPath := DeriveFlowInstancePath(source, "grandchild", "inst-1")
	entityID := FlowInstanceEntityID(flowPath)
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      subjectID,
		SubjectID:       subjectID,
		StorageRef:      subjectID,
		WorkflowName:    "root",
		WorkflowVersion: "1.6.0",
		CurrentState:    "idle",
		Metadata: map[string]any{
			"subject_id": subjectID,
		},
	}); err != nil {
		t.Fatalf("upsert subject: %v", err)
	}
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      entityID,
		SubjectID:       subjectID,
		StorageRef:      flowPath,
		WorkflowName:    "grandchild",
		WorkflowVersion: "1.6.0",
		CurrentState:    "done",
		Metadata: map[string]any{
			"subject_id": subjectID,
			"flow_path":  flowPath,
			"gates": map[string]any{
				"child/grandchild/g_ready": true,
			},
		},
	}); err != nil {
		t.Fatalf("upsert deep child: %v", err)
	}

	pc := &PipelineCoordinator{
		workflowStore: store,
		module:        &previewWorkflowModule{bundle: bundle},
	}
	if err := pc.projectWorkflowSubjectGates(context.Background(), entityID); err != nil {
		t.Fatalf("projectWorkflowSubjectGates: %v", err)
	}

	subject, ok, err := store.Load(context.Background(), subjectID)
	if err != nil {
		t.Fatalf("load subject: %v", err)
	}
	if !ok {
		t.Fatal("subject entity missing after projection")
	}
	gates := workflowStateGatesAsBools(subject.Metadata)
	if !gates["child/grandchild/g_ready"] {
		t.Fatalf("subject gates missing projected deep-scope gate from instanced flow path: %#v", gates)
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
	mutation := runtimeengine.StateMutation{
		Metadata: map[string]any{
			"name": "Test Vertical",
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "name", Value: runtimecontracts.LiteralExpression("Test Vertical")},
			},
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
		},
		StateBuckets: map[string]any{},
	}
	mutation := runtimeengine.StateMutation{
		Metadata: map[string]any{
			"composite_score": 71,
			"scoring_rubric":  "corpus_rubric",
		},
	}

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
}

func TestApplyEngineStateMutationCapturesSubjectIDFromMetadata(t *testing.T) {
	instance := &WorkflowInstance{}
	mutation := runtimeengine.StateMutation{
		Metadata: map[string]any{
			"subject_id": "11111111-1111-1111-1111-111111111111",
		},
	}

	applyEngineStateMutation(instance, mutation, nil, nil, "")

	if got := instance.SubjectID; got != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("SubjectID = %q, want propagated subject id", got)
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

	err := pc.updateEntityState(context.Background(), "11111111-1111-1111-1111-111111111111", "marginal_review", "scoring/vertical.marginal")
	if err == nil {
		t.Fatal("expected updateEntityState to fail when workflow store mutate fails")
	}
}

func TestPipelineEngineStateRepoSaveStateRejectsForeignFlowWrite(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := NewWorkflowInstanceStore(db)
	entityID := "11111111-1111-1111-1111-111111111111"
	if err := store.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      entityID,
		SubjectID:       entityID,
		StorageRef:      entityID,
		WorkflowName:    "flow-a",
		WorkflowVersion: "1.6.0",
		CurrentState:    "pending",
		Metadata: map[string]any{
			"subject_id": entityID,
		},
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
	ctx := withPipelineFlowScope(context.Background(), "flow-b")
	err := repo.SaveState(ctx, identity.NormalizeEntityID(entityID), runtimeengine.StateMutation{
		Metadata: map[string]any{"note": "bad write"},
	})
	if err == nil || !strings.Contains(err.Error(), "cross_flow_write_forbidden") {
		t.Fatalf("expected cross_flow_write_forbidden, got %v", err)
	}
}

func TestRecordWorkflowEvidence_ReturnsWorkflowStoreMutationError(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	pc := &PipelineCoordinator{
		workflowStore: NewWorkflowInstanceStore(db),
	}

	err := pc.recordWorkflowEvidence(context.Background(), "11111111-1111-1111-1111-111111111111", "research", map[string]any{"summary": "done"})
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
			Event: (events.Event{
				Type:    "research.completed",
				Payload: []byte(`{"summary":"done"}`),
			}).WithEntityID("11111111-1111-1111-1111-111111111111"),
		},
	})
	if !ok {
		t.Fatal("expected record_evidence action to be claimed")
	}
	if err == nil {
		t.Fatal("expected record_evidence action to return mutation error")
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
		Event: events.Event{
			Type:    events.EventType("child/child.internal"),
			Payload: json.RawMessage(`{"entity_id":"ent-child","step":"done"}`),
		}.WithEntityID("ent-child"),
		State: runtimeengine.StateSnapshot{
			EntityID: identity.NormalizeEntityID("ent-child"),
			Metadata: map[string]any{
				"flow_path":        "child/inst-1",
				"subject_id":       "ent-parent",
				"parent_entity_id": "ent-parent",
			},
		},
	}

	internal, err := shaper.ShapeEmitPayload(context.Background(), req, "child/child.internal", map[string]any{"entity_id": "ent-child", "step": "done"})
	if err != nil {
		t.Fatalf("ShapeEmitPayload internal: %v", err)
	}
	if got := internal["entity_id"]; got != "ent-child" {
		t.Fatalf("internal emit entity_id = %#v, want ent-child", got)
	}
	if got := internal["step"]; got != "done" {
		t.Fatalf("internal emit step = %#v, want done", got)
	}

	output, err := shaper.ShapeEmitPayload(context.Background(), req, "child/child.done", map[string]any{"entity_id": "ent-child", "step": "done"})
	if err != nil {
		t.Fatalf("ShapeEmitPayload output: %v", err)
	}
	if got := output["entity_id"]; got != "ent-parent" {
		t.Fatalf("output emit entity_id = %#v, want ent-parent", got)
	}
	if _, ok := output["step"]; ok {
		t.Fatalf("output emit step should be trimmed to the target event schema: %#v", output["step"])
	}
}

func TestPipelineEnginePayloadShaper_TrimsUndeclaredFieldsAcrossCrossFlowOutputRetarget(t *testing.T) {
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
		Event: events.Event{
			Type:    events.EventType("child/child.internal"),
			Payload: json.RawMessage(`{"entity_id":"ent-child"}`),
		}.WithEntityID("ent-child"),
		State: runtimeengine.StateSnapshot{
			EntityID: identity.NormalizeEntityID("ent-child"),
			Metadata: map[string]any{
				"flow_path":        "child/inst-1",
				"subject_id":       "ent-parent",
				"parent_entity_id": "ent-parent",
			},
		},
	}

	output, err := shaper.ShapeEmitPayload(context.Background(), req, "child/child.done", map[string]any{
		"entity_id":   "ent-child",
		"vertical_id": "ent-child",
		"result":      "accepted",
	})
	if err != nil {
		t.Fatalf("ShapeEmitPayload output: %v", err)
	}
	if got := output["entity_id"]; got != "ent-parent" {
		t.Fatalf("output emit entity_id = %#v, want ent-parent", got)
	}
	if _, ok := output["vertical_id"]; ok {
		t.Fatalf("output emit vertical_id should be trimmed to the target event schema: %#v", output["vertical_id"])
	}
	if _, ok := output["result"]; ok {
		t.Fatalf("output emit result should be trimmed to the target event schema: %#v", output["result"])
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
func (s *recordingScheduleStore) MarkScheduleFiredExact(context.Context, Schedule) error { return nil }

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
	id := identity.NormalizeActionKey("create_flow_instance")

	if !registry.HasAction(id) {
		t.Fatal("expected builtin action to be discoverable without explicit registry entry")
	}
	if !registry.IsExecutable(id) {
		t.Fatal("expected builtin action to be executable without explicit registry entry")
	}
	instruction, ok := registry.Action(id)
	if !ok {
		t.Fatal("expected builtin action instruction")
	}
	if got := instruction.Builtin; got != "create_flow_instance" {
		t.Fatalf("Builtin = %q", got)
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
