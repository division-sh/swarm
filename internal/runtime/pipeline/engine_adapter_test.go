package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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

	err := pc.recordWorkflowEvidence(testPipelineRunContextNoSeed(), "11111111-1111-1111-1111-111111111111", "research", map[string]any{"summary": "done"})
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

func TestPipelineEngineActionRunner_MailboxWriteMaterializesIdempotentRow(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pc := &PipelineCoordinator{db: db}
	runner := pipelineEngineActionRunner{coordinator: pc}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	txctx := WithPipelineSQLTxContext(ctx, tx)
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
	evt := (events.Event{
		ID:      eventID,
		Type:    "mailbox.review_requested",
		Payload: []byte(`{"review_kind":"validation"}`),
	}).WithEnvelope(events.EventEnvelope{
		EntityID:     entityID,
		FlowInstance: "validation/case-1",
		Scope:        events.EventScopeEntity,
	})
	base := values.NewContext()
	base.Event = values.Wrap(evt.ContextMap(""))
	base.Payload = values.Wrap(parsePayloadMap(evt.Payload))
	execCtx := runtimeengine.ExecutionContext{
		Base: base,
		Request: runtimeengine.ExecutionRequest{
			EntityID: identity.NormalizeEntityID(entityID),
			NodeID:   identity.NormalizeNodeID("mailbox-node"),
			Event:    evt,
		},
	}
	for i := 0; i < 2; i++ {
		ok, err := runner.ExecuteAction(txctx, action, runtimeregistry.ActionInstruction{Builtin: "mailbox_write"}, execCtx)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("ExecuteAction iteration %d: %v", i, err)
		}
		if !ok {
			_ = tx.Rollback()
			t.Fatalf("ExecuteAction iteration %d was not claimed", i)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var count int
	var sourceEventID, gotEntityID, flowInstance, itemType, severity, summary, payloadKind, fromAgent, status string
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COALESCE(MAX(source_event_id::text), ''),
		       COALESCE(MAX(entity_id::text), ''),
		       COALESCE(MAX(flow_instance), ''),
		       COALESCE(MAX(item_type), ''),
		       COALESCE(MAX(severity), ''),
		       COALESCE(MAX(summary), ''),
		       COALESCE(MAX(payload->>'review_kind'), ''),
		       COALESCE(MAX(from_agent), ''),
		       COALESCE(MAX(status), '')
		FROM mailbox
		WHERE source_event_id = $1::uuid
	`, eventID).Scan(&count, &sourceEventID, &gotEntityID, &flowInstance, &itemType, &severity, &summary, &payloadKind, &fromAgent, &status); err != nil {
		t.Fatalf("query mailbox: %v", err)
	}
	if count != 1 {
		t.Fatalf("mailbox row count = %d, want 1", count)
	}
	if sourceEventID != eventID || gotEntityID != entityID || flowInstance != "validation/case-1" {
		t.Fatalf("mailbox identity = source %q entity %q flow %q", sourceEventID, gotEntityID, flowInstance)
	}
	if itemType != "review_request" || severity != "urgent" || summary != "Review validation package" || status != "pending" {
		t.Fatalf("mailbox row fields type=%q severity=%q summary=%q status=%q", itemType, severity, summary, status)
	}
	if payloadKind != "validation" {
		t.Fatalf("payload review_kind = %q", payloadKind)
	}
	if fromAgent != "system_node:mailbox-node" {
		t.Fatalf("from_agent = %q", fromAgent)
	}
}

func TestPipelineEngineActionRunner_MailboxWriteFailsClosedOnMissingRequiredExpression(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	runner := pipelineEngineActionRunner{coordinator: &PipelineCoordinator{db: db}}
	ctx := context.Background()
	eventID := "33333333-3333-3333-3333-333333333333"
	action := runtimecontracts.ActionSpec{
		ID: "mailbox_write",
		Mailbox: &runtimecontracts.MailboxWriteSpec{
			ItemType: runtimecontracts.LiteralExpression("review_request"),
			Summary:  runtimecontracts.RefExpression("payload.missing_summary"),
		},
	}
	evt := events.Event{ID: eventID, Type: "mailbox.review_requested", Payload: []byte(`{}`)}
	base := values.NewContext()
	base.Event = values.Wrap(evt.ContextMap(""))
	base.Payload = values.Wrap(parsePayloadMap(evt.Payload))

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
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE source_event_id = $1::uuid`, eventID).Scan(&count); err != nil {
		t.Fatalf("query mailbox count: %v", err)
	}
	if count != 0 {
		t.Fatalf("mailbox row count = %d, want 0", count)
	}
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
		WorkflowName:    "spec-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
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
	if got := strings.TrimSpace(asString(instance.Metadata["repo_url"])); got != "swarm-artifact://spec-repos/"+initial["spec_repo_id"].(string) {
		t.Fatalf("repo_url = %q", got)
	}
	ref := strings.TrimSpace(asString(instance.Metadata["current_ref"]))
	if len(ref) != 40 {
		t.Fatalf("current_ref length = %d ref=%q", len(ref), ref)
	}
	manifest, ok := instance.Metadata["file_manifest"].(map[string]any)
	if !ok {
		t.Fatalf("file_manifest = %#v", instance.Metadata["file_manifest"])
	}
	if got := strings.TrimSpace(asString(manifest["source_event_id"])); got != execCtx.Request.Event.ID {
		t.Fatalf("manifest source_event_id = %q", got)
	}
	files, ok := manifest["files"].([]any)
	if !ok || len(files) != 1 {
		t.Fatalf("manifest files = %#v", manifest["files"])
	}
	repoPath, err := artifactRepoPath(artifactRoot, testPipelineRunID, initial["vertical_id"].(string), initial["business_slug"].(string), initial["spec_repo_id"].(string))
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
		WorkflowName:    "spec-repo",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		Metadata:        cloneStringAnyMap(initial),
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	action, execCtx := testArtifactRepoActionAndContext(entityID, initial, "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", "name: Demo\n")
	action.ArtifactRepo.Files[0].Path = runtimecontracts.LiteralExpression("../escape.yaml")

	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(ctx, action, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, execCtx)
	if !ok {
		t.Fatal("expected artifact_repo_commit action to be claimed")
	}
	if err == nil || !strings.Contains(err.Error(), "path traversal is not allowed") {
		t.Fatalf("ExecuteAction error = %v, want path traversal", err)
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
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("failure event count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type); got != "spec_repo.commit_failed" {
		t.Fatalf("failure event type = %q", got)
	}
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
		WorkflowName:    "spec-repo",
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

	conflictAction, conflictCtx := testArtifactRepoActionAndContext(entityID, afterNext.Metadata, "77777777-7777-7777-7777-777777777777", "44444444-4444-4444-4444-444444444444", "name: Changed\n")
	ok, err := pipelineEngineActionRunner{coordinator: pc}.ExecuteAction(ctx, conflictAction, runtimeregistry.ActionInstruction{Builtin: "artifact_repo_commit"}, conflictCtx)
	if !ok {
		t.Fatal("expected artifact_repo_commit action to be claimed")
	}
	if err == nil || !strings.Contains(err.Error(), "conflicts with previously committed tree") {
		t.Fatalf("ExecuteAction error = %v, want request conflict", err)
	}
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

func testArtifactRepoEntityFields() map[string]any {
	return map[string]any{
		"spec_repo_id":              "11111111-1111-1111-1111-111111111111",
		"vertical_id":               "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		"source_validation_case_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
		"business_slug":             "Acme CRM",
	}
}

func testArtifactRepoActionAndContext(entityID string, entity map[string]any, eventID, requestID, content string) (runtimecontracts.ActionSpec, runtimeengine.ExecutionContext) {
	payload := map[string]any{
		"request_id": requestID,
		"mvp_yaml":   content,
	}
	payloadBytes, _ := json.Marshal(payload)
	evt := events.Event{
		ID:        eventID,
		Type:      "spec_file.commit_requested",
		RunID:     testPipelineRunID,
		Payload:   payloadBytes,
		CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
	}.WithEnvelope(events.EventEnvelope{EntityID: entityID})
	base := values.NewContext()
	base.Event = values.Wrap(evt.ContextMap("ready"))
	base.Payload = values.Wrap(payload)
	base.Entity = values.Wrap(entity)
	stateMetadata := cloneStringAnyMap(entity)
	return runtimecontracts.ActionSpec{
			ID: "artifact_repo_commit",
			ArtifactRepo: &runtimecontracts.ArtifactRepoSpec{
				Provider:               "local_git",
				RepoID:                 runtimecontracts.RefExpression("entity.spec_repo_id"),
				RunID:                  runtimecontracts.RefExpression("event.run_id"),
				VerticalID:             runtimecontracts.RefExpression("entity.vertical_id"),
				BusinessSlug:           runtimecontracts.RefExpression("entity.business_slug"),
				SourceValidationCaseID: runtimecontracts.RefExpression("entity.source_validation_case_id"),
				RequestID:              runtimecontracts.RefExpression("payload.request_id"),
				Author:                 runtimecontracts.LiteralExpression("validation-agent"),
				AllowedPaths:           []string{"specs/mvp.yaml"},
				Files: []runtimecontracts.ArtifactRepoFileSpec{{
					Path:        runtimecontracts.LiteralExpression("specs/mvp.yaml"),
					Content:     runtimecontracts.RefExpression("payload.mvp_yaml"),
					ContentType: "yaml",
					MaxBytes:    4096,
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
				FailureEvent: "spec_repo.commit_failed",
				FailurePayload: map[string]runtimecontracts.ExpressionValue{
					"request_id": runtimecontracts.RefExpression("payload.request_id"),
				},
			},
		}, runtimeengine.ExecutionContext{
			Base: base,
			Request: runtimeengine.ExecutionRequest{
				EntityID: identity.NormalizeEntityID(entityID),
				NodeID:   identity.NormalizeNodeID("artifact-node"),
				Event:    evt,
				State: runtimeengine.StateSnapshot{
					EntityID:        identity.NormalizeEntityID(entityID),
					WorkflowName:    "spec-repo",
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
		Event: events.Event{
			Type:    events.EventType("child/child.internal"),
			Payload: json.RawMessage(`{"entity_id":"ent-child","step":"done"}`),
		}.WithEntityID("ent-child"),
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
		Event: events.Event{
			Type:    events.EventType("child/child.internal"),
			Payload: json.RawMessage(`{"entity_id":"ent-child"}`),
		}.WithEntityID("ent-child"),
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
		Event: events.Event{
			Type:    events.EventType("child/child.internal"),
			Payload: json.RawMessage(`{"entity_id":"ent-child","step":"done"}`),
		}.WithEntityID("ent-child"),
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
		Event: events.Event{
			Type:    events.EventType("child/child.start"),
			Payload: json.RawMessage(`{"entity_id":"ent-child"}`),
		}.WithEntityID("ent-child"),
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
		Event: events.Event{
			Type:    events.EventType("child/child.start"),
			Payload: json.RawMessage(`{"entity_id":"ent-child"}`),
		}.WithEntityID("ent-child"),
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
		Event: events.Event{
			Type:    events.EventType("child/child.internal"),
			Payload: json.RawMessage(`{"entity_id":"ent-child"}`),
		}.WithEntityID("ent-child"),
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
		Event: events.Event{
			Type:    events.EventType("child/child.internal"),
			Payload: json.RawMessage(`{"entity_id":"ent-child","step":"done"}`),
		}.WithEntityID("ent-child"),
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
