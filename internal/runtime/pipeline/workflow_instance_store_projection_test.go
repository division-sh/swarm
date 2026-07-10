package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkflowInstanceStoreProjection_RoundTripPreservesCanonicalState(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := NewWorkflowInstanceStore(db)
	storageRef := uuid.NewString()
	parentID := uuid.NewString()
	parentFlowID := "operating"
	parentFlowInstance := "operating/root"
	now := time.Now().UTC().Round(time.Microsecond)

	instance := WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      storageRef,
		WorkflowName:    "projection-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "active",
		EnteredStageAt:  now,
		Config: map[string]any{
			"custom_threshold": float64(3),
		},
		TransitionHistory: []WorkflowTransitionRecord{{
			TransitionID:   "tr-1",
			From:           "queued",
			To:             "active",
			TriggerEventID: "evt-1",
			FiredAt:        now,
		}},
		StateBuckets: map[string]any{
			"evidence": map[string]any{
				"audit": []any{
					map[string]any{"kind": "note", "score": float64(1)},
				},
			},
		},
		Metadata: map[string]any{
			"business_brief":       map[string]any{"title": "hello"},
			"status":               "open",
			"slug":                 "projection",
			"name":                 "Projection Flow",
			"entity_type":          "workflow_subject",
			"instance_id":          "inst-1",
			"flow_path":            "review/inst-1",
			"instance_kind":        "materialized",
			"template_version":     "v1",
			"last_source_event":    "review.started",
			"parent_flow_id":       parentFlowID,
			"parent_flow_instance": parentFlowInstance,
			"parent_entity_id":     parentID,
			"gates": map[string]any{
				"g_ready": true,
			},
		},
	}

	if err := store.Upsert(testWorkflowStoreRunContext(t, store), instance); err != nil {
		t.Fatalf("upsert workflow instance: %v", err)
	}

	loaded, ok, err := store.Load(testWorkflowStoreRunContext(t, store), storageRef)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := loaded.InstanceID; got != "inst-1" {
		t.Fatalf("InstanceID = %q, want inst-1", got)
	}
	if got := loaded.WorkflowVersion; got != "1.0.0" {
		t.Fatalf("WorkflowVersion = %q, want 1.0.0", got)
	}
	if got := strings.TrimSpace(asString(loaded.Config["custom_threshold"])); got != "3" {
		t.Fatalf("Config custom_threshold = %#v, want 3", loaded.Config["custom_threshold"])
	}
	if got := strings.TrimSpace(asString(loaded.Metadata["status"])); got != "open" {
		t.Fatalf("Metadata status = %#v, want open", loaded.Metadata["status"])
	}
	if got := strings.TrimSpace(asString(loaded.Metadata["flow_path"])); got != "review/inst-1" {
		t.Fatalf("Metadata flow_path = %#v, want review/inst-1", loaded.Metadata["flow_path"])
	}
	if got := strings.TrimSpace(asString(loaded.Metadata["parent_flow_id"])); got != parentFlowID {
		t.Fatalf("Metadata parent_flow_id = %#v, want %q", loaded.Metadata["parent_flow_id"], parentFlowID)
	}
	if got := strings.Trim(strings.TrimSpace(asString(loaded.Metadata["parent_flow_instance"])), "/"); got != parentFlowInstance {
		t.Fatalf("Metadata parent_flow_instance = %#v, want %q", loaded.Metadata["parent_flow_instance"], parentFlowInstance)
	}
	if got := strings.TrimSpace(asString(loaded.Metadata["parent_entity_id"])); got != parentID {
		t.Fatalf("Metadata parent_entity_id = %#v, want %q", loaded.Metadata["parent_entity_id"], parentID)
	}
	if got := strings.TrimSpace(asString(loaded.Metadata["storage_ref"])); got != storageRef {
		t.Fatalf("Metadata storage_ref = %#v, want %q", loaded.Metadata["storage_ref"], storageRef)
	}
	if got := strings.TrimSpace(asString(loaded.Metadata["subject_id"])); got != "" {
		t.Fatalf("Metadata subject_id = %#v, want empty", loaded.Metadata["subject_id"])
	}
	if gates := workflowStateGatesAsBools(loaded.Metadata); !gates["g_ready"] {
		t.Fatalf("Metadata gates = %#v, want g_ready=true", loaded.Metadata["gates"])
	}
	if len(loaded.TransitionHistory) != 1 || loaded.TransitionHistory[0].TransitionID != "tr-1" {
		t.Fatalf("TransitionHistory = %#v, want tr-1", loaded.TransitionHistory)
	}
	gotEvidence, ok := workflowStateBucketObject(loaded, "evidence")
	if !ok {
		t.Fatal("expected evidence bucket")
	}
	wantEvidence := `{"audit":[{"kind":"note","score":1}]}`
	if got := mustCanonicalJSONString(t, gotEvidence); got != wantEvidence {
		t.Fatalf("evidence = %s, want %s", got, wantEvidence)
	}
	identity, err := workflowInstancePersistedIdentity(nil, loaded)
	if err != nil {
		t.Fatalf("workflowInstancePersistedIdentity(loaded): %v", err)
	}
	if identity.StorageRef != storageRef {
		t.Fatalf("identity.StorageRef = %q, want %q", identity.StorageRef, storageRef)
	}
	if identity.ScopeKey != "review" {
		t.Fatalf("identity.ScopeKey = %q, want review", identity.ScopeKey)
	}
	if identity.InstancePath != "review/inst-1" {
		t.Fatalf("identity.InstancePath = %q, want review/inst-1", identity.InstancePath)
	}
	if identity.InstanceID != "inst-1" {
		t.Fatalf("identity.InstanceID = %q, want inst-1", identity.InstanceID)
	}
	if identity.EntityID != runtimeflowidentity.EntityID("review/inst-1") {
		t.Fatalf("identity.EntityID = %q, want canonical id for review/inst-1", identity.EntityID)
	}
	if identity.ParentRoute.FlowID != parentFlowID {
		t.Fatalf("identity.ParentRoute.FlowID = %q, want %q", identity.ParentRoute.FlowID, parentFlowID)
	}
	if identity.ParentRoute.FlowInstance != parentFlowInstance {
		t.Fatalf("identity.ParentRoute.FlowInstance = %q, want %q", identity.ParentRoute.FlowInstance, parentFlowInstance)
	}
	if identity.ParentRoute.EntityID != parentID {
		t.Fatalf("identity.ParentRoute.EntityID = %q, want %q", identity.ParentRoute.EntityID, parentID)
	}
}

func TestWorkflowInstanceStoreProjection_DoesNotExposeControlStatusAsEntityField(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	workflowStore := NewWorkflowInstanceStore(db)
	storageRef := uuid.NewString()
	ctx := testWorkflowStoreRunContext(t, workflowStore)
	instance := WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      storageRef,
		WorkflowName:    "projection-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "reviewing",
		EnteredStageAt:  time.Now().UTC().Round(time.Microsecond),
		Config: map[string]any{
			"status": "waiting",
		},
		Metadata: map[string]any{
			"status":      "entity-open",
			"entity_type": "workflow_subject",
			"slug":        "projection",
			"name":        "Projection Flow",
		},
		StateBuckets: map[string]any{
			"score": float64(9),
		},
	}

	if err := workflowStore.Upsert(ctx, instance); err != nil {
		t.Fatalf("upsert workflow instance: %v", err)
	}

	loaded, ok, err := workflowStore.Load(ctx, storageRef)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := loaded.CurrentState; got != "reviewing" {
		t.Fatalf("CurrentState = %q, want reviewing", got)
	}
	if got := strings.TrimSpace(asString(loaded.Metadata["status"])); got != "entity-open" {
		t.Fatalf("Metadata status = %#v, want entity-open", loaded.Metadata["status"])
	}

	var currentState string
	var fieldsRaw []byte
	var controlStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT es.current_state, es.fields, COALESCE(fi.config->>'status', '')
		FROM entity_state es
		JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = $1::uuid AND es.entity_id = $2::uuid
	`, testPipelineRunID, workflowInstanceRowID(storageRef)).Scan(&currentState, &fieldsRaw, &controlStatus); err != nil {
		t.Fatalf("query entity_state projection: %v", err)
	}
	if got := strings.TrimSpace(currentState); got != "reviewing" {
		t.Fatalf("entity_state.current_state = %q, want reviewing", got)
	}
	fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", fieldsRaw)
	if err != nil {
		t.Fatalf("decode entity_state.fields: %v", err)
	}
	if got := fields["status"]; got != "entity-open" {
		t.Fatalf("entity_state.fields status = %#v, want entity-open", got)
	}
	if got := controlStatus; got != "waiting" {
		t.Fatalf("flow_instances.config status = %q, want waiting", got)
	}
}

func TestWorkflowInstanceStoreCreateRejectsDuplicateWithoutMutatingProjection(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := NewWorkflowInstanceStore(db)
	ctx := testWorkflowStoreRunContext(t, store)
	const storageRef = "review/inst-1"
	first := WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      storageRef,
		WorkflowName:    "review",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Config: map[string]any{
			"name": "alpha",
		},
		Metadata: map[string]any{
			"business_brief": "first",
			"entity_type":    "workflow_subject",
			"flow_path":      storageRef,
			"instance_id":    "inst-1",
		},
		StateBuckets: map[string]any{
			"score": map[string]any{"value": float64(1)},
		},
	}
	if err := store.Create(ctx, first); err != nil {
		t.Fatalf("create workflow instance: %v", err)
	}

	duplicate := first
	duplicate.CurrentState = "mutated"
	duplicate.Config = map[string]any{
		"name": "beta",
	}
	duplicate.Metadata = map[string]any{
		"business_brief": "second",
		"entity_type":    "workflow_subject",
		"flow_path":      storageRef,
		"instance_id":    "inst-1",
	}
	duplicate.StateBuckets = map[string]any{
		"score": map[string]any{"value": float64(99)},
	}
	err := store.Create(ctx, duplicate)
	failure, ok := runtimefailures.As(err)
	if err == nil || !ok || failure.Failure.Class != runtimefailures.ClassConflictingDuplicate || failure.Failure.Detail.Code != "flow_instance_already_exists" || failure.Failure.Detail.Attributes["flow_instance"] != storageRef {
		t.Fatalf("duplicate create failure = %#v, want canonical already-exists failure", failure)
	}

	loaded, ok, err := store.Load(ctx, storageRef)
	if err != nil {
		t.Fatalf("load workflow instance after duplicate create: %v", err)
	}
	if !ok {
		t.Fatal("expected original workflow instance to remain")
	}
	if got := loaded.CurrentState; got != "queued" {
		t.Fatalf("CurrentState = %q, want queued", got)
	}
	if got := loaded.Config["name"]; got != "alpha" {
		t.Fatalf("Config name = %#v, want alpha", got)
	}
	if got := loaded.Metadata["business_brief"]; got != "first" {
		t.Fatalf("Metadata business_brief = %#v, want first", got)
	}
	gotScore, ok := workflowStateBucketObject(loaded, "score")
	if !ok || gotScore["value"] != float64(1) {
		t.Fatalf("StateBuckets score = %#v ok=%v, want 1", gotScore, ok)
	}

	var (
		revision   int
		configName string
		fieldsRaw  []byte
	)
	if err := db.QueryRowContext(ctx, `
		SELECT es.revision, COALESCE(fi.config->>'name', ''), es.fields
		FROM entity_state es
		JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = $1::uuid AND es.entity_id = $2::uuid
	`, testPipelineRunID, workflowInstanceRowID(storageRef)).Scan(&revision, &configName, &fieldsRaw); err != nil {
		t.Fatalf("query persisted projection after duplicate create: %v", err)
	}
	if revision != 1 {
		t.Fatalf("entity_state.revision = %d, want 1", revision)
	}
	if configName != "alpha" {
		t.Fatalf("flow_instances.config name = %q, want alpha", configName)
	}
	fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", fieldsRaw)
	if err != nil {
		t.Fatalf("decode entity_state.fields: %v", err)
	}
	if got := fields["business_brief"]; got != "first" {
		t.Fatalf("entity_state.fields business_brief = %#v, want first", got)
	}
}

func TestWorkflowInstanceStoreProjection_StaticRowsDoNotGainMaterializedFlowPathOnRoundTrip(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := NewWorkflowInstanceStore(db)
	storageRef := uuid.NewString()
	instance := WorkflowInstance{
		InstanceID:      storageRef,
		StorageRef:      storageRef,
		WorkflowName:    "static-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Metadata: map[string]any{
			"instance_id": storageRef,
		},
		StateBuckets: map[string]any{},
	}

	if err := store.Upsert(testWorkflowStoreRunContext(t, store), instance); err != nil {
		t.Fatalf("upsert static workflow instance: %v", err)
	}

	loaded, ok, err := store.Load(testWorkflowStoreRunContext(t, store), storageRef)
	if err != nil {
		t.Fatalf("load static workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected static workflow instance to persist")
	}
	if got := strings.TrimSpace(asString(loaded.Metadata["flow_path"])); got != "" {
		t.Fatalf("Metadata flow_path = %#v, want empty for static row", loaded.Metadata["flow_path"])
	}
	identity, err := workflowInstancePersistedIdentity(nil, loaded)
	if err != nil {
		t.Fatalf("workflowInstancePersistedIdentity(static): %v", err)
	}
	if identity.HasStoredPath {
		t.Fatalf("identity.HasStoredPath = true, want false for static row")
	}
	if identity.ScopeKey != "static-flow" {
		t.Fatalf("identity.ScopeKey = %q, want static-flow", identity.ScopeKey)
	}
	if identity.InstancePath != "static-flow" {
		t.Fatalf("identity.InstancePath = %q, want canonical static path", identity.InstancePath)
	}
}

func TestWorkflowInstanceStoreProjection_RejectsMalformedPersistedShapes(t *testing.T) {
	cases := []struct {
		name         string
		mutateSQL    string
		mutateKey    string
		mutateArg    string
		wantContains string
	}{
		{
			name:         "fields not object",
			mutateSQL:    `UPDATE entity_state SET fields = $2::jsonb WHERE entity_id = $1::uuid`,
			mutateKey:    "entity",
			mutateArg:    `[]`,
			wantContains: "entity_state.fields must be a JSON object",
		},
		{
			name:         "gates not bool map",
			mutateSQL:    `UPDATE entity_state SET gates = $2::jsonb WHERE entity_id = $1::uuid`,
			mutateKey:    "entity",
			mutateArg:    `{"g_ready":1}`,
			wantContains: "entity_state.gates must be an object of booleans",
		},
		{
			name:         "accumulator not object",
			mutateSQL:    `UPDATE entity_state SET accumulator = $2::jsonb WHERE entity_id = $1::uuid`,
			mutateKey:    "entity",
			mutateArg:    `[]`,
			wantContains: "entity_state.accumulator must be a JSON object",
		},
		{
			name:         "control metadata malformed",
			mutateSQL:    `UPDATE flow_instances SET config = $2::jsonb WHERE instance_id = $1`,
			mutateKey:    "storage",
			mutateArg:    `{"workflow_version":"1.0.0","instance_id":"inst-1","storage_ref":"storage-ref","transition_history":"bad"}`,
			wantContains: "flow_instances.config transition_history must be an array of workflow transition records",
		},
		{
			name:         "instance id disagrees with flow path",
			mutateSQL:    `UPDATE flow_instances SET config = $2::jsonb WHERE instance_id = $1`,
			mutateKey:    "storage",
			mutateArg:    `{"workflow_version":"1.0.0","instance_id":"inst-2","storage_ref":"storage-ref","flow_path":"review/inst-1"}`,
			wantContains: "disagrees with flow_instance_path",
		},
		{
			name:         "slash-only flow path normalizes before fallback",
			mutateSQL:    `UPDATE flow_instances SET config = $2::jsonb WHERE instance_id = $1`,
			mutateKey:    "storage",
			mutateArg:    `{"workflow_version":"1.0.0","instance_id":"inst-1","storage_ref":"storage-ref","flow_path":"/"}`,
			wantContains: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)

			store := NewWorkflowInstanceStore(db)
			storageRef := "storage-ref"
			if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
				InstanceID:      "inst-1",
				StorageRef:      storageRef,
				WorkflowName:    "projection-flow",
				WorkflowVersion: "1.0.0",
				CurrentState:    "queued",
				Metadata: map[string]any{
					"instance_id": "inst-1",
				},
				StateBuckets: map[string]any{},
			}); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			mutateID := storageRef
			if tc.mutateKey == "entity" {
				mutateID = workflowInstanceRowID(storageRef)
			}
			if _, err := db.ExecContext(context.Background(), tc.mutateSQL, mutateID, tc.mutateArg); err != nil {
				t.Fatalf("mutate malformed persisted shape: %v", err)
			}

			loaded, ok, err := store.Load(testWorkflowStoreRunContext(t, store), storageRef)
			if tc.wantContains == "" {
				if err != nil {
					t.Fatalf("load with slash-only flow_path: %v", err)
				}
				if !ok {
					t.Fatal("expected slash-only flow_path row to load")
				}
				if got := strings.TrimSpace(asString(loaded.Metadata["instance_id"])); got != "inst-1" {
					t.Fatalf("loaded Metadata instance_id = %#v, want inst-1", loaded.Metadata["instance_id"])
				}
				return
			}
			if err == nil {
				t.Fatal("expected load to fail on malformed persisted shape")
			}
			if !strings.Contains(err.Error(), tc.wantContains) {
				t.Fatalf("load error = %v, want substring %q", err, tc.wantContains)
			}
		})
	}
}

func mustCanonicalJSONString(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal canonical json: %v", err)
	}
	return string(raw)
}
