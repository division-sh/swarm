package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/uuid"
)

func TestWorkflowInstanceStoreProjectionRejectsFieldsWithoutEntityContract(t *testing.T) {
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: "inst-1", StorageRef: "review/inst-1", EntityID: uuid.NewString(),
		WorkflowName: "review", WorkflowVersion: "1", CurrentState: "active", Fields: map[string]any{"value": "business-field"},
	})
	if _, err := workflowInstancePersistedProjectionFromInstance(instance, instance.StorageRef); err == nil || !strings.Contains(err.Error(), "fieldless workflow header cannot carry entity fields") {
		t.Fatalf("missing entity contract projection error = %v", err)
	}
}

func TestWorkflowInstanceReadRejectsUnexpectedFieldRow(t *testing.T) {
	now := time.Date(2026, time.August, 23, 4, 10, 0, 0, time.UTC)
	record := WorkflowInstancePersistenceRecord{
		EntityID: uuid.NewString(), WorkflowName: "review", WorkflowVersion: "1", Mode: "template", Status: "active",
		CurrentState: "active", Revision: 1, EnteredStageAt: now,
		Gates: []byte(`{}`), Fields: []byte(`{}`), Bookkeeping: []byte(`{}`), Accumulator: []byte(`{}`),
		Config:       []byte(`{"workflow_version":"1","instance_id":"inst-1","flow_path":"review/inst-1"}`),
		FlowInstance: "review/inst-1", EntityType: "   ", CreatedAt: now, UpdatedAt: now,
	}
	if _, err := DecodeWorkflowInstancePersistenceRecord(record); err == nil || !strings.Contains(err.Error(), "fieldless workflow review/inst-1 has an unexpected field row") {
		t.Fatalf("blank entity contract read error = %v", err)
	}
	record.EntityType = ""
	record.Fields = nil
	if instance, err := DecodeWorkflowInstancePersistenceRecord(record); err != nil || instance.EntityID != record.EntityID || len(instance.Fields) != 0 || instance.CurrentState != record.CurrentState {
		t.Fatalf("fieldless constructed read changed header: instance=%+v err=%v", instance, err)
	}
	record.EntityType = "review_subject"
	if _, err := DecodeWorkflowInstancePersistenceRecord(record); err == nil || !strings.Contains(err.Error(), "missing its declared field row") {
		t.Fatalf("declared field row absence accepted: %v", err)
	}
}

func VerifyWorkflowInstanceFixtureListUsesConstructedHeadersForTest(t *testing.T, open func(*testing.T, string) WorkflowProjectionHeaderNativeFixtureForTest) {
	fixture := open(t, testPipelineRunID)
	ctx := fixture.Context

	for _, fielded := range []bool{false, true} {
		path, entityType := "fieldless", ""
		var fields map[string]any
		if fielded {
			path, entityType = "fielded", "subject"
			fields = map[string]any{"value": "business"}
		}
		instance := materializedWorkflowInstanceForTest(WorkflowInstance{
			StorageRef: path, WorkflowName: path, WorkflowVersion: "fixture", CurrentState: "active", StageDefined: true,
			EntityType: entityType, Fields: fields, Gates: map[string]bool{"ready": true},
			Bookkeeping: map[string]any{"authority": "header"}, StateBuckets: map[string]any{"count": int64(1)},
		})
		if err := fixture.Construct(ctx, instance); err != nil {
			t.Fatalf("construct fixture %s: %v", path, err)
		}
	}
	changed, err := fixture.ObsoleteFieldRows(ctx, testPipelineRunID)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("obsolete shadow fixture: rows=%d err=%v", changed, err)
	}

	instances, err := fixture.List(ctx, testPipelineRunID)
	if err != nil || len(instances) != 2 {
		t.Fatalf("list constructed headers: count=%d err=%v", len(instances), err)
	}
	for _, instance := range instances {
		if instance.CurrentState != "active" || !instance.StageDefined || !instance.Gates["ready"] || instance.Bookkeeping["authority"] != "header" || instance.StateBuckets["count"] != int64(1) {
			t.Fatalf("list consumed obsolete field-row lifecycle: %+v", instance)
		}
		if instance.StorageRef == "fieldless" && (instance.EntityType != "" || len(instance.Fields) != 0) {
			t.Fatalf("fieldless header acquired fields: %+v", instance)
		}
		if instance.StorageRef == "fielded" && (instance.EntityType != "subject" || instance.Fields["value"] != "business") {
			t.Fatalf("declared field projection changed: %+v", instance)
		}
	}
}

func TestPersistedWorkflowStatePreservesIntegerForCELArithmetic(t *testing.T) {
	now := time.Date(2026, time.August, 31, 1, 2, 3, 0, time.UTC)
	instance, err := DecodeWorkflowInstancePersistenceRecord(WorkflowInstancePersistenceRecord{
		EntityID: uuid.NewString(), WorkflowName: "review", WorkflowVersion: "1", Mode: "template", Status: "active",
		CurrentState: "active", Revision: 1, EnteredStageAt: now,
		Gates: []byte(`{}`), Fields: []byte(`{"integer":75,"decimal":75.0,"exponent":75e0}`), Bookkeeping: []byte(`{}`), Accumulator: []byte(`{}`),
		Config:       []byte(`{"workflow_version":"1","instance_id":"inst-1","flow_path":"review/inst-1"}`),
		FlowInstance: "review/inst-1", EntityType: "review_subject", CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("decode persisted workflow state: %v", err)
	}
	if got := instance.Fields["integer"]; got != int64(75) {
		t.Fatalf("persisted integer = %#v, want admitted integer", got)
	}
	if instance.Fields["decimal"] != float64(75) || instance.Fields["exponent"] != float64(75) {
		t.Fatalf("persisted decimal carriers = %#v, want doubles", instance.Fields)
	}
	entityType := pipelineExpressionObjectType("numeric.persisted", map[string]runtimecontracts.CatalogTypeKind{
		"integer": runtimecontracts.CatalogTypeInteger, "decimal": runtimecontracts.CatalogTypeNumber, "exponent": runtimecontracts.CatalogTypeNumber,
	})
	matched, err := newWorkflowExpressionEvaluator().EvalBoolWithOptions(
		"entity.integer + 1 == 76 && double(entity.decimal) + 1.0 == 76.0 && double(entity.exponent) + 1.0 == 76.0",
		workflowExpressionContext{Entity: instance.Fields}, workflowexpr.ValueExpressionOptions{EntityType: &entityType},
	)
	if err != nil || !matched {
		t.Fatalf("persisted workflow-state integer arithmetic = %v err=%v", matched, err)
	}
}

func TestWorkflowStateWriterPreservesNativeWholeDoubleForCELArithmetic(t *testing.T) {
	now := time.Date(2026, time.August, 31, 1, 2, 3, 0, time.UTC)
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{
		StorageRef: "review/inst-1", EntityType: "review_subject",
		WorkflowName: "review", WorkflowVersion: "1", CurrentState: "active",
		Fields: map[string]any{
			"integer": int64(75),
			"double":  float64(75),
			"nested":  []any{json.Number("75.0"), json.Number("75e0")},
		},
	})
	record, err := workflowEngineStateRecord(
		runtimeflowidentity.RunScopedFlowInstance{RunID: uuid.NewString(), Route: testWorkflowInstanceRoute(instance.StorageRef)}, instance,
		"", 0, WorkflowEngineStateTransitionCreateStateAndCompanion, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(record.Fields) != `{"double":75.0,"integer":75,"nested":[75.0,75.0]}` {
		t.Fatalf("persisted workflow fields = %s", record.Fields)
	}
	fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", record.Fields)
	if err != nil {
		t.Fatal(err)
	}
	entityType := pipelineExpressionObjectType("numeric.persisted.writer", map[string]runtimecontracts.CatalogTypeKind{
		"integer": runtimecontracts.CatalogTypeInteger, "double": runtimecontracts.CatalogTypeNumber,
	})
	entityType.Fields = append(entityType.Fields, runtimecontracts.ResolvedCatalogField{Name: "nested", Type: runtimecontracts.ResolvedCatalogType{
		Kind: runtimecontracts.CatalogTypeList, Element: &runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeNumber},
	}})
	matched, err := newWorkflowExpressionEvaluator().EvalBoolWithOptions(
		"entity.integer + 1 == 76 && double(entity.double) + 1.0 == 76.0 && double(entity.nested[?0].value()) + 1.0 == 76.0 && double(entity.nested[?1].value()) + 1.0 == 76.0",
		workflowExpressionContext{Entity: fields}, workflowexpr.ValueExpressionOptions{EntityType: &entityType},
	)
	if err != nil || !matched {
		t.Fatalf("persisted workflow writer arithmetic = %v err=%v fields=%#v", matched, err, fields)
	}
}

func TestPersistedWorkflowStateRejectsUnsafeIntegerBeforeReadback(t *testing.T) {
	now := time.Date(2026, time.August, 31, 1, 2, 3, 0, time.UTC)
	_, err := DecodeWorkflowInstancePersistenceRecord(WorkflowInstancePersistenceRecord{
		EntityID: uuid.NewString(), WorkflowName: "review", WorkflowVersion: "1", Mode: "template", Status: "active",
		CurrentState: "active", Revision: 1, EnteredStageAt: now,
		Gates: []byte(`{}`), Fields: []byte(`{"score":9007199254740992}`), Bookkeeping: []byte(`{}`), Accumulator: []byte(`{}`),
		Config:       []byte(`{"workflow_version":"1","instance_id":"inst-1","flow_path":"review/inst-1"}`),
		FlowInstance: "review/inst-1", EntityType: "review_subject", CreatedAt: now, UpdatedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "$.score") || !strings.Contains(err.Error(), "declare the field as string") {
		t.Fatalf("unsafe persisted workflow-state error = %v, want typed $.score remediation", err)
	}
}

func VerifyWorkflowInstanceStoreProjection_RoundTripPreservesCanonicalStateForTest(t *testing.T, newFixture func(*testing.T, string) WorkflowProjectionNativeFixtureForTest) {
	fixture := newFixture(t, testPipelineRunID)
	ctx := fixture.Context
	storageRef := "review/inst-1"
	parentID := uuid.NewString()
	parentFlowID := "operating"
	parentFlowInstance := "operating/root"
	entityID := uuid.NewString()
	now := time.Now().UTC().Round(time.Microsecond)

	instance := materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:         "inst-1",
		StorageRef:         storageRef,
		EntityID:           entityID,
		EntityType:         "workflow_subject",
		Slug:               "projection",
		Name:               "Projection Flow",
		InstanceKind:       "materialized",
		TemplateVersion:    "v1",
		ParentFlowID:       parentFlowID,
		ParentFlowInstance: parentFlowInstance,
		ParentEntityID:     parentID,
		WorkflowName:       "review",
		WorkflowVersion:    "1.0.0",
		CurrentState:       "active",
		EnteredStageAt:     now,
		TransitionHistory:  []WorkflowTransitionRecord{lifecycleTransitionRecordFixtureForTest(t, "review", "queued", "active", "evt-1", now)},
		StateBuckets: map[string]any{
			"evidence": map[string]any{
				"audit": []any{
					map[string]any{"kind": "note", "score": float64(1)},
				},
			},
		},
		Fields:      map[string]any{"custom_threshold": float64(3), "business_brief": map[string]any{"title": "hello"}, "status": "open"},
		Bookkeeping: map[string]any{"last_source_event": "review.started"},
		Gates:       map[string]bool{"g_ready": true},
	})

	if err := fixture.Construct(ctx, instance); err != nil {
		t.Fatalf("upsert workflow instance: %v", err)
	}

	loaded, ok, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstance("review/inst-1"))
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
	if got := strings.TrimSpace(asString(loaded.Fields["custom_threshold"])); got != "3" {
		t.Fatalf("Config custom_threshold = %#v, want 3", loaded.Fields["custom_threshold"])
	}
	if got := strings.TrimSpace(asString(loaded.Fields["status"])); got != "open" {
		t.Fatalf("Fields status = %#v, want open", loaded.Fields["status"])
	}
	if got := strings.TrimSpace(loaded.StorageRef); got != "review/inst-1" {
		t.Fatalf("StorageRef = %q, want review/inst-1", got)
	}
	if got := strings.TrimSpace(loaded.ParentFlowID); got != parentFlowID {
		t.Fatalf("ParentFlowID = %q, want %q", got, parentFlowID)
	}
	if got := strings.Trim(strings.TrimSpace(loaded.ParentFlowInstance), "/"); got != parentFlowInstance {
		t.Fatalf("ParentFlowInstance = %q, want %q", got, parentFlowInstance)
	}
	if got := strings.TrimSpace(loaded.ParentEntityID); got != parentID {
		t.Fatalf("ParentEntityID = %q, want %q", got, parentID)
	}
	if _, exists := loaded.Fields["subject_id"]; exists {
		t.Fatalf("Fields subject_id = %#v, want absent", loaded.Fields["subject_id"])
	}
	if !loaded.Gates["g_ready"] {
		t.Fatalf("Gates = %#v, want g_ready=true", loaded.Gates)
	}
	if len(loaded.TransitionHistory) != 1 || loaded.TransitionHistory[0].TransitionID != instance.TransitionHistory[0].TransitionID {
		t.Fatalf("TransitionHistory = %#v, want admitted cause %s", loaded.TransitionHistory, instance.TransitionHistory[0].TransitionID)
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
	if identity.StorageRef != "review/inst-1" {
		t.Fatalf("identity.StorageRef = %q, want review/inst-1", identity.StorageRef)
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
	if identity.EntityID != entityID {
		t.Fatalf("identity.EntityID = %q, want explicit typed id %q", identity.EntityID, entityID)
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

func VerifyWorkflowInstanceStoreProjection_DoesNotExposeControlStatusAsEntityFieldForTest(t *testing.T, open func(*testing.T, string) WorkflowProjectionStorageNativeFixtureForTest) {
	fixture := open(t, testPipelineRunID)
	entityID := uuid.NewString()
	storageRef := "projection-flow"
	ctx := fixture.Context
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      storageRef,
		EntityID:        entityID,
		EntityType:      "workflow_subject",
		Slug:            "projection",
		Name:            "Projection Flow",
		WorkflowName:    "projection-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "reviewing",
		Status:          "active",
		EnteredStageAt:  time.Now().UTC().Round(time.Microsecond),
		Fields:          map[string]any{"status": "entity-open"},
		StateBuckets: map[string]any{
			"score": float64(9),
		},
	})

	if err := fixture.Construct(ctx, instance); err != nil {
		t.Fatalf("upsert workflow instance: %v", err)
	}

	loaded, ok, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, "projection-flow"))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := loaded.CurrentState; got != "reviewing" {
		t.Fatalf("CurrentState = %q, want reviewing", got)
	}
	if got := strings.TrimSpace(asString(loaded.Fields["status"])); got != "entity-open" {
		t.Fatalf("Fields status = %#v, want entity-open", loaded.Fields["status"])
	}

	observed, err := fixture.ReadControl(ctx, testPipelineRunID, workflowInstanceRowID(entityID))
	if err != nil {
		t.Fatalf("query entity_state projection: %v", err)
	}
	currentState, fieldsRaw, controlStatus := observed.CurrentState, observed.Fields, observed.ControlStatus
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
	if got := controlStatus; got != "active" {
		t.Fatalf("flow_instances.config runtime status = %q, want active", got)
	}
}

func VerifyWorkflowInstanceStoreCreateRejectsDuplicateWithoutMutatingProjectionForTest(t *testing.T, open func(*testing.T, string) WorkflowProjectionStorageNativeFixtureForTest) {
	fixture := open(t, testPipelineRunID)
	ctx := fixture.Context
	const storageRef = "review/inst-1"
	first := materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      storageRef,
		WorkflowName:    "review",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		EntityType:      "workflow_subject",
		Fields:          map[string]any{"business_brief": "first", "name": "alpha"},
		StateBuckets: map[string]any{
			"score": map[string]any{"value": float64(1)},
		},
	})
	if err := fixture.Construct(ctx, first); err != nil {
		t.Fatalf("create workflow instance: %v", err)
	}

	duplicate := first
	duplicate.CurrentState = "mutated"
	duplicate.Fields = map[string]any{"business_brief": "second", "name": "beta"}
	duplicate.StateBuckets = map[string]any{
		"score": map[string]any{"value": float64(99)},
	}
	err := fixture.Construct(ctx, duplicate)
	failure, ok := runtimefailures.As(err)
	if err == nil || !ok || failure.Failure.Class != runtimefailures.ClassConflictingDuplicate || failure.Failure.Detail.Code != "flow_instance_already_exists" || failure.Failure.Detail.Attributes["flow_instance"] != storageRef {
		t.Fatalf("duplicate create failure = %#v, want canonical already-exists failure", failure)
	}

	loaded, ok, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, storageRef))
	if err != nil {
		t.Fatalf("load workflow instance after duplicate create: %v", err)
	}
	if !ok {
		t.Fatal("expected original workflow instance to remain")
	}
	if got := loaded.CurrentState; got != "queued" {
		t.Fatalf("CurrentState = %q, want queued", got)
	}
	if got := loaded.Fields["business_brief"]; got != "first" {
		t.Fatalf("Fields business_brief = %#v, want first", got)
	}
	gotScore, ok := workflowStateBucketObject(loaded, "score")
	if !ok || gotScore["value"] != float64(1) {
		t.Fatalf("StateBuckets score = %#v ok=%v, want double 1", gotScore, ok)
	}

	observed, err := fixture.ReadDuplicate(ctx, testPipelineRunID, workflowInstanceRowID(storageRef))
	if err != nil {
		t.Fatalf("query persisted projection after duplicate create: %v", err)
	}
	revision, configName, fieldsRaw := observed.Revision, observed.FieldName, observed.Fields
	if revision != 1 {
		t.Fatalf("entity_state.revision = %d, want 1", revision)
	}
	if configName != "alpha" {
		t.Fatalf("entity_state.fields name = %q, want alpha", configName)
	}
	fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", fieldsRaw)
	if err != nil {
		t.Fatalf("decode entity_state.fields: %v", err)
	}
	if got := fields["business_brief"]; got != "first" {
		t.Fatalf("entity_state.fields business_brief = %#v, want first", got)
	}
}

func VerifyWorkflowInstanceStoreProjection_StaticRowsPersistCanonicalFlowPathOnRoundTripForTest(t *testing.T, newFixture func(*testing.T, string) WorkflowProjectionNativeFixtureForTest) {
	fixture := newFixture(t, testPipelineRunID)
	ctx := fixture.Context
	storageRef := uuid.NewString()
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      "static-flow",
		StorageRef:      "static-flow",
		EntityID:        storageRef,
		WorkflowName:    "static-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Fields:          map[string]any{},
		StateBuckets:    map[string]any{},
		EntityType:      "test_entity",
	})

	if err := fixture.Construct(ctx, instance); err != nil {
		t.Fatalf("upsert static workflow instance: %v", err)
	}

	loaded, ok, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstance("static-flow"))
	if err != nil {
		t.Fatalf("load static workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected static workflow instance to persist")
	}
	if got := strings.TrimSpace(loaded.StorageRef); got != "static-flow" {
		t.Fatalf("StorageRef = %q, want canonical static route", got)
	}
	identity, err := workflowInstancePersistedIdentity(nil, loaded)
	if err != nil {
		t.Fatalf("workflowInstancePersistedIdentity(static): %v", err)
	}
	if !identity.HasStoredPath {
		t.Fatalf("identity.HasStoredPath = false, want persisted canonical static route")
	}
	if identity.ScopeKey != "static-flow" {
		t.Fatalf("identity.ScopeKey = %q, want static-flow", identity.ScopeKey)
	}
	if identity.InstancePath != "static-flow" {
		t.Fatalf("identity.InstancePath = %q, want canonical static path", identity.InstancePath)
	}
	if identity.StorageRef != "static-flow" {
		t.Fatalf("identity.StorageRef = %q, want canonical static path", identity.StorageRef)
	}
}

func VerifyWorkflowInstanceStoreProjection_RejectsMalformedPersistedShapesForTest(t *testing.T, open func(*testing.T, string) WorkflowProjectionShapeNativeFixtureForTest) {
	cases := []struct {
		name         string
		mutate       func(WorkflowProjectionShapeNativeFixtureForTest, context.Context, string) (int64, error)
		mutateKey    string
		wantContains string
	}{
		{
			name: "fields not object",
			mutate: func(fixture WorkflowProjectionShapeNativeFixtureForTest, ctx context.Context, key string) (int64, error) {
				return fixture.FieldsArray(ctx, testPipelineRunID, key)
			},
			mutateKey:    "entity",
			wantContains: "entity_state.fields must be a JSON object",
		},
		{
			name: "gates not bool map",
			mutate: func(fixture WorkflowProjectionShapeNativeFixtureForTest, ctx context.Context, key string) (int64, error) {
				return fixture.NumericGate(ctx, testPipelineRunID, key)
			},
			mutateKey:    "storage",
			wantContains: "entity_state.gates must be an object of booleans",
		},
		{
			name: "accumulator not object",
			mutate: func(fixture WorkflowProjectionShapeNativeFixtureForTest, ctx context.Context, key string) (int64, error) {
				return fixture.AccumulatorArray(ctx, testPipelineRunID, key)
			},
			mutateKey:    "storage",
			wantContains: "entity_state.accumulator must be a JSON object",
		},
		{
			name: "control metadata malformed",
			mutate: func(fixture WorkflowProjectionShapeNativeFixtureForTest, ctx context.Context, key string) (int64, error) {
				return fixture.MalformedTransitionHistory(ctx, testPipelineRunID, key)
			},
			mutateKey:    "storage",
			wantContains: "flow_instances.config transition_history must be an array of workflow transition records",
		},
		{
			name: "instance id disagrees with flow path",
			mutate: func(fixture WorkflowProjectionShapeNativeFixtureForTest, ctx context.Context, key string) (int64, error) {
				return fixture.ConflictingInstanceID(ctx, testPipelineRunID, key)
			},
			mutateKey:    "storage",
			wantContains: "instance_id",
		},
		{
			name: "slash-only flow path fails closed",
			mutate: func(fixture WorkflowProjectionShapeNativeFixtureForTest, ctx context.Context, key string) (int64, error) {
				return fixture.SlashOnlyFlowPath(ctx, testPipelineRunID, key)
			},
			mutateKey:    "storage",
			wantContains: "flow_path",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := open(t, testPipelineRunID)
			ctx := fixture.Context
			storageRef := "storage-ref"
			entityID := uuid.NewString()
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: "inst-1", StorageRef: storageRef, EntityID: entityID,
				WorkflowName: "projection-flow", WorkflowVersion: "1.0.0", CurrentState: "queued",
				Fields: map[string]any{}, StateBuckets: map[string]any{}, EntityType: "test_entity",
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			mutateID := storageRef
			if tc.mutateKey == "entity" {
				mutateID = entityID
			}
			changed, err := tc.mutate(fixture, ctx, mutateID)
			if err != nil {
				t.Fatalf("mutate malformed persisted shape: %v", err)
			}
			if changed != 1 {
				t.Fatalf("mutate exact persisted authority: rows=%d err=%v", changed, err)
			}
			_, _, err = fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstance(storageRef))
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
