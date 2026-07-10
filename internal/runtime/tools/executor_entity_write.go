package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func (e *Executor) execSaveEntityField(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	store, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID, err := parseEntityID(payload["entity_id"])
	if err != nil {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.entity_id", nil)
	}
	identity, err := runtimecurrentstate.RequireIdentity(ctx, entityID)
	if err != nil {
		return nil, failures.WrapDetail("write_failed", "tool-executor", "exec_save_entity_field.run_context", nil, err)
	}
	if err := enforceEntityWriteOwnership(ctx, store, source, actor, entityID, e.runtimeLogSink()); err != nil {
		return nil, err
	}
	row, found, err := loadEntityState(ctx, store, entityID)
	if err != nil {
		return nil, failures.WrapDetail("query_failed", "tool-executor", "exec_save_entity_field.lookup", map[string]any{"entity_id": entityID}, err)
	}
	if !found {
		return nil, failures.NewDetail("not_found", "tool-executor", "exec_save_entity_field.lookup", map[string]any{"entity_id": entityID})
	}
	if flowInstance := normalizeEntityToolFlowInstance(asString(payload["flow_instance"])); flowInstance != "" {
		if !entityToolExistingFlowInstanceMatches(source, flowInstance, asString(row["flow_instance"])) {
			return nil, failures.New(failures.ClassAuthorizationDenied, "entity_flow_ownership_mismatch", "tool-executor", "exec_save_entity_field.flow_instance", map[string]any{"action": "entity_write", "entity_id": entityID, "requested_flow_path": flowInstance, "owner_flow_path": asString(row["flow_instance"])})
		}
	}
	schema, err := entityToolSchemaForEntityRow(source, row)
	if err != nil {
		return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.schema", nil, err)
	}
	fieldName := strings.TrimSpace(asString(payload["field"]))
	if fieldName == "" {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", map[string]any{"field": "field"})
	}
	field, err := schema.declaredField(fieldName)
	if err != nil {
		return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", map[string]any{"field": fieldName}, err)
	}
	if strings.TrimSpace(field.FieldDecl.MaterializeFrom) != "" {
		return nil, failures.New(failures.ClassAuthorizationDenied, "runtime_materialized_field_write_forbidden", "tool-executor", "exec_save_entity_field.field", map[string]any{"action": "entity_write", "field": fieldName})
	}
	currentFields := entityRowFieldMap(row)
	value, err := normalizeEntityFieldValue(schema, field, payload["value"])
	if err != nil {
		return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.value", map[string]any{"field": fieldName}, err)
	}
	if field.FieldDecl.Immutable {
		materializedCurrent, currentErr := entityruntime.Materialize(schema.Contract, entityruntime.DeclaredValues(schema.Contract, currentFields))
		if currentErr != nil {
			return nil, failures.Wrap(failures.ClassInternalFailure, "immutable_field_current_value_unavailable", "tool-executor", "exec_save_entity_field.field", map[string]any{"field": fieldName}, currentErr)
		}
		if currentValue, exists := entityruntime.PathValue(materializedCurrent, field.Path); exists && !valuesEqual(currentValue, value) {
			return nil, failures.New(failures.ClassAuthorizationDenied, "immutable_field_write_forbidden", "tool-executor", "exec_save_entity_field.field", map[string]any{"action": "entity_write", "field": fieldName})
		}
	}
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.value", map[string]any{"field": fieldName}, err)
	}
	pathSegments, err := entityJSONPathSegments(field.Path)
	if err != nil {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", map[string]any{"field": fieldName})
	}

	revision, err := store.SaveEntityField(ctx, EntityFieldUpdate{
		RunID:        identity.RunID,
		EntityID:     identity.EntityID,
		FieldPath:    field.Path,
		PathSegments: pathSegments,
		ValueJSON:    json.RawMessage(valueJSON),
		Writer: EntityMutationWriter{
			Type:        "agent",
			ID:          strings.TrimSpace(actor.ID),
			HandlerStep: "save_entity_field",
		},
	})
	if err != nil {
		return nil, failures.WrapDetail("write_failed", "tool-executor", "exec_save_entity_field.update", map[string]any{"entity_id": entityID, "field": fieldName}, err)
	}
	return map[string]any{
		"entity_id": entityID,
		"field":     field.Path,
		"revision":  revision,
	}, nil
}

func entityJSONPathSegments(path string) ([]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("field is required")
	}
	if strings.Contains(path, "[") || strings.Contains(path, "]") {
		return nil, fmt.Errorf("list index writes are not supported for path %s", path)
	}
	rawSegments := strings.Split(path, ".")
	segments := make([]string, 0, len(rawSegments))
	for _, segment := range rawSegments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return nil, fmt.Errorf("field is required")
		}
		segments = append(segments, segment)
	}
	return segments, nil
}

func enforceEntityWriteOwnership(ctx context.Context, store EntityPersistence, source semanticview.Source, actor models.AgentConfig, entityID string, logger runtimeToolLogSink) error {
	flowRoot := actorFlowOwnershipRoot(source, actor.ID)
	if flowRoot == "" || store == nil {
		return nil
	}
	row, found, err := loadEntityState(ctx, store, entityID)
	if err != nil {
		return failures.WrapDetail("query_failed", "tool-executor", "entity_write_ownership.lookup", map[string]any{"entity_id": entityID}, err)
	}
	if !found {
		return nil
	}
	targetFlow := strings.Trim(asString(row["flow_instance"]), "/")
	if targetFlow == "" || entityFlowOwnedBy(flowRoot, targetFlow) {
		return nil
	}
	denial := failures.NewDetail(
		"cross_flow_write_forbidden",
		"tool-executor",
		"entity_write_ownership.enforce",
		map[string]any{
			"action":          "entity_write",
			"actor_id":        strings.TrimSpace(actor.ID),
			"entity_id":       entityID,
			"owner_flow_path": targetFlow,
		},
	)
	if logger != nil {
		failure, _ := failures.EnvelopeFromError(denial)
		logger.LogRuntime(toolExecutorRuntimeLogContext(ctx), runtimepipeline.RuntimeLogEntry{
			Level:     "warn",
			Message:   "Entity write was denied because the target entity belongs to a different flow",
			Component: "tool-executor",
			Action:    "entity_write_denied",
			AgentID:   strings.TrimSpace(actor.ID),
			EntityID:  strings.TrimSpace(entityID),
			Detail: map[string]any{
				"denial_layer":       "executor",
				"denial_reason":      "cross_flow_write_forbidden",
				"tool_name":          "save_entity_field",
				"actor_id":           strings.TrimSpace(actor.ID),
				"actor_role":         strings.TrimSpace(actor.Role),
				"turn_flow":          strings.TrimSpace(flowRoot),
				"entity_owner_flow":  targetFlow,
				"write_target_id":    strings.TrimSpace(entityID),
				"ownership_relation": "foreign",
			},
			Failure: failures.CloneEnvelope(&failure),
		})
	}
	return denial
}

func actorFlowOwnershipRoot(source semanticview.Source, actorID string) string {
	actorID = strings.TrimSpace(actorID)
	if source == nil || actorID == "" {
		return ""
	}
	contractSource, ok := source.AgentContractSource(actorID)
	if !ok {
		return ""
	}
	flowID := strings.TrimSpace(contractSource.FlowID)
	if flowID == "" {
		return ""
	}
	return runtimeflowidentity.ScopeKey(source, flowID)
}

func entityFlowOwnedBy(flowRoot, targetFlow string) bool {
	return runtimeflowidentity.OwnedByScope(flowRoot, targetFlow)
}

func (e *Executor) execCreateEntity(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	store, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, failures.WrapDetail("write_failed", "tool-executor", "exec_create_entity.run_context", nil, err)
	}
	if strings.TrimSpace(asString(payload["entity_id"])) != "" {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_create_entity.entity_id", map[string]any{"field": "entity_id"})
	}
	entityID := uuid.NewString()
	flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/")
	if flowInstance == "" {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_create_entity.flow_instance", map[string]any{"field": "flow_instance"})
	}
	contract, ok := entityruntime.ResolveForActor(source, actor.ID)
	if !ok {
		contract, ok = entityruntime.ResolveForFlowInstance(source, flowInstance)
	}
	if !ok {
		return nil, failures.NewDetail("not_found", "tool-executor", "exec_create_entity.flow_instance", map[string]any{"flow_path": flowInstance})
	}
	flowID := entityruntime.ResolveFlowIDForInstance(source, flowInstance)
	if contract.FlowID != "" && flowID != "" && contract.FlowID != flowID {
		return nil, failures.New(failures.ClassAuthorizationDenied, "flow_scope_create_forbidden", "tool-executor", "exec_create_entity.flow_instance", map[string]any{"action": "entity_create", "flow_path": flowInstance, "actor_id": strings.TrimSpace(actor.ID)})
	}
	if strings.TrimSpace(asString(payload["entity_type"])) != "" {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_create_entity.entity_type", map[string]any{"field": "entity_type"})
	}
	if strings.TrimSpace(asString(payload["subject_id"])) != "" {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_create_entity.subject_id", map[string]any{"field": "subject_id"})
	}
	name := strings.TrimSpace(asString(payload["name"]))
	currentState := strings.TrimSpace(asString(payload["initial_state"]))
	if currentState == "" && source != nil {
		currentState = strings.TrimSpace(source.FlowInitialStage(flowID))
	}
	if currentState == "" {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_create_entity.initial_state", map[string]any{"field": "initial_state"})
	}

	fieldsPayload := map[string]any{}
	if raw, ok := payload["fields"]; ok && raw != nil {
		if decoded, ok := raw.(map[string]any); ok {
			fieldsPayload = decoded
		} else if err := decodeToolInput(raw, &fieldsPayload); err != nil {
			return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_create_entity.fields", nil, err)
		}
	}
	normalizedFields, err := entityruntime.Materialize(contract, fieldsPayload)
	if err != nil {
		return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_create_entity.fields", nil, err)
	}
	fieldsJSON, err := json.Marshal(normalizedFields)
	if err != nil {
		return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_create_entity.fields", nil, err)
	}
	now := time.Now().UTC()
	if err := store.CreateEntity(ctx, EntityCreateRecord{
		RunID:        runID,
		EntityID:     entityID,
		FlowInstance: flowInstance,
		EntityType:   contract.EntityType,
		Name:         name,
		CurrentState: currentState,
		FieldsJSON:   json.RawMessage(fieldsJSON),
		CreatedAt:    now,
		Writer: EntityMutationWriter{
			Type:        "platform",
			ID:          "create_entity",
			HandlerStep: "create_entity",
		},
	}); err != nil {
		return nil, failures.WrapDetail("write_failed", "tool-executor", "exec_create_entity.insert", map[string]any{"entity_id": entityID}, err)
	}
	return map[string]any{
		"entity_id":     entityID,
		"current_state": currentState,
		"created_at":    now.Format(time.RFC3339Nano),
	}, nil
}

func valuesEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	if leftErr != nil || rightErr != nil {
		return left == right
	}
	return string(leftJSON) == string(rightJSON)
}
