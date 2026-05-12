package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	models "swarm/internal/runtime/core/actors"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "swarm/internal/runtime/currentstate"
	"swarm/internal/runtime/entityruntime"
	runtimemutationlog "swarm/internal/runtime/mutationlog"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

func (e *Executor) execSaveEntityField(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	db, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID, err := parseEntityID(payload["entity_id"])
	if err != nil {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.entity_id", false, err.Error())
	}
	identity, err := runtimecurrentstate.RequireIdentity(ctx, entityID)
	if err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_save_entity_field.run_context", true, err, "resolve entity_state run context")
	}
	if err := enforceEntityWriteOwnership(ctx, db, source, actor, entityID, e.runtimeLogSink()); err != nil {
		return nil, err
	}
	row, found, err := loadEntityState(ctx, db, entityID)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_save_entity_field.lookup", true, err, "load entity %s", entityID)
	}
	if !found {
		return nil, NewRuntimeError("not_found", "tool-executor", "exec_save_entity_field.lookup", false, "entity %s not found", entityID)
	}
	if flowInstance := normalizeEntityToolFlowInstance(asString(payload["flow_instance"])); flowInstance != "" {
		if !entityToolExistingFlowInstanceMatches(source, flowInstance, asString(row["flow_instance"])) {
			return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.flow_instance", false, "flow_instance does not match entity ownership")
		}
	}
	schema, err := entityToolSchemaForEntityRow(source, row)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.schema", false, err, "resolve entity contract")
	}
	fieldName := strings.TrimSpace(asString(payload["field"]))
	if fieldName == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", false, "field is required")
	}
	field, err := schema.declaredField(fieldName)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", false, err, "validate field")
	}
	if strings.TrimSpace(field.FieldDecl.MaterializeFrom) != "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", false, "field %s is materialized by runtime accumulator projection and is not agent-writable", fieldName)
	}
	currentFields := entityRowFieldMap(row)
	value, err := normalizeEntityFieldValue(schema, field, payload["value"])
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.value", false, err, "validate value")
	}
	if field.FieldDecl.Immutable {
		materializedCurrent, currentErr := entityruntime.Materialize(schema.Contract, entityruntime.DeclaredValues(schema.Contract, currentFields))
		if currentErr != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", false, currentErr, "resolve immutable current value")
		}
		if currentValue, exists := entityruntime.PathValue(materializedCurrent, field.Path); exists && !valuesEqual(currentValue, value) {
			return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", false, "immutable field %s cannot be changed after create", fieldName)
		}
	}
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.value", false, err, "marshal value")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_save_entity_field.begin", true, err, "begin save entity field tx")
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	pathSegments, err := entityJSONPathSegments(field.Path)
	if err != nil {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", false, err.Error())
	}
	pathArray := pq.Array(pathSegments)

	var oldValue []byte
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(COALESCE(fields, '{}'::jsonb) #> $3::text[], 'null'::jsonb)
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
		FOR UPDATE
	`, identity.RunID, identity.EntityID, pathArray).Scan(&oldValue); err != nil {
		if err == sql.ErrNoRows {
			return nil, NewRuntimeError("not_found", "tool-executor", "exec_save_entity_field.lookup", false, "entity %s not found", entityID)
		}
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_save_entity_field.lookup", true, err, "load current entity field %s", fieldName)
	}

	var revision int
	if err := tx.QueryRowContext(ctx, `
		UPDATE entity_state
		SET
			fields = jsonb_set(COALESCE(fields, '{}'::jsonb), $2::text[], $3::jsonb, true),
			revision = revision + 1,
			updated_at = now()
		WHERE entity_id = $1::uuid
		  AND run_id = $4::uuid
		RETURNING revision
	`, identity.EntityID, pathArray, string(valueJSON), identity.RunID).Scan(&revision); err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_save_entity_field.update", true, err, "save entity field %s", fieldName)
	}
	if err := runtimemutationlog.InsertEntityStateDiff(ctx, tx, entityID, runtimemutationlog.EntityStateProjection{
		Fields: map[string]any{
			field.Path: nullableJSONBytes(oldValue),
		},
	}, runtimemutationlog.EntityStateProjection{
		Fields: map[string]any{
			field.Path: json.RawMessage(valueJSON),
		},
	}, runtimemutationlog.Writer{
		Type:        "agent",
		ID:          strings.TrimSpace(actor.ID),
		HandlerStep: "save_entity_field",
	}); err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_save_entity_field.mutation_log", true, err, "record entity mutation %s", fieldName)
	}
	if err := tx.Commit(); err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_save_entity_field.commit", true, err, "commit save entity field")
	}
	committed = true
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

func nullableJSONBytes(raw []byte) any {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil
	}
	return json.RawMessage(append([]byte(nil), raw...))
}

func enforceEntityWriteOwnership(ctx context.Context, db *sql.DB, source semanticview.Source, actor models.AgentConfig, entityID string, logger runtimeToolLogSink) error {
	flowRoot := actorFlowOwnershipRoot(source, actor.ID)
	if flowRoot == "" || db == nil {
		return nil
	}
	identity, err := runtimecurrentstate.RequireIdentity(ctx, entityID)
	if err != nil {
		return WrapRuntimeError("query_failed", "tool-executor", "entity_write_ownership.run_context", true, err, "resolve entity_state run context")
	}
	var flowInstance sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(flow_instance, '') FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, identity.RunID, identity.EntityID).Scan(&flowInstance); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return WrapRuntimeError("query_failed", "tool-executor", "entity_write_ownership.lookup", true, err, "load entity ownership")
	}
	targetFlow := strings.Trim(flowInstance.String, "/")
	if targetFlow == "" || entityFlowOwnedBy(flowRoot, targetFlow) {
		return nil
	}
	if logger != nil {
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
		})
	}
	return NewRuntimeError(
		"cross_flow_write_forbidden",
		"tool-executor",
		"entity_write_ownership.enforce",
		false,
		"actor %s cannot write entity %s owned by flow_instance %s",
		strings.TrimSpace(actor.ID),
		entityID,
		targetFlow,
	)
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
	db, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_create_entity.run_context", true, err, "resolve entity_state run context")
	}
	if strings.TrimSpace(asString(payload["entity_id"])) != "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.entity_id", false, "entity_id is platform-minted and must not be supplied")
	}
	entityID := uuid.NewString()
	flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/")
	if flowInstance == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.flow_instance", false, "flow_instance is required")
	}
	contract, ok := entityruntime.ResolveForActor(source, actor.ID)
	if !ok {
		contract, ok = entityruntime.ResolveForFlowInstance(source, flowInstance)
	}
	if !ok {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.flow_instance", false, "flow_instance does not resolve to a flow-owned entity contract")
	}
	flowID := entityruntime.ResolveFlowIDForInstance(source, flowInstance)
	if contract.FlowID != "" && flowID != "" && contract.FlowID != flowID {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.flow_instance", false, "flow_instance is outside the caller's flow scope")
	}
	if strings.TrimSpace(asString(payload["entity_type"])) != "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.entity_type", false, "entity_type is inferred from flow_instance and must not be supplied")
	}
	if strings.TrimSpace(asString(payload["subject_id"])) != "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.subject_id", false, "subject_id is deprecated; use explicit payload/config fields for business correlation")
	}
	name := strings.TrimSpace(asString(payload["name"]))
	currentState := strings.TrimSpace(asString(payload["initial_state"]))
	if currentState == "" && source != nil {
		currentState = strings.TrimSpace(source.FlowInitialStage(flowID))
	}
	if currentState == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.initial_state", false, "initial_state is required when workflow has no initial stage")
	}

	fieldsPayload := map[string]any{}
	if raw, ok := payload["fields"]; ok && raw != nil {
		if decoded, ok := raw.(map[string]any); ok {
			fieldsPayload = decoded
		} else if err := decodeToolInput(raw, &fieldsPayload); err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.fields", false, err, "decode fields")
		}
	}
	normalizedFields, err := entityruntime.Materialize(contract, fieldsPayload)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.fields", false, err, "materialize fields")
	}
	fieldsJSON, err := json.Marshal(normalizedFields)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.fields", false, err, "marshal fields")
	}
	now := time.Now().UTC()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_create_entity.begin", true, err, "begin create entity tx")
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, NULLIF($5, ''),
			$6, '{}'::jsonb, $7::jsonb, '{}'::jsonb, 1,
			$8, $8, $8
		)
	`, runID, entityID, flowInstance, contract.EntityType, name, currentState, string(fieldsJSON), now); err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_create_entity.insert", false, err, "create entity %s", entityID)
	}
	if err := runtimemutationlog.InsertEntityStateDiff(ctx, tx, entityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
		CurrentState: currentState,
		Fields:       normalizedFields,
	}, runtimemutationlog.Writer{
		Type:        "platform",
		ID:          "create_entity",
		HandlerStep: "create_entity",
	}); err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_create_entity.mutation_log", true, err, "record initial entity mutations")
	}
	if err := tx.Commit(); err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_create_entity.commit", true, err, "commit create entity")
	}
	committed = true
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
