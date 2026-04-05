package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	models "swarm/internal/runtime/core/actors"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimemutationlog "swarm/internal/runtime/mutationlog"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

func (e *Executor) execSaveEntityField(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	entityID, err := parseEntityID(payload["entity_id"])
	if err != nil {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.entity_id", false, err.Error())
	}
	if err := enforceEntityWriteOwnership(ctx, db, source, actor, entityID, e.runtimeLogSink()); err != nil {
		return nil, err
	}
	fieldName := strings.TrimSpace(asString(payload["field"]))
	if fieldName == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", false, "field is required")
	}
	field, err := schema.field(fieldName)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", false, err, "validate field")
	}
	value, err := normalizeEntityFieldValue(field, payload["value"])
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.value", false, err, "validate value")
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

	var oldValue []byte
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(fields -> $2, 'null'::jsonb)
		FROM entity_state
		WHERE entity_id = $1::uuid
		FOR UPDATE
	`, entityID, fieldName).Scan(&oldValue); err != nil {
		if err == sql.ErrNoRows {
			return nil, NewRuntimeError("not_found", "tool-executor", "exec_save_entity_field.lookup", false, "entity %s not found", entityID)
		}
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_save_entity_field.lookup", true, err, "load current entity field %s", fieldName)
	}

	var revision int
	if err := tx.QueryRowContext(ctx, `
		UPDATE entity_state
		SET
			fields = jsonb_set(COALESCE(fields, '{}'::jsonb), ARRAY[$2], $3::jsonb, true),
			revision = revision + 1,
			updated_at = now()
		WHERE entity_id = $1::uuid
		RETURNING revision
	`, entityID, fieldName, string(valueJSON)).Scan(&revision); err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_save_entity_field.update", true, err, "save entity field %s", fieldName)
	}
	if err := runtimemutationlog.InsertEntityStateDiff(ctx, tx, entityID, runtimemutationlog.EntityStateProjection{
		Fields: map[string]any{
			fieldName: nullableJSONBytes(oldValue),
		},
	}, runtimemutationlog.EntityStateProjection{
		Fields: map[string]any{
			fieldName: json.RawMessage(valueJSON),
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
		"field":     fieldName,
		"revision":  revision,
	}, nil
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
	var flowInstance sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(flow_instance, '') FROM entity_state WHERE entity_id = $1::uuid`, entityID).Scan(&flowInstance); err != nil {
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
		logger.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
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

func (e *Executor) execCreateEntity(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID := strings.TrimSpace(asString(payload["entity_id"]))
	if entityID == "" {
		entityID = uuid.NewString()
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.entity_id", false, "entity_id must be uuid")
	}
	flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/")
	if flowInstance == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.flow_instance", false, "flow_instance is required")
	}
	entityType := strings.TrimSpace(asString(payload["entity_type"]))
	if entityType == "" {
		entityType = "default"
	}
	subjectID := strings.TrimSpace(asString(payload["subject_id"]))
	if subjectID == "" {
		subjectID = entityID
	}
	if _, err := uuid.Parse(subjectID); err != nil {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.subject_id", false, "subject_id must be uuid")
	}
	name := strings.TrimSpace(asString(payload["name"]))
	currentState := strings.TrimSpace(asString(payload["initial_state"]))
	if currentState == "" {
		e.mu.RLock()
		source := e.workflowSource
		e.mu.RUnlock()
		if source != nil {
			currentState = strings.TrimSpace(source.WorkflowInitialStage())
		}
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
	normalizedFields := map[string]any{}
	for _, fieldName := range orderedEntityFieldNamesFromInput(mapKeys(fieldsPayload)) {
		field, err := schema.field(fieldName)
		if err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.field", false, err, "validate field")
		}
		value, err := normalizeEntityFieldValue(field, fieldsPayload[fieldName])
		if err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.value", false, err, "validate field %s", fieldName)
		}
		normalizedFields[fieldName] = value
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
			entity_id, subject_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, NULLIF($5, ''),
			$6, '{}'::jsonb, $7::jsonb, '{}'::jsonb, 1,
			$8, $8, $8
		)
	`, entityID, subjectID, flowInstance, entityType, name, currentState, string(fieldsJSON), now); err != nil {
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
		"subject_id":    subjectID,
		"current_state": currentState,
		"created_at":    now.Format(time.RFC3339Nano),
	}, nil
}
