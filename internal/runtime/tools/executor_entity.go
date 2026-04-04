package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/uuid"
	models "swarm/internal/runtime/core/actors"
	runtimemutationlog "swarm/internal/runtime/mutationlog"
	"swarm/internal/runtime/semanticview"
)

var entityStateTopLevelFields = map[string]struct{}{
	"entity_id":        {},
	"subject_id":       {},
	"flow_instance":    {},
	"entity_type":      {},
	"name":             {},
	"current_state":    {},
	"gates":            {},
	"fields":           {},
	"accumulator":      {},
	"revision":         {},
	"entered_state_at": {},
	"created_at":       {},
	"updated_at":       {},
}

func (e *Executor) execGetEntity(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, _, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID, err := parseEntityID(payload["entity_id"])
	if err != nil {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_get_entity.entity_id", false, err.Error())
	}
	entity, found, err := loadEntityState(ctx, db, entityID)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_get_entity.lookup", true, err, "load entity %s", entityID)
	}
	if !found {
		return nil, NewRuntimeError("not_found", "tool-executor", "exec_get_entity.lookup", false, "entity %s not found", entityID)
	}
	return entity, nil
}

func (e *Executor) execGetSubjectStatus(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, _, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	subjectID, err := parseEntityID(payload["subject_id"])
	if err != nil {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_get_subject_status.subject_id", false, err.Error())
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	rows, err := queryEntityStateRows(ctx, db, " WHERE subject_id = $1::uuid", subjectID)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_get_subject_status.query", true, err, "query subject status")
	}
	subjectStatusSortRows(source, rows)
	entities := make([]map[string]any, 0, len(rows))
	allTerminal := true
	latestFlow := ""
	latestState := ""
	for idx, row := range rows {
		flowInstance := strings.Trim(strings.TrimSpace(asString(row["flow_instance"])), "/")
		flowID := subjectStatusFlowID(source, flowInstance)
		state := strings.TrimSpace(asString(row["current_state"]))
		terminal := subjectStatusTerminal(source, flowID, state)
		if !terminal {
			allTerminal = false
		}
		if idx == 0 {
			latestFlow = flowID
			latestState = state
		}
		entities = append(entities, map[string]any{
			"entity_id": strings.TrimSpace(asString(row["entity_id"])),
			"flow":      flowID,
			"state":     state,
			"terminal":  terminal,
		})
	}
	return map[string]any{
		"subject_id":   subjectID,
		"entities":     entities,
		"all_terminal": allTerminal,
		"latest_flow":  latestFlow,
		"latest_state": latestState,
	}, nil
}

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
	if err := enforceEntityWriteOwnership(ctx, db, source, actor, entityID); err != nil {
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
	if err := runtimemutationlog.Insert(ctx, tx, runtimemutationlog.Record{
		EntityID:    entityID,
		Field:       fieldName,
		OldValue:    nullableJSONBytes(oldValue),
		NewValue:    json.RawMessage(valueJSON),
		WriterType:  "agent",
		WriterID:    strings.TrimSpace(actor.ID),
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

func enforceEntityWriteOwnership(ctx context.Context, db *sql.DB, source semanticview.Source, actor models.AgentConfig, entityID string) error {
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
	if flowPath := strings.Trim(source.FlowPath(flowID), "/"); flowPath != "" {
		return flowPath
	}
	return flowID
}

func entityFlowOwnedBy(flowRoot, targetFlow string) bool {
	flowRoot = strings.Trim(flowRoot, "/")
	targetFlow = strings.Trim(targetFlow, "/")
	if flowRoot == "" || targetFlow == "" {
		return true
	}
	return targetFlow == flowRoot || strings.HasPrefix(targetFlow, flowRoot+"/")
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
	if _, err := db.ExecContext(ctx, `
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
	return map[string]any{
		"entity_id":     entityID,
		"subject_id":    subjectID,
		"current_state": currentState,
		"created_at":    now.Format(time.RFC3339Nano),
	}, nil
}

func (e *Executor) execSearchEntities(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	filterSQL := make([]string, 0, 4)
	args := make([]any, 0, 4)
	currentStateFilter := strings.TrimSpace(asString(payload["current_state"]))
	if subjectID := strings.TrimSpace(asString(payload["subject_id"])); subjectID != "" {
		args = append(args, subjectID)
		filterSQL = append(filterSQL, fmt.Sprintf("subject_id = $%d::uuid", len(args)))
	}
	if flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/"); flowInstance != "" {
		args = append(args, flowInstance)
		filterSQL = append(filterSQL, fmt.Sprintf("flow_instance = $%d", len(args)))
	}
	if entityType := strings.TrimSpace(asString(payload["entity_type"])); entityType != "" {
		args = append(args, entityType)
		filterSQL = append(filterSQL, fmt.Sprintf("entity_type = $%d", len(args)))
	}
	if rawFilter, ok := payload["filter"]; ok && rawFilter != nil {
		filterObject := map[string]any{}
		if decoded, ok := rawFilter.(map[string]any); ok {
			filterObject = decoded
		} else if err := decodeToolInput(rawFilter, &filterObject); err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.filter", false, err, "decode filter")
		}
		normalizedFilter := map[string]any{}
		for _, fieldName := range orderedEntityFieldNamesFromInput(mapKeys(filterObject)) {
			field, err := schema.field(fieldName)
			if err != nil {
				return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.filter", false, err, "validate filter field")
			}
			value, err := normalizeEntityFieldValue(field, filterObject[fieldName])
			if err != nil {
				return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.filter", false, err, "validate filter field %s", fieldName)
			}
			normalizedFilter[fieldName] = value
		}
		if len(normalizedFilter) > 0 {
			filterJSON, err := json.Marshal(normalizedFilter)
			if err != nil {
				return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.filter", false, err, "marshal filter")
			}
			args = append(args, string(filterJSON))
			filterSQL = append(filterSQL, fmt.Sprintf("COALESCE(fields, '{}'::jsonb) @> $%d::jsonb", len(args)))
		}
	}
	whereClause := joinEntityStateWhere(filterSQL)
	limit := defaultEntitySearchLimit(asInt(payload["limit"]))
	offset := asInt(payload["offset"])
	if offset < 0 {
		offset = 0
	}
	rows, err := queryEntityStateRows(ctx, db, whereClause+" ORDER BY created_at DESC", args...)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_search_entities.query", true, err, "search entity_state")
	}
	if currentStateFilter != "" {
		filtered := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			if currentStateFilter != "" && strings.TrimSpace(asString(row["current_state"])) != currentStateFilter {
				continue
			}
			filtered = append(filtered, row)
		}
		rows = filtered
	}
	total := len(rows)
	if offset >= len(rows) {
		rows = []map[string]any{}
	} else {
		end := offset + limit
		if end > len(rows) {
			end = len(rows)
		}
		rows = rows[offset:end]
	}
	return map[string]any{
		"results": rows,
		"total":   total,
	}, nil
}

func (e *Executor) execQueryEntities(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	whereClause, args := entityStateBaseQuery(payload, false)
	rows, err := queryEntityStateRows(ctx, db, whereClause+" ORDER BY created_at DESC", args...)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_query_entities.query", true, err, "query entity_state")
	}
	filtered, err := filterEntityStateRowsCEL(strings.TrimSpace(asString(payload["filter"])), rows, schema)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_entities.filter", false, err, "evaluate CEL filter")
	}
	selectFields, err := decodeEntitySelect(payload["select"], schema)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_entities.select", false, err, "decode select")
	}
	limit := defaultEntitySearchLimit(asInt(payload["limit"]))
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}
	groupBy := strings.TrimSpace(asString(payload["group_by"]))
	if groupBy != "" {
		if err := validateEntitySelector(schema, groupBy); err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_entities.group_by", false, err, "validate group_by")
		}
		grouped := groupEntityStateRows(filtered, groupBy, selectFields)
		return map[string]any{"results": grouped}, nil
	}
	projected := projectEntityStateRows(filtered, selectFields)
	return map[string]any{"results": projected}, nil
}

func (e *Executor) execQueryMetrics(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	metric := strings.ToLower(strings.TrimSpace(asString(payload["metric"])))
	if metric == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.metric", false, "metric is required")
	}
	fieldName := strings.TrimSpace(asString(payload["field"]))
	if metric != "count" && fieldName == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.field", false, "field is required for metric %s", metric)
	}
	if fieldName != "" {
		if err := validateEntitySelector(schema, fieldName); err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.field", false, err, "validate metric field")
		}
	}
	groupBy := strings.TrimSpace(asString(payload["group_by"]))
	if groupBy != "" {
		if err := validateEntitySelector(schema, groupBy); err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.group_by", false, err, "validate group_by")
		}
	}
	whereClause, args := entityStateBaseQuery(payload, true)
	rows, err := queryEntityStateRows(ctx, db, whereClause+" ORDER BY created_at DESC", args...)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_query_metrics.query", true, err, "query entity_state metrics")
	}
	filtered, err := filterEntityStateRowsCEL(strings.TrimSpace(asString(payload["filter"])), rows, schema)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.filter", false, err, "evaluate CEL filter")
	}
	if groupBy == "" {
		value, err := aggregateEntityMetric(metric, fieldName, filtered)
		if err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.aggregate", false, err, "aggregate metric")
		}
		return map[string]any{"value": value}, nil
	}
	groups, err := aggregateEntityMetricGroups(metric, fieldName, groupBy, filtered)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.aggregate", false, err, "aggregate grouped metric")
	}
	return map[string]any{"groups": groups}, nil
}

func (e *Executor) entityToolDependencies(input any) (*sql.DB, entityToolSchema, map[string]any, error) {
	e.mu.RLock()
	db := e.sqlDB
	source := e.workflowSource
	e.mu.RUnlock()
	if db == nil {
		return nil, entityToolSchema{}, nil, NewRuntimeError("dependency_unavailable", "tool-executor", "entity_tool.db", true, "sql database is not configured")
	}
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return nil, entityToolSchema{}, nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "entity_tool.decode", false, err, "decode entity tool input")
	}
	if payload == nil {
		payload = map[string]any{}
	}
	schema, err := entityToolSchemaFromSource(source)
	if err != nil {
		return nil, entityToolSchema{}, nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "entity_tool.schema", false, err, "resolve entity schema")
	}
	return db, schema, payload, nil
}

func parseEntityID(raw any) (string, error) {
	entityID := strings.TrimSpace(asString(raw))
	if entityID == "" {
		return "", fmt.Errorf("entity_id is required")
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return "", fmt.Errorf("entity_id must be uuid")
	}
	return entityID, nil
}

func loadEntityState(ctx context.Context, db *sql.DB, entityID string) (map[string]any, bool, error) {
	row := db.QueryRowContext(ctx, entityStateRowQuery(" WHERE entity_id = $1::uuid"), entityID)
	var raw []byte
	if err := row.Scan(&raw); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, err
	}
	entity, err := decodeEntityJSONMap(raw)
	if err != nil {
		return nil, false, err
	}
	return entity, true, nil
}

func entityStateRowQuery(suffix string) string {
	return `
		SELECT row_to_json(t)
		FROM (
			SELECT
				entity_id::text AS entity_id,
				subject_id::text AS subject_id,
				COALESCE(flow_instance, '') AS flow_instance,
				COALESCE(entity_type, '') AS entity_type,
				name,
				current_state,
				COALESCE(gates, '{}'::jsonb) AS gates,
				COALESCE(fields, '{}'::jsonb) AS fields,
				COALESCE(accumulator, '{}'::jsonb) AS accumulator,
				revision,
				entered_state_at,
				created_at,
				updated_at
			FROM entity_state` + suffix + `
		) AS t
	`
}

func queryEntityStateRows(ctx context.Context, db *sql.DB, suffix string, args ...any) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT entity_id::text, subject_id::text, COALESCE(flow_instance, ''), COALESCE(entity_type, ''), name, current_state,
		       COALESCE(gates, '{}'::jsonb), COALESCE(fields, '{}'::jsonb), COALESCE(accumulator, '{}'::jsonb),
		       revision, entered_state_at, created_at, updated_at
		FROM entity_state`+suffix, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var entityID, flowInstance, entityType, currentState string
		var subjectID sql.NullString
		var name sql.NullString
		var gatesRaw, fieldsRaw, accumulatorRaw []byte
		var revision int
		var enteredStateAt, createdAt, updatedAt time.Time
		if err := rows.Scan(
			&entityID,
			&subjectID,
			&flowInstance,
			&entityType,
			&name,
			&currentState,
			&gatesRaw,
			&fieldsRaw,
			&accumulatorRaw,
			&revision,
			&enteredStateAt,
			&createdAt,
			&updatedAt,
		); err != nil {
			return nil, err
		}
		gates, err := decodeEntityJSONMap(gatesRaw)
		if err != nil {
			return nil, err
		}
		fields, err := decodeEntityJSONMap(fieldsRaw)
		if err != nil {
			return nil, err
		}
		accumulator, err := decodeEntityJSONMap(accumulatorRaw)
		if err != nil {
			return nil, err
		}
		row := map[string]any{
			"entity_id":        entityID,
			"subject_id":       nullStringValue(subjectID),
			"flow_instance":    flowInstance,
			"entity_type":      entityType,
			"name":             nullStringValue(name),
			"current_state":    currentState,
			"gates":            gates,
			"fields":           fields,
			"accumulator":      accumulator,
			"revision":         revision,
			"entered_state_at": enteredStateAt.Format(time.RFC3339Nano),
			"created_at":       createdAt.Format(time.RFC3339Nano),
			"updated_at":       updatedAt.Format(time.RFC3339Nano),
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func entityStateBaseQuery(payload map[string]any, includeFlowInstance bool) (string, []any) {
	clauses := make([]string, 0, 2)
	args := make([]any, 0, 2)
	if subjectID := strings.TrimSpace(asString(payload["subject_id"])); subjectID != "" {
		args = append(args, subjectID)
		clauses = append(clauses, fmt.Sprintf("subject_id = $%d::uuid", len(args)))
	}
	if entityType := strings.TrimSpace(asString(payload["entity_type"])); entityType != "" {
		args = append(args, entityType)
		clauses = append(clauses, fmt.Sprintf("entity_type = $%d", len(args)))
	}
	if includeFlowInstance {
		if flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/"); flowInstance != "" {
			args = append(args, flowInstance)
			clauses = append(clauses, fmt.Sprintf("flow_instance = $%d", len(args)))
		}
	}
	return joinEntityStateWhere(clauses), args
}

func joinEntityStateWhere(clauses []string) string {
	if len(clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(clauses, " AND ")
}

func filterEntityStateRowsCEL(expression string, rows []map[string]any, schema entityToolSchema) ([]map[string]any, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return rows, nil
	}
	env, err := newEntityFilterEnv(rows, schema)
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	program, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		val, _, err := program.Eval(entityCELActivation(row))
		if err != nil {
			return nil, err
		}
		boolVal, ok := val.(types.Bool)
		if !ok {
			return nil, fmt.Errorf("filter returned non-bool %T", val)
		}
		if bool(boolVal) {
			out = append(out, row)
		}
	}
	return out, nil
}

func newEntityFilterEnv(rows []map[string]any, schema entityToolSchema) (*cel.Env, error) {
	decls := []cel.EnvOption{cel.Variable("entity", cel.DynType)}
	keys := map[string]struct{}{}
	for key := range entityStateTopLevelFields {
		keys[key] = struct{}{}
	}
	for key := range schema.Fields {
		keys[key] = struct{}{}
	}
	for _, row := range rows {
		for key := range entityRowFieldMap(row) {
			keys[key] = struct{}{}
		}
	}
	names := make([]string, 0, len(keys))
	for key := range keys {
		names = append(names, key)
	}
	sort.Strings(names)
	for _, key := range names {
		decls = append(decls, cel.Variable(key, cel.DynType))
	}
	return cel.NewEnv(decls...)
}

func entityCELActivation(row map[string]any) map[string]any {
	activation := map[string]any{
		"entity": row,
	}
	for key, value := range row {
		activation[key] = value
	}
	for key, value := range entityRowFieldMap(row) {
		if _, exists := activation[key]; !exists {
			activation[key] = value
		}
	}
	return activation
}

func decodeEntitySelect(raw any, schema entityToolSchema) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	var requested []string
	if err := decodeToolInput(raw, &requested); err != nil {
		return nil, err
	}
	requested = orderedEntityFieldNamesFromInput(requested)
	for _, key := range requested {
		if err := validateEntitySelector(schema, key); err != nil {
			return nil, err
		}
	}
	return requested, nil
}

func validateEntitySelector(schema entityToolSchema, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("field is required")
	}
	if _, ok := entityStateTopLevelFields[name]; ok {
		return nil
	}
	_, err := schema.field(name)
	return err
}

func projectEntityStateRows(rows []map[string]any, selectFields []string) []map[string]any {
	if len(selectFields) == 0 {
		return cloneEntityRows(rows)
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		projected := map[string]any{
			"entity_id": row["entity_id"],
		}
		for _, field := range selectFields {
			projected[field] = resolveEntitySelectorValue(row, field)
		}
		out = append(out, projected)
	}
	return out
}

func groupEntityStateRows(rows []map[string]any, groupBy string, selectFields []string) []map[string]any {
	grouped := map[string][]map[string]any{}
	order := make([]string, 0)
	for _, row := range rows {
		key := fmt.Sprint(resolveEntitySelectorValue(row, groupBy))
		if _, ok := grouped[key]; !ok {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], projectEntityStateRows([]map[string]any{row}, selectFields)[0])
	}
	sort.Strings(order)
	out := make([]map[string]any, 0, len(order))
	for _, key := range order {
		out = append(out, map[string]any{
			"group_key": key,
			"items":     grouped[key],
		})
	}
	return out
}

func aggregateEntityMetric(metric, fieldName string, rows []map[string]any) (any, error) {
	if metric == "count" {
		return len(rows), nil
	}
	values, err := entityMetricValues(fieldName, rows)
	if err != nil {
		return nil, err
	}
	return aggregateMetricValues(metric, values)
}

func aggregateEntityMetricGroups(metric, fieldName, groupBy string, rows []map[string]any) ([]map[string]any, error) {
	grouped := map[string][]map[string]any{}
	keys := make([]string, 0)
	for _, row := range rows {
		key := fmt.Sprint(resolveEntitySelectorValue(row, groupBy))
		if _, ok := grouped[key]; !ok {
			keys = append(keys, key)
		}
		grouped[key] = append(grouped[key], row)
	}
	sort.Strings(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		value, err := aggregateEntityMetric(metric, fieldName, grouped[key])
		if err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"group_key": key,
			"value":     value,
		})
	}
	return out, nil
}

func entityMetricValues(fieldName string, rows []map[string]any) ([]float64, error) {
	values := make([]float64, 0, len(rows))
	for _, row := range rows {
		raw := resolveEntitySelectorValue(row, fieldName)
		if raw == nil {
			continue
		}
		number, ok := numericEntityValue(raw)
		if !ok {
			return nil, fmt.Errorf("field %s is not numeric", fieldName)
		}
		values = append(values, number)
	}
	return values, nil
}

func aggregateMetricValues(metric string, values []float64) (any, error) {
	switch metric {
	case "count":
		return len(values), nil
	case "sum":
		total := 0.0
		for _, value := range values {
			total += value
		}
		return total, nil
	case "avg":
		if len(values) == 0 {
			return 0.0, nil
		}
		total := 0.0
		for _, value := range values {
			total += value
		}
		return total / float64(len(values)), nil
	case "min":
		if len(values) == 0 {
			return nil, nil
		}
		min := values[0]
		for _, value := range values[1:] {
			if value < min {
				min = value
			}
		}
		return min, nil
	case "max":
		if len(values) == 0 {
			return nil, nil
		}
		max := values[0]
		for _, value := range values[1:] {
			if value > max {
				max = value
			}
		}
		return max, nil
	default:
		return nil, fmt.Errorf("unsupported metric %q", metric)
	}
}

func resolveEntitySelectorValue(row map[string]any, field string) any {
	field = strings.TrimSpace(field)
	if field == "" {
		return nil
	}
	if value, ok := row[field]; ok {
		return value
	}
	return entityRowFieldMap(row)[field]
}

func entityRowFieldMap(row map[string]any) map[string]any {
	fields, _ := row["fields"].(map[string]any)
	if fields == nil {
		return map[string]any{}
	}
	return fields
}

func cloneEntityRows(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		cloned := make(map[string]any, len(row))
		for key, value := range row {
			cloned[key] = value
		}
		out = append(out, cloned)
	}
	return out
}

func subjectStatusFlowID(source semanticview.Source, flowInstance string) string {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" || source == nil {
		return flowInstance
	}
	if _, ok := source.FlowScopeByID(flowInstance); ok {
		return flowInstance
	}
	for flowID := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		if strings.Trim(source.FlowPath(flowID), "/") == flowInstance {
			return flowID
		}
	}
	return flowInstance
}

func subjectStatusTerminal(source semanticview.Source, flowID, state string) bool {
	state = strings.TrimSpace(state)
	if source == nil || flowID == "" || state == "" {
		return false
	}
	for _, terminal := range source.FlowTerminalStages(flowID) {
		if strings.EqualFold(strings.TrimSpace(terminal), state) {
			return true
		}
	}
	return false
}

func subjectStatusSortRows(source semanticview.Source, rows []map[string]any) {
	flowDepth := subjectStatusFlowDepths(source)
	sort.SliceStable(rows, func(i, j int) bool {
		left := subjectStatusSortKey(source, flowDepth, rows[i])
		right := subjectStatusSortKey(source, flowDepth, rows[j])
		if !left.entered.Equal(right.entered) {
			return left.entered.After(right.entered)
		}
		if left.depth != right.depth {
			return left.depth > right.depth
		}
		if !left.updated.Equal(right.updated) {
			return left.updated.After(right.updated)
		}
		if !left.created.Equal(right.created) {
			return left.created.After(right.created)
		}
		return left.entityID > right.entityID
	})
}

type subjectStatusKey struct {
	entered  time.Time
	updated  time.Time
	created  time.Time
	depth    int
	entityID string
}

func subjectStatusSortKey(source semanticview.Source, flowDepth map[string]int, row map[string]any) subjectStatusKey {
	flowInstance := strings.Trim(strings.TrimSpace(asString(row["flow_instance"])), "/")
	flowID := subjectStatusFlowID(source, flowInstance)
	state := strings.TrimSpace(asString(row["current_state"]))
	entered := subjectStatusTime(row["entered_state_at"])
	if subjectStatusTerminal(source, flowID, state) {
		entered = time.Time{}
	}
	return subjectStatusKey{
		entered:  entered,
		updated:  subjectStatusTime(row["updated_at"]),
		created:  subjectStatusTime(row["created_at"]),
		depth:    flowDepth[flowID],
		entityID: strings.TrimSpace(asString(row["entity_id"])),
	}
}

func subjectStatusTime(value any) time.Time {
	raw := strings.TrimSpace(asString(value))
	if raw == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func subjectStatusFlowDepths(source semanticview.Source) map[string]int {
	if source == nil {
		return nil
	}
	scopes := source.FlowScopes()
	out := make(map[string]int, len(scopes))
	for idx, scope := range scopes {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" {
			continue
		}
		out[flowID] = idx
	}
	return out
}

func mapKeys(values map[string]any) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	return out
}

func orderedEntityFieldNamesFromInput(names []string) []string {
	out := append([]string{}, names...)
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	sort.Strings(out)
	deduped := out[:0]
	for _, name := range out {
		if name == "" {
			continue
		}
		if len(deduped) > 0 && deduped[len(deduped)-1] == name {
			continue
		}
		deduped = append(deduped, name)
	}
	return deduped
}

func nullStringValue(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return strings.TrimSpace(value.String)
}

func numericEntityValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func decodeEntityJSONMap(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}
