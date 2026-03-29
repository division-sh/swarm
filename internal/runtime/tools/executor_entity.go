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
)

var entityStateTopLevelFields = map[string]struct{}{
	"entity_id":        {},
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

func (e *Executor) execSaveEntityField(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID, err := parseEntityID(payload["entity_id"])
	if err != nil {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.entity_id", false, err.Error())
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
	var revision int
	if err := db.QueryRowContext(ctx, `
		UPDATE entity_state
		SET
			fields = jsonb_set(COALESCE(fields, '{}'::jsonb), ARRAY[$2], $3::jsonb, true),
			revision = revision + 1,
			updated_at = now()
		WHERE entity_id = $1::uuid
		RETURNING revision
	`, entityID, fieldName, string(valueJSON)).Scan(&revision); err != nil {
		if err == sql.ErrNoRows {
			return nil, NewRuntimeError("not_found", "tool-executor", "exec_save_entity_field.update", false, "entity %s not found", entityID)
		}
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_save_entity_field.update", true, err, "save entity field %s", fieldName)
	}
	return map[string]any{
		"entity_id": entityID,
		"field":     fieldName,
		"revision":  revision,
	}, nil
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
			entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2, $3, NULLIF($4, ''),
			$5, '{}'::jsonb, $6::jsonb, '{}'::jsonb, 1,
			$7, $7, $7
		)
	`, entityID, flowInstance, entityType, name, currentState, string(fieldsJSON), now); err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_create_entity.insert", false, err, "create entity %s", entityID)
	}
	return map[string]any{
		"entity_id":     entityID,
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
	if flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/"); flowInstance != "" {
		args = append(args, flowInstance)
		filterSQL = append(filterSQL, fmt.Sprintf("flow_instance = $%d", len(args)))
	}
	if entityType := strings.TrimSpace(asString(payload["entity_type"])); entityType != "" {
		args = append(args, entityType)
		filterSQL = append(filterSQL, fmt.Sprintf("entity_type = $%d", len(args)))
	}
	if currentState := strings.TrimSpace(asString(payload["current_state"])); currentState != "" {
		args = append(args, currentState)
		filterSQL = append(filterSQL, fmt.Sprintf("current_state = $%d", len(args)))
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

	totalQuery := "SELECT COUNT(*) FROM entity_state" + whereClause
	var total int
	if err := db.QueryRowContext(ctx, totalQuery, args...).Scan(&total); err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_search_entities.total", true, err, "count entity_state rows")
	}

	args = append(args, limit, offset)
	rows, err := queryEntityStateRows(ctx, db, whereClause+fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args)), args...)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_search_entities.query", true, err, "search entity_state")
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
	return entity, err == nil, err
}

func entityStateRowQuery(suffix string) string {
	return `
		SELECT row_to_json(t)
		FROM (
			SELECT
				entity_id::text AS entity_id,
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
		SELECT entity_id::text, COALESCE(flow_instance, ''), COALESCE(entity_type, ''), name, current_state,
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
		var name sql.NullString
		var gatesRaw, fieldsRaw, accumulatorRaw []byte
		var revision int
		var enteredStateAt, createdAt, updatedAt time.Time
		if err := rows.Scan(
			&entityID,
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
