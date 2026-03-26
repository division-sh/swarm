package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
)

func (e *Executor) execGetEntity(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID := strings.TrimSpace(asString(payload["entity_id"]))
	if entityID == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_get_entity.entity_id", false, "entity_id is required")
	}
	columns, err := schema.selectColumns(nil)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_get_entity.columns", false, err, "resolve entity columns")
	}
	query := fmt.Sprintf(
		"SELECT row_to_json(t) FROM (SELECT %s FROM %s WHERE %s = $1) AS t",
		entitySelectClause(columns),
		entityQuoteIdent(schema.EntityType),
		entityQuoteIdent("entity_id"),
	)
	var raw []byte
	if err := db.QueryRowContext(ctx, query, entityID).Scan(&raw); err != nil {
		if err == sql.ErrNoRows {
			return nil, NewRuntimeError("not_found", "tool-executor", "exec_get_entity.lookup", false, "entity %s/%s not found", schema.EntityType, entityID)
		}
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_get_entity.lookup", true, err, "load entity %s/%s", schema.EntityType, entityID)
	}
	return decodeEntityJSONMap(raw)
}

func (e *Executor) execSaveEntityField(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID := strings.TrimSpace(asString(payload["entity_id"]))
	if entityID == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.entity_id", false, "entity_id is required")
	}
	field, err := schema.field(asString(payload["field"]))
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", false, err, "validate field")
	}
	value, err := normalizeEntityFieldValue(field, payload["value"])
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_save_entity_field.value", false, err, "validate value")
	}
	query := fmt.Sprintf(
		"WITH updated AS (UPDATE %s SET %s = $2, %s = NOW() WHERE %s = $1 RETURNING 1) "+
			"INSERT INTO %s (%s, %s, %s) "+
			"SELECT $1, $2, NOW() WHERE NOT EXISTS (SELECT 1 FROM updated) "+
			"ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s, %s = NOW()",
		entityQuoteIdent(schema.EntityType),
		entityQuoteIdent(strings.TrimSpace(field.Name)),
		entityQuoteIdent("updated_at"),
		entityQuoteIdent("entity_id"),
		entityQuoteIdent(schema.EntityType),
		entityQuoteIdent("entity_id"),
		entityQuoteIdent(strings.TrimSpace(field.Name)),
		entityQuoteIdent("updated_at"),
		entityQuoteIdent("entity_id"),
		entityQuoteIdent(strings.TrimSpace(field.Name)),
		entityQuoteIdent(strings.TrimSpace(field.Name)),
		entityQuoteIdent("updated_at"),
	)
	if _, err := db.ExecContext(ctx, query, entityID, value); err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_save_entity_field.upsert", true, err, "save entity field %s.%s", schema.EntityType, field.Name)
	}
	return map[string]any{"success": true}, nil
}

func (e *Executor) execCreateEntity(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID := strings.TrimSpace(asString(payload["entity_id"]))
	if entityID == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.entity_id", false, "entity_id is required")
	}
	fields := map[string]any{}
	if raw, ok := payload["fields"]; ok && raw != nil {
		if decoded, ok := raw.(map[string]any); ok {
			fields = decoded
		} else if err := decodeToolInput(raw, &fields); err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.fields", false, err, "decode fields")
		}
	}

	columns := []string{entityQuoteIdent("entity_id")}
	args := []any{entityID}
	placeholders := []string{"$1"}
	paramIndex := 2
	fieldNames := make([]string, 0, len(fields))
	for name := range fields {
		fieldNames = append(fieldNames, name)
	}
	for _, name := range orderedEntityFieldNamesFromInput(fieldNames) {
		field, err := schema.field(name)
		if err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.field", false, err, "validate field")
		}
		value, err := normalizeEntityFieldValue(field, fields[name])
		if err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_create_entity.value", false, err, "validate field %s", name)
		}
		columns = append(columns, entityQuoteIdent(name))
		args = append(args, value)
		placeholders = append(placeholders, fmt.Sprintf("$%d", paramIndex))
		paramIndex++
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		entityQuoteIdent(schema.EntityType),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return nil, WrapRuntimeError("write_failed", "tool-executor", "exec_create_entity.insert", false, err, "create entity %s/%s", schema.EntityType, entityID)
	}
	return map[string]any{"entity_id": entityID, "created": true}, nil
}

func (e *Executor) execSearchEntities(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	var requested []string
	if rawSelect, ok := payload["select"]; ok && rawSelect != nil {
		if err := decodeToolInput(rawSelect, &requested); err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.select", false, err, "decode select")
		}
	}
	columns, err := schema.selectColumns(requested)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.columns", false, err, "validate select")
	}
	whereSQL, args, err := parseEntityFilter(schema, asString(payload["filter"]), 1)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.filter", false, err, "parse filter")
	}
	limit := defaultEntitySearchLimit(asInt(payload["limit"]))
	args = append(args, limit)
	query := fmt.Sprintf(
		"SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json) FROM (SELECT %s FROM %s%s ORDER BY %s DESC LIMIT $%d) AS t",
		entitySelectClause(columns),
		entityQuoteIdent(schema.EntityType),
		whereSQL,
		entityQuoteIdent("created_at"),
		len(args),
	)
	var raw []byte
	if err := db.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_search_entities.query", true, err, "search entities %s", schema.EntityType)
	}
	return decodeEntityJSONArray(raw)
}

func (e *Executor) execQueryMetrics(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, schema, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	metric := strings.ToLower(strings.TrimSpace(asString(payload["metric"])))
	var metricFieldName string
	var metricField runtimecontracts.EntitySchemaField
	if metric != "count" {
		metricFieldName = strings.TrimSpace(asString(payload["field"]))
		if metricFieldName == "" {
			return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.field", false, "field is required for metric %s", metric)
		}
		metricField, err = schema.field(metricFieldName)
		if err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.field", false, err, "validate metric field")
		}
	}
	expr, err := metricExpression(metric, metricField)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.metric", false, err, "build metric expression")
	}
	whereSQL, args, err := parseEntityFilter(schema, asString(payload["filter"]), 1)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.filter", false, err, "parse filter")
	}
	groupBy := strings.TrimSpace(asString(payload["group_by"]))
	if groupBy == "" {
		query := fmt.Sprintf("SELECT to_json(%s) FROM %s%s", expr, entityQuoteIdent(schema.EntityType), whereSQL)
		var raw []byte
		if err := db.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
			return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_query_metrics.scalar", true, err, "query metrics %s", schema.EntityType)
		}
		result, err := decodeEntityJSONValue(raw)
		if err != nil {
			return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_query_metrics.scalar", true, err, "decode metric result")
		}
		return map[string]any{"result": result}, nil
	}
	groupField, err := schema.field(groupBy)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.group_by", false, err, "validate group_by")
	}
	query := fmt.Sprintf(
		"SELECT COALESCE(json_object_agg(k, v), '{}'::json) FROM (SELECT COALESCE(%s::text, 'null') AS k, %s AS v FROM %s%s GROUP BY %s ORDER BY %s) AS grouped",
		entityQuoteIdent(strings.TrimSpace(groupField.Name)),
		expr,
		entityQuoteIdent(schema.EntityType),
		whereSQL,
		entityQuoteIdent(strings.TrimSpace(groupField.Name)),
		entityQuoteIdent(strings.TrimSpace(groupField.Name)),
	)
	var raw []byte
	if err := db.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_query_metrics.grouped", true, err, "query grouped metrics %s", schema.EntityType)
	}
	result, err := decodeEntityJSONMap(raw)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_query_metrics.grouped", true, err, "decode grouped metric result")
	}
	return map[string]any{"result": result}, nil
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
	schema, err := entityToolSchemaFromSource(source, asString(payload["entity_type"]))
	if err != nil {
		return nil, entityToolSchema{}, nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "entity_tool.schema", false, err, "resolve entity schema")
	}
	return db, schema, payload, nil
}

func entitySelectClause(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, entityQuoteIdent(strings.TrimSpace(column)))
	}
	return strings.Join(quoted, ", ")
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

func decodeEntityJSONArray(raw []byte) ([]map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return []map[string]any{}, nil
	}
	return out, nil
}

func decodeEntityJSONValue(raw []byte) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func orderedEntityFieldNamesFromInput(names []string) []string {
	out := append([]string{}, names...)
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	out = uniqueNonEmptyStrings(out)
	sort.Strings(out)
	return out
}
