package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	models "swarm/internal/runtime/core/actors"
)

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
