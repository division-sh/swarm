package tools

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"swarm/internal/runtime/entityruntime"
	"swarm/internal/runtime/semanticview"
)

type entityToolSchema struct {
	Defined  bool
	Contract entityruntime.Contract
}

type entityFilterCondition struct {
	Field    entityruntime.Field
	Operator string
	Value    any
}

var (
	entityFilterSplitter = regexp.MustCompile(`(?i)\s+AND\s+`)
	entityFilterPattern  = regexp.MustCompile(`^\s*([a-z_][a-z0-9_.]*)\s*(=|!=|>=|<=|>|<)\s*(.+?)\s*$`)
)

func entityToolSchemaForActor(source semanticview.Source, actorID string) (entityToolSchema, error) {
	if source == nil {
		return entityToolSchema{}, fmt.Errorf("workflow source is not configured")
	}
	contract, ok := entityruntime.ResolveForActor(source, actorID)
	if !ok {
		return entityToolSchema{}, fmt.Errorf("flow-owned entity contract is not available for actor %s", strings.TrimSpace(actorID))
	}
	return entityToolSchema{Defined: true, Contract: contract}, nil
}

func entityToolSchemaForReadTarget(source semanticview.Source, actorID string, payload map[string]any) (entityToolSchema, error) {
	if source == nil {
		return entityToolSchema{}, fmt.Errorf("workflow source is not configured")
	}
	target := strings.TrimSpace(asString(payload["entity_type"]))
	contract, ok, err := entityruntime.ResolveForReadTarget(source, actorID, target)
	if err != nil {
		return entityToolSchema{}, err
	}
	if !ok {
		return entityToolSchema{}, fmt.Errorf("flow-owned entity contract is not available for actor %s", strings.TrimSpace(actorID))
	}
	return entityToolSchema{Defined: true, Contract: contract}, nil
}

func entityToolSchemaForEntityRow(source semanticview.Source, row map[string]any) (entityToolSchema, error) {
	if source == nil {
		return entityToolSchema{}, fmt.Errorf("workflow source is not configured")
	}
	contract, ok := entityruntime.ResolveForEntityRow(source, row)
	if !ok {
		return entityToolSchema{}, fmt.Errorf("flow-owned entity contract is not available for entity flow_instance %s", strings.TrimSpace(asString(row["flow_instance"])))
	}
	return entityToolSchema{Defined: true, Contract: contract}, nil
}

func (s entityToolSchema) field(name string) (entityruntime.Field, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return entityruntime.Field{}, fmt.Errorf("field is required")
	}
	field, err := entityruntime.ResolveLeafField(s.Contract, name)
	if err != nil {
		return entityruntime.Field{}, fmt.Errorf("%w: %v", ErrUnknownEntityField, err)
	}
	return field, nil
}

func (s entityToolSchema) declaredField(name string) (entityruntime.Field, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return entityruntime.Field{}, fmt.Errorf("field is required")
	}
	field, err := entityruntime.ResolveFieldPath(s.Contract, name)
	if err != nil {
		return entityruntime.Field{}, fmt.Errorf("%w: %v", ErrUnknownEntityField, err)
	}
	return field, nil
}

func normalizeEntityFieldValue(schema entityToolSchema, field entityruntime.Field, value any) (any, error) {
	return entityruntime.NormalizeFieldValue(schema.Contract, field.Path, value)
}

func parseEntityFilter(schema entityToolSchema, filter string, paramStart int) (string, []any, error) {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return "", nil, nil
	}
	parts := entityFilterSplitter.Split(filter, -1)
	clauses := make([]string, 0, len(parts))
	args := make([]any, 0, len(parts))
	paramIndex := paramStart
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		match := entityFilterPattern.FindStringSubmatch(part)
		if len(match) != 4 {
			return "", nil, fmt.Errorf("invalid filter expression %q", part)
		}
		field, err := schema.field(match[1])
		if err != nil {
			return "", nil, err
		}
		value, err := parseEntityFilterValue(schema, field, match[3])
		if err != nil {
			return "", nil, err
		}
		clauses = append(clauses, fmt.Sprintf("%s %s $%d", entityFilterSQLPath(field.Path), strings.TrimSpace(match[2]), paramIndex))
		args = append(args, value)
		paramIndex++
	}
	if len(clauses) == 0 {
		return "", nil, nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args, nil
}

func parseEntityFilterValue(schema entityToolSchema, field entityruntime.Field, raw string) (any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("filter value for field %s is required", strings.TrimSpace(field.Path))
	}
	if strings.EqualFold(raw, "null") {
		return normalizeEntityFieldValue(schema, field, nil)
	}
	if unquoted, ok := unquoteEntityFilterValue(raw); ok {
		return normalizeEntityFieldValue(schema, field, unquoted)
	}
	switch strings.ToLower(field.Type) {
	case "boolean":
		switch strings.ToLower(raw) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
	case "integer":
		var v int64
		if _, err := fmt.Sscanf(raw, "%d", &v); err == nil {
			return normalizeEntityFieldValue(schema, field, v)
		}
	default:
		if f := parseLooseNumeric(raw); f != raw {
			return normalizeEntityFieldValue(schema, field, f)
		}
	}
	return normalizeEntityFieldValue(schema, field, raw)
}

func unquoteEntityFilterValue(raw string) (string, bool) {
	if len(raw) < 2 {
		return "", false
	}
	if (strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'")) || (strings.HasPrefix(raw, `"`) && strings.HasSuffix(raw, `"`)) {
		return raw[1 : len(raw)-1], true
	}
	return "", false
}

func parseLooseNumeric(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	var floatValue float64
	if _, err := fmt.Sscanf(raw, "%f", &floatValue); err == nil {
		return floatValue
	}
	return raw
}

func metricExpression(metric string, field entityruntime.Field) (string, error) {
	metric = strings.ToLower(strings.TrimSpace(metric))
	switch metric {
	case "count":
		return "COUNT(*)", nil
	case "sum", "avg":
		fieldType := strings.ToLower(strings.TrimSpace(field.Type))
		if fieldType != "integer" && !strings.HasPrefix(fieldType, "numeric") {
			return "", fmt.Errorf("metric %s requires numeric field, got %s", metric, field.Type)
		}
		return strings.ToUpper(metric) + "(" + entityFilterSQLPath(field.Path) + ")", nil
	case "min", "max":
		return strings.ToUpper(metric) + "(" + entityFilterSQLPath(field.Path) + ")", nil
	default:
		return "", fmt.Errorf("unsupported metric %q", metric)
	}
}

func defaultEntitySearchLimit(value int) int {
	if value <= 0 {
		return 100
	}
	if value > 1000 {
		return 1000
	}
	return value
}

func orderedEntityFieldNames(fields map[string]entityruntime.Field) []string {
	out := make([]string, 0, len(fields))
	for name := range fields {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func entityFilterSQLPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return `COALESCE(fields, '{}'::jsonb)`
	}
	segments := strings.Split(path, ".")
	if len(segments) == 1 {
		return fmt.Sprintf("COALESCE(fields, '{}'::jsonb) -> %s", entitySQLLiteral(segments[0]))
	}
	sqlPath := make([]string, 0, len(segments))
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		sqlPath = append(sqlPath, entitySQLLiteral(segment))
	}
	return fmt.Sprintf("COALESCE(fields, '{}'::jsonb) #> ARRAY[%s]", strings.Join(sqlPath, ", "))
}

func entitySQLLiteral(value string) string {
	value = strings.ReplaceAll(strings.TrimSpace(value), `'`, `''`)
	return "'" + value + "'"
}

func entityQuoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}
