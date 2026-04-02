package tools

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
	runtimesharedjson "swarm/internal/runtime/sharedjson"
)

type entityToolSchema struct {
	Defined bool
	Fields  map[string]runtimecontracts.EntitySchemaField
}

type entityFilterCondition struct {
	Field    runtimecontracts.EntitySchemaField
	Operator string
	Value    any
}

var (
	entityFilterSplitter = regexp.MustCompile(`(?i)\s+AND\s+`)
	entityFilterPattern  = regexp.MustCompile(`^\s*([a-z_][a-z0-9_]*)\s*(=|!=|>=|<=|>|<)\s*(.+?)\s*$`)
)

func entityToolSchemaFromSource(source semanticview.Source) (entityToolSchema, error) {
	if source == nil {
		return entityToolSchema{}, fmt.Errorf("workflow source is not configured")
	}
	fields := map[string]runtimecontracts.EntitySchemaField{}
	defined := false
	for _, group := range source.WorkflowEntitySchema().Groups {
		for _, field := range group.Fields {
			name := strings.TrimSpace(field.Name)
			if name == "" {
				continue
			}
			fields[name] = field
			defined = true
		}
	}
	return entityToolSchema{
		Defined: defined,
		Fields:  fields,
	}, nil
}

func (s entityToolSchema) field(name string) (runtimecontracts.EntitySchemaField, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return runtimecontracts.EntitySchemaField{}, fmt.Errorf("field is required")
	}
	if !s.Defined {
		return runtimecontracts.EntitySchemaField{Name: name}, nil
	}
	field, ok := s.Fields[name]
	if !ok {
		return runtimecontracts.EntitySchemaField{}, fmt.Errorf("%w: entity schema does not define field %s", ErrUnknownEntityField, name)
	}
	return field, nil
}

func normalizeEntityFieldValue(field runtimecontracts.EntitySchemaField, value any) (any, error) {
	fieldType := strings.ToLower(strings.TrimSpace(field.Type))
	if fieldType == "" {
		return value, nil
	}
	if value == nil {
		if field.Nullable {
			return nil, nil
		}
		return nil, fmt.Errorf("field %s is not nullable", strings.TrimSpace(field.Name))
	}
	switch {
	case fieldType == "text" || fieldType == "string":
		if _, ok := value.(string); !ok {
			return nil, fmt.Errorf("field %s must be string", strings.TrimSpace(field.Name))
		}
		return value, nil
	case fieldType == "integer":
		if !runtimesharedjson.IsInteger(value) {
			return nil, fmt.Errorf("field %s must be integer", strings.TrimSpace(field.Name))
		}
		f, _ := runtimesharedjson.AsFloat64(value)
		return int64(f), nil
	case strings.HasPrefix(fieldType, "numeric("):
		if !runtimesharedjson.IsNumeric(value) {
			return nil, fmt.Errorf("field %s must be numeric", strings.TrimSpace(field.Name))
		}
		f, _ := runtimesharedjson.AsFloat64(value)
		return f, nil
	case fieldType == "boolean":
		if _, ok := value.(bool); !ok {
			return nil, fmt.Errorf("field %s must be boolean", strings.TrimSpace(field.Name))
		}
		return value, nil
	case fieldType == "jsonb":
		if _, err := json.Marshal(value); err != nil {
			return nil, fmt.Errorf("field %s must be valid json: %w", strings.TrimSpace(field.Name), err)
		}
		return value, nil
	case fieldType == "timestamp":
		switch t := value.(type) {
		case time.Time:
			return t.UTC(), nil
		case string:
			parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(t))
			if err != nil {
				return nil, fmt.Errorf("field %s must be RFC3339 timestamp", strings.TrimSpace(field.Name))
			}
			return parsed.UTC(), nil
		default:
			return nil, fmt.Errorf("field %s must be timestamp", strings.TrimSpace(field.Name))
		}
	case fieldType == "uuid":
		id := strings.TrimSpace(runtimesharedjson.AsString(value))
		if _, err := uuid.Parse(id); err != nil {
			return nil, fmt.Errorf("field %s must be uuid", strings.TrimSpace(field.Name))
		}
		return id, nil
	default:
		return nil, fmt.Errorf("field %s has unsupported schema type %s", strings.TrimSpace(field.Name), field.Type)
	}
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
		value, err := parseEntityFilterValue(field, match[3])
		if err != nil {
			return "", nil, err
		}
		operator := strings.TrimSpace(match[2])
		clauses = append(clauses, fmt.Sprintf("%s %s $%d", entityQuoteIdent(strings.TrimSpace(field.Name)), operator, paramIndex))
		args = append(args, value)
		paramIndex++
	}
	if len(clauses) == 0 {
		return "", nil, nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args, nil
}

func parseEntityFilterValue(field runtimecontracts.EntitySchemaField, raw string) (any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("filter value for field %s is required", strings.TrimSpace(field.Name))
	}
	if strings.EqualFold(raw, "null") {
		return normalizeEntityFieldValue(field, nil)
	}
	if unquoted, ok := unquoteEntityFilterValue(raw); ok {
		return normalizeEntityFieldValue(field, unquoted)
	}
	switch strings.ToLower(strings.TrimSpace(field.Type)) {
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
			return v, nil
		}
	default:
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(field.Type)), "numeric(") {
			if f, ok := runtimesharedjson.AsFloat64(parseLooseNumeric(raw)); ok {
				return normalizeEntityFieldValue(field, f)
			}
		}
	}
	return normalizeEntityFieldValue(field, raw)
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
		return nil
	}
	var floatValue float64
	if _, err := fmt.Sscanf(raw, "%f", &floatValue); err == nil {
		return floatValue
	}
	return raw
}

func metricExpression(metric string, field runtimecontracts.EntitySchemaField) (string, error) {
	metric = strings.ToLower(strings.TrimSpace(metric))
	switch metric {
	case "count":
		return "COUNT(*)", nil
	case "sum", "avg":
		fieldType := strings.ToLower(strings.TrimSpace(field.Type))
		if fieldType != "integer" && !strings.HasPrefix(fieldType, "numeric(") {
			return "", fmt.Errorf("metric %s requires numeric field, got %s", metric, field.Type)
		}
		return strings.ToUpper(metric) + "(" + entityQuoteIdent(strings.TrimSpace(field.Name)) + ")", nil
	case "min", "max":
		if strings.EqualFold(strings.TrimSpace(field.Type), "jsonb") {
			return "", fmt.Errorf("metric %s does not support jsonb field %s", metric, strings.TrimSpace(field.Name))
		}
		return strings.ToUpper(metric) + "(" + entityQuoteIdent(strings.TrimSpace(field.Name)) + ")", nil
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

func orderedEntityFieldNames(fields map[string]runtimecontracts.EntitySchemaField) []string {
	out := make([]string, 0, len(fields))
	for name := range fields {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func entityQuoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}
