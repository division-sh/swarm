package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types"
	models "swarm/internal/runtime/core/actors"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	"swarm/internal/runtime/entityruntime"
	"swarm/internal/runtime/semanticview"
)

func (e *Executor) execSearchEntities(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	db, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	schema, err := entityToolSchemaForReadTarget(source, actor.ID, payload)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.schema", false, err, "resolve entity contract")
	}
	filterSQL, args := entityStateBaseQueryForContract(source, schema.Contract, payload, true)
	currentStateFilter := strings.TrimSpace(asString(payload["current_state"]))
	if currentStateFilter != "" {
		args = append(args, currentStateFilter)
		filterSQL = append(filterSQL, fmt.Sprintf("current_state = $%d", len(args)))
	}
	if rawFilter, ok := payload["filter"]; ok && rawFilter != nil {
		filterObject := map[string]any{}
		if decoded, ok := rawFilter.(map[string]any); ok {
			filterObject = decoded
		} else if err := decodeToolInput(rawFilter, &filterObject); err != nil {
			return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.filter", false, err, "decode filter")
		}
		for _, fieldName := range orderedEntityFieldNamesFromInput(mapKeys(filterObject)) {
			field, err := schema.field(fieldName)
			if err != nil {
				return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.filter", false, err, "validate filter field")
			}
			value, err := normalizeEntityFieldValue(schema, field, filterObject[fieldName])
			if err != nil {
				return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.filter", false, err, "validate filter field %s", fieldName)
			}
			filterJSON, err := json.Marshal(value)
			if err != nil {
				return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_search_entities.filter", false, err, "marshal filter field %s", fieldName)
			}
			args = append(args, string(filterJSON))
			filterSQL = append(filterSQL, fmt.Sprintf("%s = $%d::jsonb", entityFilterSQLPath(field.Path), len(args)))
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
	rows, err = materializeEntityStateRows(source, rows)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_search_entities.materialize", false, err, "materialize query results")
	}
	rows = filterEntityReadRowsForActor(source, actor.ID, rows)
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

func (e *Executor) execQueryEntities(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	db, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	schema, err := entityToolSchemaForReadTarget(source, actor.ID, payload)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_entities.schema", false, err, "resolve entity contract")
	}
	filterSQL, args := entityStateBaseQueryForContract(source, schema.Contract, payload, true)
	whereClause := joinEntityStateWhere(filterSQL)
	rows, err := queryEntityStateRows(ctx, db, whereClause+" ORDER BY created_at DESC", args...)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_query_entities.query", true, err, "query entity_state")
	}
	rows, err = materializeEntityStateRows(source, rows)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_query_entities.materialize", false, err, "materialize query results")
	}
	rows = filterEntityReadRowsForActor(source, actor.ID, rows)
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

func (e *Executor) execQueryMetrics(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	db, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	schema, err := entityToolSchemaForReadTarget(source, actor.ID, payload)
	if err != nil {
		return nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_metrics.schema", false, err, "resolve entity contract")
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
	filterSQL, args := entityStateBaseQueryForContract(source, schema.Contract, payload, true)
	whereClause := joinEntityStateWhere(filterSQL)
	rows, err := queryEntityStateRows(ctx, db, whereClause+" ORDER BY created_at DESC", args...)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_query_metrics.query", true, err, "query entity_state metrics")
	}
	rows, err = materializeEntityStateRows(source, rows)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_query_metrics.materialize", false, err, "materialize query results")
	}
	rows = filterEntityReadRowsForActor(source, actor.ID, rows)
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

func entityStateBaseQuery(source semanticview.Source, actor models.AgentConfig, payload map[string]any, includeFlowInstance bool) ([]string, []any) {
	contract, _ := entityruntime.ResolveForActor(source, actor.ID)
	return entityStateBaseQueryForContract(source, contract, payload, includeFlowInstance)
}

func entityStateBaseQueryForContract(source semanticview.Source, contract entityruntime.Contract, payload map[string]any, includeFlowInstance bool) ([]string, []any) {
	clauses := make([]string, 0, 4)
	args := make([]any, 0, 4)
	flowRoot := entityReadFlowScopeRoot(source, contract)
	if flowRoot != "" {
		args = append(args, flowRoot, flowRoot+"/%")
		clauses = append(clauses, fmt.Sprintf("(flow_instance = $%d OR flow_instance LIKE $%d)", len(args)-1, len(args)))
	}
	if includeFlowInstance {
		clauses, args = appendEntityToolExistingFlowInstanceFilter(source, clauses, args, asString(payload["flow_instance"]))
	}
	return clauses, args
}

func entityReadFlowScopeRoot(source semanticview.Source, contract entityruntime.Contract) string {
	flowID := strings.TrimSpace(contract.FlowID)
	if source == nil || flowID == "" {
		return ""
	}
	return runtimeflowidentity.ScopeKey(source, flowID)
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
	env, err := newEntityFilterEnv(schema)
	if err != nil {
		return nil, err
	}
	if err := validateEntityFilterExpression(env, expression, schema); err != nil {
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
		val, _, err := program.Eval(entityCELActivation(row, schema))
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

func newEntityFilterEnv(schema entityToolSchema) (*cel.Env, error) {
	decls := []cel.EnvOption{cel.Variable("entity", cel.DynType)}
	keys := map[string]struct{}{}
	for key := range entityStateTopLevelFields {
		keys[key] = struct{}{}
	}
	for _, key := range entityruntime.FieldNames(schema.Contract) {
		keys[key] = struct{}{}
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

type entityFilterSelectorReference struct {
	Path         string
	EntityScoped bool
}

func (r entityFilterSelectorReference) display() string {
	path := strings.TrimSpace(r.Path)
	if path == "" {
		return ""
	}
	if r.EntityScoped {
		return "entity." + path
	}
	return path
}

func validateEntityFilterExpression(env *cel.Env, expression string, schema entityToolSchema) error {
	if env == nil {
		return fmt.Errorf("entity filter environment is not configured")
	}
	parsed, issues := env.Parse(expression)
	if issues != nil && issues.Err() != nil {
		return issues.Err()
	}
	for _, ref := range entityFilterSelectorReferences(parsed) {
		if err := validateEntityFilterSelectorReference(schema, ref); err != nil {
			return err
		}
	}
	return nil
}

func entityFilterSelectorReferences(parsed *cel.Ast) []entityFilterSelectorReference {
	if parsed == nil || parsed.NativeRep() == nil {
		return nil
	}
	root := celast.NavigateAST(parsed.NativeRep())
	seen := map[string]struct{}{}
	refs := make([]entityFilterSelectorReference, 0)
	var visit func(expr celast.NavigableExpr)
	visit = func(expr celast.NavigableExpr) {
		if expr.Kind() == celast.IdentKind && !entityFilterSelectorHasParent(expr) {
			if ref, ok := entityFilterSelectorBase(expr); ok {
				key := fmt.Sprintf("%t:%s", ref.EntityScoped, ref.Path)
				if _, exists := seen[key]; !exists {
					seen[key] = struct{}{}
					refs = append(refs, ref)
				}
			}
		}
		if expr.Kind() == celast.SelectKind && !entityFilterSelectorHasParent(expr) {
			if ref, ok := entityFilterSelectorReferenceFromExpr(expr); ok {
				key := fmt.Sprintf("%t:%s", ref.EntityScoped, ref.Path)
				if _, exists := seen[key]; !exists {
					seen[key] = struct{}{}
					refs = append(refs, ref)
				}
			}
		}
		for _, child := range expr.Children() {
			visit(child)
		}
	}
	visit(root)
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].EntityScoped != refs[j].EntityScoped {
			return !refs[i].EntityScoped && refs[j].EntityScoped
		}
		return refs[i].Path < refs[j].Path
	})
	return refs
}

func entityFilterSelectorHasParent(expr celast.NavigableExpr) bool {
	parent, ok := expr.Parent()
	if !ok || parent.Kind() != celast.SelectKind {
		return false
	}
	return parent.AsSelect().Operand().ID() == expr.ID()
}

func entityFilterSelectorReferenceFromExpr(expr celast.Expr) (entityFilterSelectorReference, bool) {
	if expr == nil || expr.Kind() != celast.SelectKind {
		return entityFilterSelectorReference{}, false
	}
	selector := expr.AsSelect()
	base, ok := entityFilterSelectorBase(selector.Operand())
	if !ok {
		return entityFilterSelectorReference{}, false
	}
	path := strings.TrimSpace(base.Path)
	field := strings.TrimSpace(selector.FieldName())
	if path == "" {
		path = field
	} else {
		path = path + "." + field
	}
	return entityFilterSelectorReference{Path: path, EntityScoped: base.EntityScoped}, true
}

func entityFilterSelectorBase(expr celast.Expr) (entityFilterSelectorReference, bool) {
	if expr == nil {
		return entityFilterSelectorReference{}, false
	}
	switch expr.Kind() {
	case celast.IdentKind:
		name := strings.TrimSpace(expr.AsIdent())
		if name == "" {
			return entityFilterSelectorReference{}, false
		}
		if name == "entity" {
			return entityFilterSelectorReference{EntityScoped: true}, true
		}
		return entityFilterSelectorReference{Path: name}, true
	case celast.SelectKind:
		return entityFilterSelectorReferenceFromExpr(expr)
	default:
		return entityFilterSelectorReference{}, false
	}
}

func validateEntityFilterSelectorReference(schema entityToolSchema, ref entityFilterSelectorReference) error {
	if ref.EntityScoped {
		path := strings.TrimSpace(ref.Path)
		if path == "" {
			return fmt.Errorf("%w: query filter selectors must not use the entity root", ErrUnknownEntityField)
		}
		return fmt.Errorf("%w: query filter selectors must not use entity.%s; use %s instead", ErrUnknownEntityField, path, path)
	}
	if err := validateEntitySelector(schema, ref.Path); err != nil {
		return decorateEntityFilterSelectorError(schema, ref, err)
	}
	return nil
}

func decorateEntityFilterSelectorError(schema entityToolSchema, ref entityFilterSelectorReference, err error) error {
	if err == nil {
		return nil
	}
	if !entityFilterUndeclaredPathError(err) {
		return err
	}
	display := ref.display()
	if display == "" {
		return err
	}
	if suggestion := nearestEntityFilterSelector(schema, ref); suggestion != "" {
		return fmt.Errorf("%w: undeclared field %s (did you mean %s?)", ErrUnknownEntityField, display, suggestion)
	}
	return fmt.Errorf("%w: undeclared field %s", ErrUnknownEntityField, display)
}

func entityFilterUndeclaredPathError(err error) bool {
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(text, "undeclared field") || strings.Contains(text, "undeclared path")
}

func nearestEntityFilterSelector(schema entityToolSchema, ref entityFilterSelectorReference) string {
	candidates := entityToolSelectableFieldNames(schema.Contract)
	if len(candidates) == 0 {
		return ""
	}
	path := strings.TrimSpace(ref.Path)
	if path == "" {
		return ""
	}
	best := ""
	bestScore := -1
	inputParent, inputLeaf := entityFilterPathParentAndLeaf(path)
	for _, candidate := range candidates {
		score := levenshteinDistance(strings.ToLower(path), strings.ToLower(candidate))
		candidateParent, candidateLeaf := entityFilterPathParentAndLeaf(candidate)
		if inputParent != "" && candidateParent == inputParent {
			if leafScore := levenshteinDistance(strings.ToLower(inputLeaf), strings.ToLower(candidateLeaf)); leafScore < score {
				score = leafScore
			}
		}
		if bestScore == -1 || score < bestScore || (score == bestScore && candidate < best) {
			best = candidate
			bestScore = score
		}
	}
	if best == "" || bestScore > entityFilterSuggestionThreshold(path) {
		return ""
	}
	if ref.EntityScoped {
		return "entity." + best
	}
	return best
}

func entityFilterPathParentAndLeaf(path string) (string, string) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", ""
	}
	idx := strings.LastIndex(path, ".")
	if idx < 0 {
		return "", path
	}
	return strings.TrimSpace(path[:idx]), strings.TrimSpace(path[idx+1:])
}

func entityFilterSuggestionThreshold(path string) int {
	path = strings.TrimSpace(path)
	switch {
	case len(path) <= 6:
		return 2
	case len(path) <= 16:
		return 3
	default:
		return 4
	}
}

func levenshteinDistance(a, b string) int {
	if a == b {
		return 0
	}
	if a == "" {
		return len(b)
	}
	if b == "" {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := 0; j <= len(b); j++ {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			insert := curr[j-1] + 1
			delete := prev[j] + 1
			replace := prev[j-1] + cost
			curr[j] = minEntityFilterDistance(insert, delete, replace)
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

func minEntityFilterDistance(values ...int) int {
	best := values[0]
	for _, value := range values[1:] {
		if value < best {
			best = value
		}
	}
	return best
}

func entityCELActivation(row map[string]any, schema entityToolSchema) map[string]any {
	activation := map[string]any{
		"entity": row,
	}
	for key, value := range row {
		activation[key] = value
	}
	for _, key := range entityruntime.FieldNames(schema.Contract) {
		if value, ok := entityRowFieldMap(row)[key]; ok {
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
	if value, ok := entityruntime.PathValue(entityRowFieldMap(row), field); ok {
		return value
	}
	return nil
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
