package pipeline

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

var workflowExpressionKeywordReplacer = regexp.MustCompile(`\b(AND|OR|NOT|TRUE|FALSE)\b`)
var workflowExpressionPolicyPlaceholder = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}`)
var workflowExpressionCountGEPattern = regexp.MustCompile(`count\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*>=\s*([0-9]+(?:\.[0-9]+)?)\s*\)`)
var workflowExpressionStageRangeCountPattern = regexp.MustCompile(`count\(\s*entities\s+in\s+\[([a-zA-Z_][a-zA-Z0-9_]*)\.\.([a-zA-Z_][a-zA-Z0-9_]*)\]\s*\)`)
var workflowExpressionQueryEntitiesCountPattern = regexp.MustCompile(`query_entities\(\s*([^()]+?)\s*\)\.count`)
var workflowExpressionQueryPredicatePattern = regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*(==|!=|>=|<=|>|<)\s*(.+?)\s*$`)

type workflowExpressionContext struct {
	Entity                       map[string]any
	PlatformEntity               map[string]any
	Event                        map[string]any
	Payload                      map[string]any
	Policy                       map[string]any
	Accumulated                  any
	FanOut                       map[string]any
	WorkflowName                 string
	QueryEntityCount             func(string) (int, error)
	AllowUnresolvedQueryOperands bool
}

type workflowExpressionEvaluator struct {
	env      *cel.Env
	mu       sync.RWMutex
	programs map[string]cel.Program
}

func newWorkflowExpressionEvaluator() *workflowExpressionEvaluator {
	env, err := cel.NewEnv(
		cel.Variable("entity", cel.DynType),
		cel.Variable("_entity", cel.DynType),
		cel.Variable("event", cel.DynType),
		cel.Variable("payload", cel.DynType),
		cel.Variable("policy", cel.DynType),
		cel.Variable("accumulated", cel.DynType),
		cel.Variable("fan_out", cel.DynType),
		cel.Function("count_ge",
			cel.Overload(
				"count_ge_dyn_dyn",
				[]*cel.Type{cel.DynType, cel.DynType},
				cel.IntType,
				cel.FunctionBinding(workflowExpressionCountGE),
			),
		),
	)
	if err != nil {
		return &workflowExpressionEvaluator{}
	}
	return &workflowExpressionEvaluator{
		env:      env,
		programs: map[string]cel.Program{},
	}
}

func (e *workflowExpressionEvaluator) EvalBool(expression string, ctx workflowExpressionContext) (bool, error) {
	if e == nil || e.env == nil {
		return false, fmt.Errorf("workflow expression evaluator is not initialized")
	}
	normalized, normalizedCtx, err := normalizeWorkflowExpression(expression, ctx)
	if err != nil {
		return false, err
	}
	if normalized == "" {
		return false, fmt.Errorf("workflow expression is empty")
	}
	if missing := missingEntityReferences(normalized, normalizedCtx.Entity); len(missing) > 0 {
		return false, fmt.Errorf("entity field(s) unavailable in expression context: %s", strings.Join(missing, ", "))
	}
	program, err := e.program(normalized)
	if err != nil {
		return false, err
	}
	out, _, err := program.Eval(map[string]any{
		"entity":      workflowNormalizeCELInput(cloneStringAnyMap(normalizedCtx.Entity)),
		"_entity":     workflowNormalizeCELInput(cloneStringAnyMap(normalizedCtx.PlatformEntity)),
		"event":       workflowNormalizeCELInput(cloneStringAnyMap(normalizedCtx.Event)),
		"payload":     workflowNormalizeCELInput(cloneStringAnyMap(normalizedCtx.Payload)),
		"policy":      workflowNormalizeCELInput(cloneStringAnyMap(normalizedCtx.Policy)),
		"accumulated": normalizedCtx.Accumulated,
		"fan_out":     workflowNormalizeCELInput(cloneStringAnyMap(normalizedCtx.FanOut)),
	})
	if err != nil {
		return false, err
	}
	switch typed := out.(type) {
	case types.Bool:
		return bool(typed), nil
	default:
		return false, fmt.Errorf("workflow expression returned non-bool %T", out)
	}
}

func missingEntityReferences(expression string, entity map[string]any) []string {
	refs := WorkflowEntityReferences(expression)
	if len(refs) == 0 {
		return nil
	}
	guarded := WorkflowPresenceGuardedEntityFields(expression)
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if _, ok := guarded[WorkflowEntityReferenceField(ref)]; ok {
			continue
		}
		if _, ok := workflowExpressionLookupPath(entity, ref); ok {
			continue
		}
		out = append(out, "entity."+ref)
	}
	return out
}

func (e *workflowExpressionEvaluator) program(expression string) (cel.Program, error) {
	e.mu.RLock()
	if program, ok := e.programs[expression]; ok {
		e.mu.RUnlock()
		return program, nil
	}
	e.mu.RUnlock()

	ast, issues := e.env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	program, err := e.env.Program(ast)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	if e.programs == nil {
		e.programs = map[string]cel.Program{}
	}
	e.programs[expression] = program
	e.mu.Unlock()
	return program, nil
}

func normalizeWorkflowExpression(expression string, ctx workflowExpressionContext) (string, workflowExpressionContext, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return "", workflowExpressionContext{
			Entity:                       cloneStringAnyMap(ctx.Entity),
			PlatformEntity:               cloneStringAnyMap(ctx.PlatformEntity),
			Event:                        cloneStringAnyMap(ctx.Event),
			Payload:                      cloneStringAnyMap(ctx.Payload),
			Policy:                       cloneStringAnyMap(ctx.Policy),
			Accumulated:                  cloneAccumulatedItems(ctx.Accumulated),
			FanOut:                       cloneStringAnyMap(ctx.FanOut),
			WorkflowName:                 strings.TrimSpace(ctx.WorkflowName),
			QueryEntityCount:             ctx.QueryEntityCount,
			AllowUnresolvedQueryOperands: ctx.AllowUnresolvedQueryOperands,
		}, nil
	}
	if strings.EqualFold(expression, "else") {
		expression = "true"
	}
	normalized := workflowExpressionKeywordReplacer.ReplaceAllStringFunc(expression, func(token string) string {
		switch strings.ToUpper(strings.TrimSpace(token)) {
		case "AND":
			return "&&"
		case "OR":
			return "||"
		case "NOT":
			return "!"
		case "TRUE":
			return "true"
		case "FALSE":
			return "false"
		default:
			return token
		}
	})
	normalized = normalizeWorkflowExpressionStringLiterals(normalized)
	normalized = rewriteWorkflowExpressionEntityNullPresenceChecks(normalized)
	normalizedCtx := workflowExpressionContext{
		Entity:                       cloneStringAnyMap(ctx.Entity),
		PlatformEntity:               cloneStringAnyMap(ctx.PlatformEntity),
		Event:                        cloneStringAnyMap(ctx.Event),
		Payload:                      cloneStringAnyMap(ctx.Payload),
		Policy:                       cloneStringAnyMap(ctx.Policy),
		Accumulated:                  cloneAccumulatedItems(ctx.Accumulated),
		FanOut:                       cloneStringAnyMap(ctx.FanOut),
		WorkflowName:                 strings.TrimSpace(ctx.WorkflowName),
		QueryEntityCount:             ctx.QueryEntityCount,
		AllowUnresolvedQueryOperands: ctx.AllowUnresolvedQueryOperands,
	}
	normalized = workflowExpressionPolicyPlaceholder.ReplaceAllStringFunc(normalized, func(token string) string {
		match := workflowExpressionPolicyPlaceholder.FindStringSubmatch(token)
		if len(match) != 2 {
			return token
		}
		key := strings.TrimSpace(match[1])
		if value, ok := workflowExpressionPolicyValue(normalizedCtx.Policy, key); ok {
			return workflowExpressionLiteral(value)
		}
		if key != "" {
			return "policy." + key
		}
		return token
	})
	normalized = workflowExpressionCountGEPattern.ReplaceAllString(normalized, "count_ge($1, $2)")
	var err error
	normalized, err = rewriteWorkflowExpressionQueryEntityCounts(normalized, normalizedCtx)
	if err != nil {
		return "", workflowExpressionContext{}, err
	}
	return normalized, normalizedCtx, nil
}

func rewriteWorkflowExpressionEntityNullPresenceChecks(expression string) string {
	return workflowexpr.RewriteEntityNullPresenceChecks(expression)
}

func rewriteWorkflowExpressionQueryEntityCounts(expression string, ctx workflowExpressionContext) (string, error) {
	matches := workflowExpressionQueryEntitiesCountPattern.FindAllStringSubmatchIndex(expression, -1)
	if len(matches) == 0 {
		return expression, nil
	}
	var out strings.Builder
	last := 0
	for _, match := range matches {
		if len(match) < 4 {
			continue
		}
		start, end := match[0], match[1]
		predicate := strings.TrimSpace(expression[match[2]:match[3]])
		if _, err := parseWorkflowEntityQueryPredicate(predicate, ctx); err != nil {
			return "", err
		}
		count := 0
		if ctx.QueryEntityCount != nil {
			resolved, err := ctx.QueryEntityCount(predicate)
			if err != nil {
				return "", err
			}
			count = resolved
		}
		out.WriteString(expression[last:start])
		out.WriteString(strconv.Itoa(count))
		last = end
	}
	out.WriteString(expression[last:])
	return out.String(), nil
}

type workflowEntityQueryPredicate struct {
	Field string
	Op    string
	Value any
}

func parseWorkflowEntityQueryPredicate(predicate string, ctx workflowExpressionContext) (workflowEntityQueryPredicate, error) {
	match := workflowExpressionQueryPredicatePattern.FindStringSubmatch(strings.TrimSpace(predicate))
	if len(match) != 4 {
		return workflowEntityQueryPredicate{}, fmt.Errorf("unsupported query_entities predicate %q", predicate)
	}
	value, err := workflowExpressionResolveQueryOperand(strings.TrimSpace(match[3]), ctx)
	if err != nil {
		return workflowEntityQueryPredicate{}, err
	}
	return workflowEntityQueryPredicate{
		Field: strings.TrimSpace(match[1]),
		Op:    strings.TrimSpace(match[2]),
		Value: value,
	}, nil
}

func workflowExpressionResolveQueryOperand(raw string, ctx workflowExpressionContext) (any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("query_entities operand is empty")
	}
	if unquoted, err := strconv.Unquote(raw); err == nil {
		return unquoted, nil
	}
	switch strings.ToLower(raw) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	case "null":
		return nil, nil
	}
	if value, ok := workflowExpressionLookupContextValue(raw, ctx); ok {
		return value, nil
	}
	if parsed, err := strconv.ParseFloat(raw, 64); err == nil {
		return parsed, nil
	}
	if root, scoped := workflowExpressionQueryOperandScope(raw); scoped {
		switch root {
		case "entity", "_entity", "event", "payload", "policy", "fan_out":
			if ctx.AllowUnresolvedQueryOperands {
				return raw, nil
			}
			return nil, fmt.Errorf("query_entities operand %q is unavailable in expression context", raw)
		default:
			return nil, fmt.Errorf("unsupported query_entities operand scope %q in %q", root, raw)
		}
	}
	return raw, nil
}

func workflowExpressionQueryOperandScope(raw string) (string, bool) {
	root, _, ok := strings.Cut(strings.TrimSpace(raw), ".")
	if !ok {
		return "", false
	}
	root = strings.TrimSpace(root)
	if root == "" {
		return "", false
	}
	for i, ch := range root {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (i > 0 && ch >= '0' && ch <= '9') {
			continue
		}
		return "", false
	}
	return root, true
}

func workflowExpressionLookupContextValue(ref string, ctx workflowExpressionContext) (any, bool) {
	ref = strings.TrimSpace(ref)
	switch {
	case strings.HasPrefix(ref, "entity."):
		return workflowExpressionLookupPath(ctx.Entity, strings.TrimPrefix(ref, "entity."))
	case strings.HasPrefix(ref, "_entity."):
		return workflowExpressionLookupPath(ctx.PlatformEntity, strings.TrimPrefix(ref, "_entity."))
	case strings.HasPrefix(ref, "event."):
		return workflowExpressionLookupPath(ctx.Event, strings.TrimPrefix(ref, "event."))
	case strings.HasPrefix(ref, "payload."):
		return workflowExpressionLookupPath(ctx.Payload, strings.TrimPrefix(ref, "payload."))
	case strings.HasPrefix(ref, "policy."):
		return workflowExpressionLookupPath(ctx.Policy, strings.TrimPrefix(ref, "policy."))
	case strings.HasPrefix(ref, "fan_out."):
		return workflowExpressionLookupPath(ctx.FanOut, strings.TrimPrefix(ref, "fan_out."))
	default:
		return nil, false
	}
}

func cloneAccumulatedItems(value any) any {
	switch typed := value.(type) {
	case nil:
		return []any{}
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			if object, ok := item.(map[string]any); ok {
				out = append(out, cloneStringAnyMap(object))
				continue
			}
			out = append(out, item)
		}
		return out
	default:
		return typed
	}
}

func workflowNormalizeCELInput(value any) any {
	switch typed := value.(type) {
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, workflowNormalizeCELInput(item))
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = workflowNormalizeCELInput(item)
		}
		return out
	case float64:
		if math.Trunc(typed) == typed && typed <= math.MaxInt && typed >= math.MinInt {
			return int(typed)
		}
		return typed
	default:
		return typed
	}
}

func normalizeWorkflowExpressionStringLiterals(expression string) string {
	if expression == "" || !strings.ContainsRune(expression, '\'') {
		return expression
	}
	var out strings.Builder
	inSingle := false
	inDouble := false
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			out.WriteByte(ch)
			continue
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			out.WriteByte('"')
			continue
		}
		if ch == '\\' && i+1 < len(expression) {
			out.WriteByte(ch)
			i++
			out.WriteByte(expression[i])
			continue
		}
		out.WriteByte(ch)
	}
	return out.String()
}

func workflowExpressionCountGE(args ...ref.Val) ref.Val {
	if len(args) != 2 {
		return types.Int(0)
	}
	threshold, ok := workflowExpressionNumber(args[1])
	if !ok {
		return types.Int(0)
	}
	count := 0
	for _, item := range workflowExpressionListValues(args[0]) {
		if value, ok := workflowExpressionAnyNumber(item); ok && value >= threshold {
			count++
		}
	}
	return types.Int(count)
}

func workflowExpressionNumber(value ref.Val) (float64, bool) {
	if value == nil {
		return 0, false
	}
	return workflowExpressionAnyNumber(value.Value())
}

func workflowExpressionAnyNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case types.Int:
		return float64(typed), true
	case types.Uint:
		return float64(typed), true
	case types.Double:
		return float64(typed), true
	default:
		return 0, false
	}
}

func workflowExpressionListValues(value ref.Val) []any {
	if value == nil {
		return nil
	}
	switch typed := value.Value().(type) {
	case []any:
		return typed
	case []int:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	case []float64:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	default:
		return nil
	}
}

func workflowExpressionPolicyValue(policy map[string]any, key string) (any, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, false
	}
	value, ok := policy[key]
	return value, ok
}

func workflowExpressionLiteral(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case string:
		return strconv.Quote(typed)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(typed)
	case int8:
		return strconv.FormatInt(int64(typed), 10)
	case int16:
		return strconv.FormatInt(int64(typed), 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint:
		return strconv.FormatUint(uint64(typed), 10)
	case uint8:
		return strconv.FormatUint(uint64(typed), 10)
	case uint16:
		return strconv.FormatUint(uint64(typed), 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", value)
	}
}
