package pipeline

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

var workflowExpressionKeywordReplacer = regexp.MustCompile(`\b(AND|OR|NOT|TRUE|FALSE)\b`)
var workflowExpressionPolicyPlaceholder = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}`)
var workflowExpressionCountGEPattern = regexp.MustCompile(`count\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*>=\s*([0-9]+(?:\.[0-9]+)?)\s*\)`)
var workflowExpressionStageRangeCountPattern = regexp.MustCompile(`count\(\s*entities\s+in\s+\[([a-zA-Z_][a-zA-Z0-9_]*)\.\.([a-zA-Z_][a-zA-Z0-9_]*)\]\s*\)`)
var workflowExpressionQueryEntitiesCountPattern = regexp.MustCompile(`query_entities\(\s*name\s*==\s*([a-zA-Z0-9_."-]+)\s*\)\.count`)

type workflowExpressionContext struct {
	Entity  map[string]any
	Payload map[string]any
	Policy  map[string]any
	Vars    map[string]any
}

type workflowExpressionEvaluator struct {
	env      *cel.Env
	mu       sync.RWMutex
	programs map[string]cel.Program
}

func newWorkflowExpressionEvaluator() *workflowExpressionEvaluator {
	env, err := cel.NewEnv(
		cel.Variable("entity", cel.DynType),
		cel.Variable("payload", cel.DynType),
		cel.Variable("policy", cel.DynType),
		cel.Variable("vars", cel.DynType),
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
	normalized, normalizedCtx := normalizeWorkflowExpression(expression, ctx)
	if normalized == "" {
		return false, fmt.Errorf("workflow expression is empty")
	}
	program, err := e.program(normalized)
	if err != nil {
		return false, err
	}
	out, _, err := program.Eval(map[string]any{
		"entity":  cloneStringAnyMap(normalizedCtx.Entity),
		"payload": cloneStringAnyMap(normalizedCtx.Payload),
		"policy":  cloneStringAnyMap(normalizedCtx.Policy),
		"vars":    cloneStringAnyMap(normalizedCtx.Vars),
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

func normalizeWorkflowExpression(expression string, ctx workflowExpressionContext) (string, workflowExpressionContext) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return "", workflowExpressionContext{
			Entity:  cloneStringAnyMap(ctx.Entity),
			Payload: cloneStringAnyMap(ctx.Payload),
			Policy:  cloneStringAnyMap(ctx.Policy),
			Vars:    cloneStringAnyMap(ctx.Vars),
		}
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
	normalizedCtx := workflowExpressionContext{
		Entity:  cloneStringAnyMap(ctx.Entity),
		Payload: cloneStringAnyMap(ctx.Payload),
		Policy:  cloneStringAnyMap(ctx.Policy),
		Vars:    workflowExpressionVars(ctx),
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
	normalized = rewriteWorkflowExpressionIdentifiers(normalized, normalizedCtx.Vars)
	return normalized, normalizedCtx
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

func workflowExpressionVars(ctx workflowExpressionContext) map[string]any {
	vars := cloneStringAnyMap(ctx.Vars)
	if vars == nil {
		vars = map[string]any{}
	}
	for key, value := range ctx.Policy {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, exists := vars[key]; !exists {
			vars[key] = value
		}
	}
	for key, value := range ctx.Entity {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		vars[key] = value
	}
	for key, value := range ctx.Payload {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		vars[key] = value
	}
	return vars
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

func rewriteWorkflowExpressionIdentifiers(expression string, vars map[string]any) string {
	if expression == "" {
		return expression
	}
	var out strings.Builder
	for idx := 0; idx < len(expression); {
		ch := rune(expression[idx])
		if !workflowExpressionIdentStart(ch) {
			out.WriteByte(expression[idx])
			idx++
			continue
		}
		start := idx
		idx++
		for idx < len(expression) && workflowExpressionIdentPart(rune(expression[idx])) {
			idx++
		}
		token := expression[start:idx]
		prev := workflowExpressionPrevNonSpace(expression, start-1)
		next := workflowExpressionNextNonSpace(expression, idx)
		if workflowExpressionShouldRewriteToken(token, prev, next) {
			if _, exists := vars[token]; !exists {
				vars[token] = token
			}
			out.WriteString("vars.")
			out.WriteString(token)
			continue
		}
		out.WriteString(token)
	}
	return out.String()
}

func workflowExpressionShouldRewriteToken(token string, prev rune, next rune) bool {
	switch strings.TrimSpace(token) {
	case "", "entity", "payload", "policy", "vars", "true", "false", "null":
		return false
	}
	if prev == '.' {
		return false
	}
	if next == '(' {
		return false
	}
	return true
}

func workflowExpressionIdentStart(ch rune) bool {
	return ch == '_' || unicode.IsLetter(ch)
}

func workflowExpressionIdentPart(ch rune) bool {
	return ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch)
}

func workflowExpressionPrevNonSpace(expression string, idx int) rune {
	for idx >= 0 {
		ch := rune(expression[idx])
		if !unicode.IsSpace(ch) {
			return ch
		}
		idx--
	}
	return 0
}

func workflowExpressionNextNonSpace(expression string, idx int) rune {
	for idx < len(expression) {
		ch := rune(expression[idx])
		if !unicode.IsSpace(ch) {
			return ch
		}
		idx++
	}
	return 0
}
