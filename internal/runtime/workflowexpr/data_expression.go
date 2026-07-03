package workflowexpr

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/events"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
)

var (
	dataExpressionEnvOnce             sync.Once
	dataExpressionEnv                 *cel.Env
	dataExpressionEnvErr              error
	dataExpressionWithBareItemEnvOnce sync.Once
	dataExpressionWithBareItemEnv     *cel.Env
	dataExpressionWithBareItemEnvErr  error

	workflowExpressionEntityReferencePattern         = regexp.MustCompile(`(^|[^a-zA-Z0-9_])entity\.([a-zA-Z_][a-zA-Z0-9_.]*)`)
	workflowExpressionPlatformEntityReferencePattern = regexp.MustCompile(`(^|[^a-zA-Z0-9_])_entity\.([a-zA-Z_][a-zA-Z0-9_.]*)`)
	workflowExpressionEventReferencePattern          = regexp.MustCompile(`(^|[^a-zA-Z0-9_])event\.([a-zA-Z_][a-zA-Z0-9_.]*)`)
	workflowExpressionEntityPresencePattern          = regexp.MustCompile(`["']([a-zA-Z_][a-zA-Z0-9_]*)["']\s+in\s+entity\b`)
	workflowExpressionEntityHasPattern               = regexp.MustCompile(`\bhas\s*\(\s*entity\.([a-zA-Z_][a-zA-Z0-9_.]*)\s*\)`)
	workflowExpressionEntityHasTernaryTruePattern    = regexp.MustCompile(`\bhas\s*\(\s*entity\.([a-zA-Z_][a-zA-Z0-9_.]*)\s*\)\s*\?\s*entity\.([a-zA-Z_][a-zA-Z0-9_.]*)`)
	workflowExpressionEntityHasTernaryFalsePattern   = regexp.MustCompile(`!\s*has\s*\(\s*entity\.([a-zA-Z_][a-zA-Z0-9_.]*)\s*\)\s*\?\s*[^:]+:\s*entity\.([a-zA-Z_][a-zA-Z0-9_.]*)`)
	workflowExpressionEntityNullCompareLeftPattern   = regexp.MustCompile(`\bentity\.([a-zA-Z_][a-zA-Z0-9_.]*)\s*(==|!=)\s*null\b`)
	workflowExpressionEntityNullCompareRightPattern  = regexp.MustCompile(`\bnull\s*(==|!=)\s*entity\.([a-zA-Z_][a-zA-Z0-9_.]*)\b`)
	workflowExpressionEntityNullNotEqualPattern      = regexp.MustCompile(`\bentity\.([a-zA-Z_][a-zA-Z0-9_.]*)\s*!=\s*null\b`)
	workflowExpressionEntityNullEqualPattern         = regexp.MustCompile(`\bentity\.([a-zA-Z_][a-zA-Z0-9_.]*)\s*==\s*null\b`)
	workflowExpressionNullEntityNotEqualPattern      = regexp.MustCompile(`\bnull\s*!=\s*entity\.([a-zA-Z_][a-zA-Z0-9_.]*)\b`)
	workflowExpressionNullEntityEqualPattern         = regexp.MustCompile(`\bnull\s*==\s*entity\.([a-zA-Z_][a-zA-Z0-9_.]*)\b`)
)

type ValueContext struct {
	Entity         map[string]any
	PlatformEntity map[string]any
	Event          map[string]any
	Payload        map[string]any
	Policy         map[string]any
	FanOut         map[string]any
}

type ValueExpressionOptions struct {
	AllowBareItem bool
}

func ValidateValueExpression(expression string) error {
	return ValidateValueExpressionWithOptions(expression, ValueExpressionOptions{})
}

func ValidateValueExpressionWithOptions(expression string, opts ValueExpressionOptions) error {
	env, err := dataExpressionEnvForContext(opts)
	if err != nil {
		return err
	}
	expression = strings.TrimSpace(RewriteEntityNullPresenceChecks(expression))
	if expression == "" {
		return fmt.Errorf("workflow data expression is empty")
	}
	if expressionReferencesRetiredFanOutTarget(expression) {
		return fmt.Errorf("fan_out.target is retired; use fan_out.item for per-item values or fan_out.count for fan-out count")
	}
	if err := ValidateEventReferences(expression); err != nil {
		return err
	}
	_, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return issues.Err()
	}
	return nil
}

func EvalValueExpression(expression string, ctx ValueContext) (any, error) {
	return EvalValueExpressionWithOptions(expression, ctx, ValueExpressionOptions{})
}

func EvalValueExpressionWithOptions(expression string, ctx ValueContext, opts ValueExpressionOptions) (any, error) {
	env, err := dataExpressionEnvForContext(opts)
	if err != nil {
		return nil, err
	}
	normalized := strings.TrimSpace(RewriteEntityNullPresenceChecks(expression))
	if normalized == "" {
		return nil, fmt.Errorf("workflow data expression is empty")
	}
	if expressionReferencesRetiredFanOutTarget(normalized) {
		return nil, fmt.Errorf("fan_out.target is retired; use fan_out.item for per-item values or fan_out.count for fan-out count")
	}
	if err := ValidateEventReferences(normalized); err != nil {
		return nil, err
	}
	if missing := MissingEntityReferences(normalized, ctx.Entity); len(missing) > 0 {
		return nil, fmt.Errorf("entity field(s) unavailable in expression context: %s", strings.Join(missing, ", "))
	}
	ast, issues := env.Compile(normalized)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	program, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	activation := map[string]any{
		"entity":  NormalizeCELInputMap(ctx.Entity),
		"_entity": NormalizeCELInputMap(ctx.PlatformEntity),
		"event":   NormalizeCELInputMap(ctx.Event),
		"payload": NormalizeCELInputMap(ctx.Payload),
		"policy":  NormalizeCELInputMap(ctx.Policy),
		"fan_out": NormalizeCELInputMap(ctx.FanOut),
	}
	if opts.AllowBareItem {
		activation["item"] = NormalizeCELValue(ctx.FanOut["item"])
	}
	out, _, err := program.Eval(activation)
	if err != nil {
		return nil, err
	}
	return NormalizeCELValue(out), nil
}

func ExpressionReferencesEntity(expression string) bool {
	return len(EntityReferences(expression)) > 0
}

func expressionReferencesRetiredFanOutTarget(expression string) bool {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return false
	}
	for i := 0; i < len(expression); i++ {
		switch expression[i] {
		case '\'', '"':
			next, ok := skipQuotedString(expression, i)
			if ok {
				i = next - 1
				continue
			}
		}
		if !strings.HasPrefix(expression[i:], "fan_out") {
			continue
		}
		if i > 0 && isIdentifierPart(expression[i-1]) {
			continue
		}
		pos := i + len("fan_out")
		if pos < len(expression) && isIdentifierPart(expression[pos]) {
			continue
		}
		pos = skipExpressionWhitespace(expression, pos)
		if pos >= len(expression) {
			continue
		}
		switch expression[pos] {
		case '.':
			pos = skipExpressionWhitespace(expression, pos+1)
			if strings.HasPrefix(expression[pos:], "target") {
				end := pos + len("target")
				if end >= len(expression) || !isIdentifierPart(expression[end]) {
					return true
				}
			}
		case '[':
			pos = skipExpressionWhitespace(expression, pos+1)
			value, next, ok := readQuotedString(expression, pos)
			if !ok {
				continue
			}
			next = skipExpressionWhitespace(expression, next)
			if next < len(expression) && expression[next] == ']' && value == "target" {
				return true
			}
		}
	}
	return false
}

func RewriteEntityNullPresenceChecks(expression string) string {
	return rewriteOutsideStringLiterals(expression, func(segment string) string {
		segment = workflowExpressionEntityNullNotEqualPattern.ReplaceAllString(segment, `has(entity.$1) && entity.$1 != null`)
		segment = workflowExpressionNullEntityNotEqualPattern.ReplaceAllString(segment, `has(entity.$1) && entity.$1 != null`)
		segment = workflowExpressionEntityNullEqualPattern.ReplaceAllString(segment, `!has(entity.$1) || entity.$1 == null`)
		segment = workflowExpressionNullEntityEqualPattern.ReplaceAllString(segment, `!has(entity.$1) || entity.$1 == null`)
		return segment
	})
}

func StripStringLiterals(expression string) string {
	if expression == "" {
		return ""
	}
	var out strings.Builder
	out.Grow(len(expression))
	inSingle := false
	inDouble := false
	escaped := false
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if escaped {
			if inSingle || inDouble {
				out.WriteByte(' ')
			} else {
				out.WriteByte(ch)
			}
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			if inSingle || inDouble {
				out.WriteByte(' ')
			} else {
				out.WriteByte(ch)
			}
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			out.WriteByte(' ')
			continue
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			out.WriteByte(' ')
			continue
		}
		if inSingle || inDouble {
			out.WriteByte(' ')
			continue
		}
		out.WriteByte(ch)
	}
	return out.String()
}

func skipQuotedString(expression string, start int) (int, bool) {
	_, next, ok := readQuotedString(expression, start)
	return next, ok
}

func readQuotedString(expression string, start int) (string, int, bool) {
	if start < 0 || start >= len(expression) {
		return "", start, false
	}
	quote := expression[start]
	if quote != '\'' && quote != '"' {
		return "", start, false
	}
	var out strings.Builder
	escaped := false
	for i := start + 1; i < len(expression); i++ {
		ch := expression[i]
		if escaped {
			out.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == quote {
			return out.String(), i + 1, true
		}
		out.WriteByte(ch)
	}
	return "", len(expression), false
}

func skipExpressionWhitespace(expression string, pos int) int {
	for pos < len(expression) {
		switch expression[pos] {
		case ' ', '\t', '\n', '\r':
			pos++
		default:
			return pos
		}
	}
	return pos
}

func isIdentifierPart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_'
}

func EntityReferences(expression string) []string {
	expression = strings.TrimSpace(StripStringLiterals(expression))
	if expression == "" {
		return nil
	}
	matches := workflowExpressionEntityReferencePattern.FindAllStringSubmatch(expression, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		ref := strings.TrimSpace(match[2])
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out
}

func PlatformEntityReferences(expression string) []string {
	expression = strings.TrimSpace(StripStringLiterals(expression))
	if expression == "" {
		return nil
	}
	matches := workflowExpressionPlatformEntityReferencePattern.FindAllStringSubmatch(expression, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		ref := strings.TrimSpace(match[2])
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out
}

func EventReferences(expression string) []string {
	expression = strings.TrimSpace(StripStringLiterals(expression))
	if expression == "" {
		return nil
	}
	matches := workflowExpressionEventReferencePattern.FindAllStringSubmatch(expression, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		ref := strings.TrimSpace(match[2])
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out
}

func ValidateEventReferences(expression string) error {
	invalid := InvalidEventReferences(expression)
	if len(invalid) == 0 {
		return nil
	}
	return fmt.Errorf("unsupported event context reference(s): %s", strings.Join(invalid, "; "))
}

func InvalidEventReferences(expression string) []string {
	refs := EventReferences(expression)
	if len(refs) == 0 {
		return nil
	}
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if err := events.ValidateEventContextReference(ref); err != nil {
			out = append(out, err.Error())
		}
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

func EntityReferenceField(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if idx := strings.IndexByte(ref, '.'); idx >= 0 {
		ref = ref[:idx]
	}
	return strings.TrimSpace(ref)
}

func PresenceGuardedEntityFields(expression string) map[string]struct{} {
	expression = strings.TrimSpace(StripStringLiterals(expression))
	if expression == "" {
		return nil
	}
	out := map[string]struct{}{}
	addField := func(field string) {
		field = EntityReferenceField(field)
		if field != "" {
			out[field] = struct{}{}
		}
	}
	for _, match := range workflowExpressionEntityPresencePattern.FindAllStringSubmatch(expression, -1) {
		if len(match) >= 2 {
			addField(match[1])
		}
	}
	for _, match := range workflowExpressionEntityHasPattern.FindAllStringSubmatch(expression, -1) {
		if len(match) >= 2 {
			addField(match[1])
		}
	}
	for _, match := range workflowExpressionEntityHasTernaryTruePattern.FindAllStringSubmatch(expression, -1) {
		if len(match) >= 3 && EntityReferenceField(match[1]) == EntityReferenceField(match[2]) {
			addField(match[1])
		}
	}
	for _, match := range workflowExpressionEntityHasTernaryFalsePattern.FindAllStringSubmatch(expression, -1) {
		if len(match) >= 3 && EntityReferenceField(match[1]) == EntityReferenceField(match[2]) {
			addField(match[1])
		}
	}
	for _, match := range workflowExpressionEntityNullCompareLeftPattern.FindAllStringSubmatch(expression, -1) {
		if len(match) >= 2 {
			addField(match[1])
		}
	}
	for _, match := range workflowExpressionEntityNullCompareRightPattern.FindAllStringSubmatch(expression, -1) {
		if len(match) >= 3 {
			addField(match[2])
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func MissingEntityReferences(expression string, entity map[string]any) []string {
	refs := EntityReferences(expression)
	if len(refs) == 0 {
		return nil
	}
	guarded := PresenceGuardedEntityFields(expression)
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		field := EntityReferenceField(ref)
		if field == "" {
			continue
		}
		if _, ok := guarded[field]; ok {
			continue
		}
		if _, ok := lookupPath(entity, ref); ok {
			continue
		}
		out = append(out, "entity."+ref)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

func NormalizeCELValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case ref.Val:
		return NormalizeCELValue(typed.Value())
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, NormalizeCELValue(item))
		}
		return out
	case []ref.Val:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, NormalizeCELValue(item))
		}
		return out
	case map[ref.Val]ref.Val:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[fmt.Sprint(NormalizeCELValue(key))] = NormalizeCELValue(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = NormalizeCELValue(item)
		}
		return out
	case float64:
		if math.Trunc(typed) == typed && typed <= math.MaxInt && typed >= math.MinInt {
			return int(typed)
		}
		return typed
	case int64:
		return int(typed)
	default:
		return typed
	}
}

func NormalizeCELInputMap(source map[string]any) map[string]any {
	if len(source) == 0 {
		return map[string]any{}
	}
	normalized, _ := NormalizeCELValue(cloneStringAnyMap(source)).(map[string]any)
	if normalized == nil {
		return map[string]any{}
	}
	return normalized
}

func dataExpressionEnvForContext(opts ValueExpressionOptions) (*cel.Env, error) {
	if opts.AllowBareItem {
		dataExpressionWithBareItemEnvOnce.Do(func() {
			dataExpressionWithBareItemEnv, dataExpressionWithBareItemEnvErr = newDataExpressionEnv(true)
		})
		return dataExpressionWithBareItemEnv, dataExpressionWithBareItemEnvErr
	}
	dataExpressionEnvOnce.Do(func() {
		dataExpressionEnv, dataExpressionEnvErr = newDataExpressionEnv(false)
	})
	return dataExpressionEnv, dataExpressionEnvErr
}

func newDataExpressionEnv(allowBareItem bool) (*cel.Env, error) {
	variables := []cel.EnvOption{
		cel.Variable("entity", cel.DynType),
		cel.Variable("_entity", cel.DynType),
		cel.Variable("event", cel.DynType),
		cel.Variable("payload", cel.DynType),
		cel.Variable("policy", cel.DynType),
		cel.Variable("fan_out", cel.DynType),
	}
	if allowBareItem {
		variables = append(variables, cel.Variable("item", cel.DynType))
	}
	return cel.NewEnv(variables...)
}

func rewriteOutsideStringLiterals(expression string, rewrite func(string) string) string {
	if expression == "" || rewrite == nil {
		return expression
	}
	var out strings.Builder
	segmentStart := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if ch == '\\' && i+1 < len(expression) {
			i++
			continue
		}
		if ch == '"' && !inSingle {
			if !inDouble {
				out.WriteString(rewrite(expression[segmentStart:i]))
				segmentStart = i
				inDouble = true
				continue
			}
			inDouble = false
			i++
			out.WriteString(expression[segmentStart:i])
			segmentStart = i
			i--
			continue
		}
		if ch == '\'' && !inDouble {
			if !inSingle {
				out.WriteString(rewrite(expression[segmentStart:i]))
				segmentStart = i
				inSingle = true
				continue
			}
			inSingle = false
			i++
			out.WriteString(expression[segmentStart:i])
			segmentStart = i
			i--
			continue
		}
	}
	if segmentStart < len(expression) {
		if inSingle || inDouble {
			out.WriteString(expression[segmentStart:])
		} else {
			out.WriteString(rewrite(expression[segmentStart:]))
		}
	}
	if segmentStart == 0 && out.Len() == 0 {
		return rewrite(expression)
	}
	return out.String()
}

func lookupPath(source map[string]any, path string) (any, bool) {
	path = strings.TrimSpace(path)
	if len(source) == 0 || path == "" {
		return nil, false
	}
	current := any(cloneStringAnyMap(source))
	for _, segment := range strings.Split(path, ".") {
		segment = strings.TrimSpace(segment)
		object, ok := current.(map[string]any)
		if !ok || segment == "" {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	return current, current != nil
}

func cloneStringAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = cloneValue(item)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, cloneValue(item))
		}
		return out
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	default:
		return typed
	}
}
