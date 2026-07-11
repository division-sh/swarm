package workflowexpr

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
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
	Computed       map[string]any
	FanOut         map[string]any
	Join           map[string]any
}

type ValueExpressionOptions struct {
	AllowBareItem bool
	ItemAlias     string
	AllowJoin     bool
	RequireBool   bool
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
	if expressionReferencesFanOutField(expression, "target") {
		return fmt.Errorf("fan_out.target is retired; use the current fan_out emit item alias for per-item values or fan_out.count for fan-out count")
	}
	if expressionReferencesFanOutField(expression, "identity") {
		return fmt.Errorf("fan_out.identity is not supported; use the declared fan_out identity expression directly through the item alias")
	}
	if expressionReferencesFanOutField(expression, "item") {
		return fmt.Errorf("fan_out.item is retired from authored fan_out expressions; use the required fan_out item alias")
	}
	if strings.TrimSpace(opts.ItemAlias) == "" && expressionReferencesFanOutField(expression, "index") {
		return fmt.Errorf("fan_out.index is only available inside fan_out.emit fields")
	}
	if !opts.AllowJoin && ExpressionReferencesRoot(expression, "join") {
		return fmt.Errorf("join.* is only available inside join completion and timeout outcomes")
	}
	if err := ValidateEventReferences(expression); err != nil {
		return err
	}
	compiled, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return issues.Err()
	}
	if opts.AllowJoin {
		if err := validateJoinAccesses(compiled); err != nil {
			return err
		}
	}
	if opts.RequireBool && compiled.OutputType() != cel.BoolType {
		return fmt.Errorf("workflow expression must return bool, got %s", compiled.OutputType())
	}
	return nil
}

func validateJoinAccesses(compiled *cel.Ast) error {
	if compiled == nil || compiled.NativeRep() == nil {
		return fmt.Errorf("workflow expression AST is unavailable")
	}
	allowed := make(map[string]struct{}, len(joinruntime.SupportedContextFields()))
	for _, field := range joinruntime.SupportedContextFields() {
		allowed[field] = struct{}{}
	}
	root := celast.NavigateAST(compiled.NativeRep())
	var visit func(celast.NavigableExpr) error
	visit = func(expr celast.NavigableExpr) error {
		if expr.Kind() == celast.IdentKind && expr.AsIdent() == "join" {
			parent, ok := expr.Parent()
			if !ok {
				return fmt.Errorf("join must be accessed as join.<field>")
			}
			switch parent.Kind() {
			case celast.SelectKind:
				selection := parent.AsSelect()
				if selection.Operand().ID() != expr.ID() {
					return fmt.Errorf("join must be accessed as join.<field>")
				}
				field := strings.TrimSpace(selection.FieldName())
				if _, ok := allowed[field]; !ok {
					return fmt.Errorf("unsupported join.%s", field)
				}
			case celast.CallKind:
				call := parent.AsCall()
				args := call.Args()
				if call.FunctionName() == "_[_]" && len(args) > 0 && args[0].ID() == expr.ID() {
					return fmt.Errorf("bracket access on join is unsupported; use join.<field>")
				}
				return fmt.Errorf("join must be accessed as join.<field>")
			default:
				return fmt.Errorf("join must be accessed as join.<field>")
			}
		}
		for _, child := range expr.Children() {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
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
	if expressionReferencesFanOutField(normalized, "target") {
		return nil, fmt.Errorf("fan_out.target is retired; use the current fan_out emit item alias for per-item values or fan_out.count for fan-out count")
	}
	if expressionReferencesFanOutField(normalized, "identity") {
		return nil, fmt.Errorf("fan_out.identity is not supported; use the declared fan_out identity expression directly through the item alias")
	}
	if expressionReferencesFanOutField(normalized, "item") {
		return nil, fmt.Errorf("fan_out.item is retired from authored fan_out expressions; use the required fan_out item alias")
	}
	if strings.TrimSpace(opts.ItemAlias) == "" && expressionReferencesFanOutField(normalized, "index") {
		return nil, fmt.Errorf("fan_out.index is only available inside fan_out.emit fields")
	}
	if !opts.AllowJoin && ExpressionReferencesRoot(normalized, "join") {
		return nil, fmt.Errorf("join.* is only available inside join completion and timeout outcomes")
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
		"entity":   NormalizeCELInputMap(ctx.Entity),
		"_entity":  NormalizeCELInputMap(ctx.PlatformEntity),
		"event":    NormalizeCELInputMap(ctx.Event),
		"payload":  NormalizeCELInputMap(ctx.Payload),
		"policy":   NormalizeCELInputMap(ctx.Policy),
		"computed": NormalizeCELInputMap(ctx.Computed),
		"fan_out":  NormalizeCELInputMap(ctx.FanOut),
		"join":     NormalizeCELInputMap(ctx.Join),
	}
	if opts.AllowBareItem {
		activation["item"] = NormalizeCELValue(ctx.FanOut["item"])
	}
	if alias := strings.TrimSpace(opts.ItemAlias); alias != "" {
		activation[alias] = NormalizeCELValue(ctx.FanOut["item"])
	}
	out, _, err := program.Eval(activation)
	if err != nil {
		return nil, err
	}
	return NormalizeCELValue(out), nil
}

func ExpressionReferencesRoot(expression, root string) bool {
	expression = StripStringLiterals(strings.TrimSpace(expression))
	root = strings.TrimSpace(root)
	if expression == "" || root == "" {
		return false
	}
	for i := 0; i < len(expression); i++ {
		if !strings.HasPrefix(expression[i:], root) {
			continue
		}
		if i > 0 && isIdentifierPart(expression[i-1]) {
			continue
		}
		end := i + len(root)
		if end < len(expression) && isIdentifierPart(expression[end]) {
			continue
		}
		end = skipExpressionWhitespace(expression, end)
		if end < len(expression) && (expression[end] == '.' || expression[end] == '[') {
			return true
		}
	}
	return false
}

func ExpressionReferencesEntity(expression string) bool {
	return len(EntityReferences(expression)) > 0
}

func ExpressionReferencesFanOutFieldForValidation(expression, field string) bool {
	return expressionReferencesFanOutField(expression, field)
}

func expressionReferencesFanOutField(expression, field string) bool {
	expression = strings.TrimSpace(expression)
	field = strings.TrimSpace(field)
	if expression == "" || field == "" {
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
			if strings.HasPrefix(expression[pos:], field) {
				end := pos + len(field)
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
			if next < len(expression) && expression[next] == ']' && value == field {
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

func isIdentifierStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		ch == '_'
}

func isRootReferenceStart(expression string, start int) bool {
	if start <= 0 {
		return true
	}
	for pos := start - 1; pos >= 0; pos-- {
		switch expression[pos] {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return !isIdentifierPart(expression[pos]) && expression[pos] != '.'
		}
	}
	return true
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
	refs, _ := scanEventReferences(expression)
	return refs
}

func scanEventReferences(expression string) ([]string, []string) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil, nil
	}
	out := []string{}
	invalid := []string{}
	seen := map[string]struct{}{}
	seenInvalid := map[string]struct{}{}
	addRef := func(ref string) {
		ref = strings.Trim(strings.TrimSpace(ref), ".")
		if ref == "" {
			return
		}
		if _, ok := seen[ref]; ok {
			return
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	addInvalid := func(message string) {
		message = strings.TrimSpace(message)
		if message == "" {
			return
		}
		if _, ok := seenInvalid[message]; ok {
			return
		}
		seenInvalid[message] = struct{}{}
		invalid = append(invalid, message)
	}
	for pos := 0; pos < len(expression); {
		switch expression[pos] {
		case '\'', '"':
			next, ok := skipQuotedString(expression, pos)
			if ok {
				pos = next
				continue
			}
		}
		if !strings.HasPrefix(expression[pos:], events.EventContextRoot) {
			pos++
			continue
		}
		rootStart := pos
		rootEnd := pos + len(events.EventContextRoot)
		pos = rootEnd
		if !isRootReferenceStart(expression, rootStart) {
			continue
		}
		if rootEnd < len(expression) && isIdentifierPart(expression[rootEnd]) {
			continue
		}
		next := skipExpressionWhitespace(expression, rootEnd)
		if next >= len(expression) || (expression[next] != '.' && expression[next] != '[') {
			continue
		}
		ref, next, invalidMessage, ok := readEventReferenceAfterRoot(expression, next)
		if invalidMessage != "" {
			addInvalid(invalidMessage)
		}
		if ok {
			addRef(ref)
		}
		if next > pos {
			pos = next
		}
	}
	return out, invalid
}

func readEventReferenceAfterRoot(expression string, pos int) (string, int, string, bool) {
	segments := []string{}
	for {
		pos = skipExpressionWhitespace(expression, pos)
		if pos >= len(expression) {
			break
		}
		switch expression[pos] {
		case '.':
			pos = skipExpressionWhitespace(expression, pos+1)
			if pos >= len(expression) || !isIdentifierStart(expression[pos]) {
				return strings.Join(segments, "."), pos, "", len(segments) > 0
			}
			start := pos
			pos++
			for pos < len(expression) && isIdentifierPart(expression[pos]) {
				pos++
			}
			segments = append(segments, expression[start:pos])
		case '[':
			segment, next, invalid, ok := readEventBracketSegment(expression, pos)
			if invalid != "" {
				return "", next, invalid, false
			}
			if !ok {
				return strings.Join(segments, "."), next, "", len(segments) > 0
			}
			segments = append(segments, segment)
			pos = next
		default:
			return strings.Join(segments, "."), pos, "", len(segments) > 0
		}
	}
	return strings.Join(segments, "."), pos, "", len(segments) > 0
}

func readEventBracketSegment(expression string, pos int) (string, int, string, bool) {
	start := pos
	pos = skipExpressionWhitespace(expression, pos+1)
	if pos >= len(expression) {
		return "", len(expression), "event[...] field access is malformed", false
	}
	if expression[pos] != '\'' && expression[pos] != '"' {
		return "", skipBracketExpression(expression, start), "event[...] dynamic field access is unsupported on handler expression surfaces; use literal event field names so unsupported fields fail closed", false
	}
	value, next, ok := readQuotedString(expression, pos)
	if !ok {
		return "", len(expression), "event[...] field access is malformed", false
	}
	next = skipExpressionWhitespace(expression, next)
	if next >= len(expression) || expression[next] != ']' {
		return "", next, "event[...] field access is malformed", false
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", next + 1, `event[""] is not a supported handler event context field`, false
	}
	if strings.Contains(value, ".") {
		return "", next + 1, fmt.Sprintf("event[%q] is not a supported handler event context field; use dotted or nested bracket route fields instead", value), false
	}
	return value, next + 1, "", true
}

func skipBracketExpression(expression string, start int) int {
	if start < 0 || start >= len(expression) || expression[start] != '[' {
		return start
	}
	depth := 0
	for pos := start; pos < len(expression); pos++ {
		switch expression[pos] {
		case '\'', '"':
			next, ok := skipQuotedString(expression, pos)
			if ok {
				pos = next - 1
			}
		case '[':
			depth++
		case ']':
			depth--
			if depth <= 0 {
				return pos + 1
			}
		}
	}
	return len(expression)
}

func ValidateEventReferences(expression string) error {
	invalid := InvalidEventReferences(expression)
	if len(invalid) == 0 {
		return nil
	}
	return fmt.Errorf("unsupported event context reference(s): %s", strings.Join(invalid, "; "))
}

func InvalidEventReferences(expression string) []string {
	refs, invalid := scanEventReferences(expression)
	out := make([]string, 0, len(refs)+len(invalid))
	out = append(out, invalid...)
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
	if strings.TrimSpace(opts.ItemAlias) != "" {
		return newDataExpressionEnv(false, strings.TrimSpace(opts.ItemAlias))
	}
	if opts.AllowBareItem {
		dataExpressionWithBareItemEnvOnce.Do(func() {
			dataExpressionWithBareItemEnv, dataExpressionWithBareItemEnvErr = newDataExpressionEnv(true, "")
		})
		return dataExpressionWithBareItemEnv, dataExpressionWithBareItemEnvErr
	}
	dataExpressionEnvOnce.Do(func() {
		dataExpressionEnv, dataExpressionEnvErr = newDataExpressionEnv(false, "")
	})
	return dataExpressionEnv, dataExpressionEnvErr
}

func newDataExpressionEnv(allowBareItem bool, itemAlias string) (*cel.Env, error) {
	variables := []cel.EnvOption{
		cel.Variable("entity", cel.DynType),
		cel.Variable("_entity", cel.DynType),
		cel.Variable("event", cel.DynType),
		cel.Variable("payload", cel.DynType),
		cel.Variable("policy", cel.DynType),
		cel.Variable("computed", cel.DynType),
		cel.Variable("fan_out", cel.DynType),
		cel.Variable("join", cel.DynType),
	}
	if allowBareItem {
		variables = append(variables, cel.Variable("item", cel.DynType))
	}
	if itemAlias = strings.TrimSpace(itemAlias); itemAlias != "" {
		variables = append(variables, cel.Variable(itemAlias, cel.DynType))
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
