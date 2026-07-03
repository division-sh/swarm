package pipeline

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
)

type WorkflowConditionContext string

const (
	WorkflowConditionContextGuard      WorkflowConditionContext = "guard"
	WorkflowConditionContextRule       WorkflowConditionContext = "rule"
	WorkflowConditionContextOnComplete WorkflowConditionContext = "on_complete"
	WorkflowConditionContextFilter     WorkflowConditionContext = "filter"
	WorkflowConditionContextCount      WorkflowConditionContext = "count"
)

func ValidateConditionCEL(expression string, context WorkflowConditionContext) error {
	expression = strings.TrimSpace(expression)
	if expression == "" || strings.EqualFold(expression, "else") {
		return nil
	}
	normalized, _, err := normalizeWorkflowExpression(expression, workflowExpressionContext{AllowUnresolvedQueryOperands: true})
	if err != nil {
		return err
	}
	if normalized == "" {
		return fmt.Errorf("workflow expression is empty")
	}
	env, err := celValidationEnv(context)
	if err != nil {
		return err
	}
	_, issues := env.Compile(normalized)
	if issues != nil {
		return issues.Err()
	}
	return nil
}

func celValidationEnv(context WorkflowConditionContext) (*cel.Env, error) {
	options := []cel.EnvOption{
		cel.Variable("entity", cel.DynType),
		cel.Variable("_entity", cel.DynType),
		cel.Variable("event", cel.DynType),
		cel.Variable("payload", cel.DynType),
		cel.Variable("policy", cel.DynType),
		cel.Function("count_ge",
			cel.Overload(
				"count_ge_dyn_dyn",
				[]*cel.Type{cel.DynType, cel.DynType},
				cel.IntType,
				cel.FunctionBinding(workflowExpressionCountGE),
			),
		),
	}
	switch context {
	case WorkflowConditionContextOnComplete:
		options = append(options, cel.Variable("accumulated", cel.DynType))
	case WorkflowConditionContextFilter, WorkflowConditionContextCount:
		options = append(options,
			cel.Variable("accumulated", cel.DynType),
			cel.Variable("item", cel.DynType),
		)
	}
	return cel.NewEnv(options...)
}

func WorkflowConditionMissingRecognizedPrefix(expression string, context WorkflowConditionContext) bool {
	expression = strings.TrimSpace(expression)
	if expression == "" || strings.EqualFold(expression, "else") {
		return false
	}
	switch strings.ToLower(expression) {
	case "true", "false", "null":
		return false
	}
	for _, root := range workflowConditionRecognizedRoots(context) {
		if workflowConditionContainsRecognizedRoot(expression, root) {
			return false
		}
	}
	return true
}

func workflowConditionContainsRecognizedRoot(expression, root string) bool {
	root = strings.TrimSpace(root)
	if root == "" {
		return false
	}
	if root == "query_entities" {
		return strings.Contains(expression, "query_entities(")
	}
	for pos := 0; pos < len(expression); {
		idx := strings.Index(expression[pos:], root)
		if idx < 0 {
			return false
		}
		start := pos + idx
		end := start + len(root)
		pos = end
		if start > 0 && isWorkflowConditionIdentifierPart(expression[start-1]) {
			continue
		}
		if end < len(expression) && isWorkflowConditionIdentifierPart(expression[end]) {
			continue
		}
		next := skipWorkflowConditionWhitespace(expression, end)
		if next < len(expression) && (expression[next] == '.' || expression[next] == '[') {
			return true
		}
	}
	return false
}

func skipWorkflowConditionWhitespace(expression string, pos int) int {
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

func isWorkflowConditionIdentifierPart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_'
}

func workflowConditionRecognizedRoots(context WorkflowConditionContext) []string {
	roots := []string{
		"payload",
		"event",
		"entity",
		"_entity",
		"policy",
		"query_entities",
	}
	switch context {
	case WorkflowConditionContextOnComplete:
		roots = append(roots, "accumulated")
	case WorkflowConditionContextFilter, WorkflowConditionContextCount:
		roots = append(roots, "accumulated", "item")
	}
	return roots
}
