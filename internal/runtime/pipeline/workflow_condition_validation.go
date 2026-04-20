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
	normalized, _, err := normalizeWorkflowExpression(expression, workflowExpressionContext{})
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
	for _, prefix := range workflowConditionRecognizedPrefixes(context) {
		if strings.Contains(expression, prefix) {
			return false
		}
	}
	return true
}

func workflowConditionRecognizedPrefixes(context WorkflowConditionContext) []string {
	prefixes := []string{
		"payload.",
		"event.",
		"entity.",
		"policy.",
		"query_entities(",
	}
	switch context {
	case WorkflowConditionContextOnComplete:
		prefixes = append(prefixes, "accumulated.")
	case WorkflowConditionContextFilter, WorkflowConditionContextCount:
		prefixes = append(prefixes, "accumulated.", "item.")
	}
	return prefixes
}
