package pipeline

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
)

func ValidateConditionCEL(expression string) error {
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
	env, err := celValidationEnv()
	if err != nil {
		return err
	}
	_, issues := env.Compile(normalized)
	if issues != nil {
		return issues.Err()
	}
	return nil
}

func celValidationEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("entity", cel.DynType),
		cel.Variable("payload", cel.DynType),
		cel.Variable("policy", cel.DynType),
		cel.Variable("accumulated", cel.DynType),
		cel.Variable("fan_out", cel.DynType),
		cel.Variable("item", cel.DynType),
		cel.Function("count_ge",
			cel.Overload(
				"count_ge_dyn_dyn",
				[]*cel.Type{cel.DynType, cel.DynType},
				cel.IntType,
				cel.FunctionBinding(workflowExpressionCountGE),
			),
		),
	)
}

func WorkflowConditionMissingRecognizedPrefix(expression string) bool {
	expression = strings.TrimSpace(expression)
	if expression == "" || strings.EqualFold(expression, "else") {
		return false
	}
	switch strings.ToLower(expression) {
	case "true", "false", "null":
		return false
	}
	for _, prefix := range []string{
		"payload.",
		"entity.",
		"policy.",
		"accumulated.",
		"fan_out.",
		"item.",
		"query_entities(",
	} {
		if strings.Contains(expression, prefix) {
			return false
		}
	}
	return true
}
