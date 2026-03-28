package pipeline

import (
	"fmt"
	"strings"
)

func ValidateConditionCEL(expression string) error {
	expression = strings.TrimSpace(expression)
	if expression == "" || strings.EqualFold(expression, "else") {
		return nil
	}
	evaluator := newWorkflowExpressionEvaluator()
	if evaluator == nil {
		return fmt.Errorf("workflow expression evaluator is not initialized")
	}
	normalized, _, err := normalizeWorkflowExpression(expression, workflowExpressionContext{})
	if err != nil {
		return err
	}
	if normalized == "" {
		return fmt.Errorf("workflow expression is empty")
	}
	_, err = evaluator.program(normalized)
	return err
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
		"gates[",
	} {
		if strings.Contains(expression, prefix) {
			return false
		}
	}
	return true
}
