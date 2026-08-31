package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
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
	return ValidateConditionCELWithOptions(expression, context, workflowexpr.ValueExpressionOptions{})
}

func ValidateConditionCELWithOptions(expression string, context WorkflowConditionContext, opts workflowexpr.ValueExpressionOptions) error {
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
	if call := workflowConditionAmbientTimeCall(normalized); call != "" {
		return fmt.Errorf("workflow expression uses ambient time call %s(); use stage timers for time-driven behavior", call)
	}
	if workflowexpr.ExpressionReferencesRoot(normalized, "fan_out") {
		return fmt.Errorf("fan_out.* is unavailable in workflow conditions")
	}
	if context != WorkflowConditionContextRule && context != WorkflowConditionContextOnComplete &&
		workflowexpr.ExpressionReferencesRoot(normalized, "computed") {
		return fmt.Errorf("computed.* is only available in rule and on_complete conditions")
	}
	opts.RequireBool = true
	opts.AllowBareItem = context == WorkflowConditionContextFilter || context == WorkflowConditionContextCount
	opts.AllowAccumulated = context == WorkflowConditionContextOnComplete || opts.AllowBareItem
	return workflowexpr.ValidateValueExpressionWithOptions(normalized, opts)
}

func workflowConditionAmbientTimeCall(expression string) string {
	stripped := stripWorkflowExpressionStringLiterals(expression)
	for _, name := range []string{"now", "timestamp"} {
		for pos := 0; pos < len(stripped); {
			idx := strings.Index(stripped[pos:], name)
			if idx < 0 {
				break
			}
			start := pos + idx
			end := start + len(name)
			pos = end
			if start > 0 && isWorkflowConditionIdentifierPart(stripped[start-1]) {
				continue
			}
			if end < len(stripped) && isWorkflowConditionIdentifierPart(stripped[end]) {
				continue
			}
			next := skipWorkflowConditionWhitespace(stripped, end)
			if next < len(stripped) && stripped[next] == '(' {
				return name
			}
		}
	}
	return ""
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
	case WorkflowConditionContextRule, WorkflowConditionContextOnComplete:
		roots = append(roots, "computed")
	}
	switch context {
	case WorkflowConditionContextOnComplete:
		roots = append(roots, "accumulated")
	case WorkflowConditionContextFilter, WorkflowConditionContextCount:
		roots = append(roots, "accumulated", "item")
	}
	return roots
}
