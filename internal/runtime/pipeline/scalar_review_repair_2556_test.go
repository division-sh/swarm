package pipeline

import "testing"

func TestScalar2556NonDefaultConditionValidationRejectsSentinel(t *testing.T) {
	for _, context := range []WorkflowConditionContext{WorkflowConditionContextGuard, WorkflowConditionContextFilter, WorkflowConditionContextCount, WorkflowConditionContextQueryFilter} {
		for _, expression := range []string{"else", "ELSE", "eLsE"} {
			if err := ValidateConditionCEL(expression, context); err == nil {
				t.Fatalf("%s admitted %q", context, expression)
			}
		}
		for _, expression := range []string{"true", "false"} {
			if err := ValidateConditionCEL(expression, context); err != nil {
				t.Fatalf("%s rejected %q: %v", context, expression, err)
			}
		}
	}
	for _, context := range []WorkflowConditionContext{WorkflowConditionContextRule, WorkflowConditionContextOnComplete} {
		if err := ValidateConditionCEL("else", context); err != nil {
			t.Fatalf("internal selection default changed: %v", err)
		}
	}
}
