package pipeline

import "testing"

func TestScalar2556EveryConditionContextUsesCanonicalCELValidation(t *testing.T) {
	for _, context := range []WorkflowConditionContext{
		WorkflowConditionContextGuard, WorkflowConditionContextFilter, WorkflowConditionContextCount,
		WorkflowConditionContextQueryFilter, WorkflowConditionContextRule, WorkflowConditionContextOnComplete,
	} {
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
}
