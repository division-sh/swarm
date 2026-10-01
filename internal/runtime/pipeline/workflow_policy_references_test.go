package pipeline

import (
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func TestR3ConditionPolicyQueryAndPlaceholderReferences(t *testing.T) {
	policy := map[string]any{"limit": 3, "a.b": 3, "1": 3}
	for _, tc := range []struct {
		expression string
		missing    bool
	}{
		{"query_entities(value == policy.limit).count > 0", false},
		{"query_entities(value == policy.missing).count > 0", true},
		{`query_entities(value == 'policy.missing').count > 0`, false},
		{"{{a.b}} > 0", false},
		{"{{1}} > 0", false},
		{"{{missing}} > 0", true},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			err := ValidateConditionCELWithOptions(tc.expression, WorkflowConditionContextGuard, workflowexpr.ValueExpressionOptions{DeclaredPolicy: policy})
			var missing *workflowexpr.PolicyReferenceError
			if errors.As(err, &missing) != tc.missing || (!tc.missing && err != nil) {
				t.Fatalf("missing=%t: %v", tc.missing, err)
			}
		})
	}
	value, err := newWorkflowExpressionEvaluator().EvalBool("{{a.b}} == 3", workflowExpressionContext{Policy: policy})
	if err != nil || !value {
		t.Fatalf("exact placeholder execution=%t: %v", value, err)
	}
	if got := WorkflowTimerPolicyReferences("{{a.b}}s"); len(got) != 1 || got[0] != "a.b" {
		t.Fatalf("timer keys=%v", got)
	}
}
