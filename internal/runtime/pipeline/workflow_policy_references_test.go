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
		{`query_entities(value == policy["a.b"]).count > 0`, false},
		{`query_entities(value == policy["missing"]).count > 0`, true},
		{"query_entities(value == {{missing}}).count > 0", true},
		{"query_entities(value == {{a.b}}).count > 0", false},
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

func TestR3PolicyQueryOperandExecution(t *testing.T) {
	policy := map[string]any{"a.b": 3, "obj": map[string]any{"key": 4}, "null": nil}
	for _, tc := range []struct {
		expression string
		want       any
	}{{`policy["a.b"]`, int64(3)}, {`policy.obj.key`, int64(4)}, {`policy["null"]`, nil}} {
		value, err := workflowExpressionResolveQueryOperand(tc.expression, workflowExpressionContext{Policy: policy})
		if err != nil || value != tc.want {
			t.Fatalf("%s: value=%#v want=%#v err=%v", tc.expression, value, tc.want, err)
		}
	}
	for _, expression := range []string{`policy["missing"]`, `policy.missing`, `policy["a.b"] + 1`, `policy[payload.key]`, `policy.?null`} {
		if _, err := workflowExpressionResolveQueryOperand(expression, workflowExpressionContext{Policy: policy}); err == nil {
			t.Fatalf("hostile query operand accepted: %s", expression)
		}
	}
	called := 0
	for _, expression := range []string{`query_entities(value == policy["a.b"]).count == 1`, `query_entities(value == {{a.b}}).count == 1`} {
		ok, err := newWorkflowExpressionEvaluator().EvalBool(expression, workflowExpressionContext{
			Policy: policy,
			QueryEntityCount: func(predicate string) (int, error) {
				parsed, err := parseWorkflowEntityQueryPredicate(predicate, workflowExpressionContext{Policy: policy})
				if err != nil || (parsed.Value != int64(3) && parsed.Value != float64(3)) {
					t.Fatalf("query callback operand=%#v err=%v", parsed.Value, err)
				}
				called++
				return 1, nil
			},
		})
		if !ok || err != nil {
			t.Fatalf("%s: %t %v", expression, ok, err)
		}
	}
	if called != 2 {
		t.Fatalf("query callbacks=%d, want 2", called)
	}
	_, err := newWorkflowExpressionEvaluator().EvalBool(`query_entities(value == {{missing}}).count == 0`, workflowExpressionContext{
		Policy: policy, QueryEntityCount: func(string) (int, error) { t.Fatal("missing policy reached query"); return 0, nil },
	})
	if err == nil {
		t.Fatal("missing placeholder became a query literal")
	}
}
