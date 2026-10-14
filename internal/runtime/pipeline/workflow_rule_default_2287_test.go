package pipeline

import (
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestRuleDefaultConditionOwnersKeepOrdinaryCELReferences(t *testing.T) {
	for _, expression := range []string{"else", "ELSE", "Else", " else "} {
		t.Run(expression, func(t *testing.T) {
			for _, context := range []WorkflowConditionContext{
				WorkflowConditionContextRule, WorkflowConditionContextOnComplete, WorkflowConditionContextGuard,
				WorkflowConditionContextFilter, WorkflowConditionContextCount, WorkflowConditionContextQueryFilter,
			} {
				err := ValidateConditionCEL(expression, context)
				if err == nil || !strings.Contains(err.Error(), strings.TrimSpace(expression)) || strings.Contains(err.Error(), "internal default marker") {
					t.Fatalf("%s must retain ordinary CEL refusal: %v", context, err)
				}
				if !WorkflowConditionMissingRecognizedPrefix(expression, context) {
					t.Fatalf("%s ordinary bare reference bypassed root admission", context)
				}
			}
			passed, err := newWorkflowExpressionEvaluator().EvalBool(expression, workflowExpressionContext{})
			if passed || err == nil {
				t.Fatalf("ordinary CEL reference was rewritten to true: passed=%t err=%v", passed, err)
			}
			readers := WorkflowHandlerExecutableReaders(nil, identity.ExecutableNode{}, "requested", c.SystemNodeEventHandler{
				Activity: c.ActivitySpec{Input: map[string]c.ExpressionValue{"value": c.CELExpression(expression)}},
				Guard:    &c.GuardSpec{Check: expression}, Rules: []c.HandlerRuleEntry{{Condition: expression}},
			})
			conditions := map[WorkflowConditionContext]int{}
			for _, reader := range readers {
				if reader.Expression != strings.TrimSpace(expression) {
					t.Fatalf("reader changed ordinary source: %#v", reader)
				}
				if reader.HasConditionContext {
					conditions[reader.ConditionContext]++
				}
			}
			if len(readers) != 3 || conditions[WorkflowConditionContextGuard] != 1 || conditions[WorkflowConditionContextRule] != 1 {
				t.Fatalf("reader census hid an ordinary expression: %#v", readers)
			}
		})
	}
}
