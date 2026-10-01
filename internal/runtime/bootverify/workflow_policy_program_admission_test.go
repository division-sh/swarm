package bootverify

import (
	"context"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func TestR3PolicyProgramConstructionFullBoot(t *testing.T) {
	for _, tc := range []struct {
		expression string
		invalid    bool
		want       bool
	}{
		{`policy.?obj[null].orValue(false)`, true, false},
		{`policy.?obj[b"child"].orValue(false)`, true, false},
		{`has(policy.missing) && policy.obj[null] == true`, true, false},
		{`has(policy.missing) ? policy.obj[b"child"] == true : false`, true, false},
		{`policy.?missing.child.orValue(false)`, false, false},
		{`policy.list[0u] == "yes"`, false, true},
	} {
		expression := tc.expression
		for _, reader := range []string{"guard", "emit"} {
			t.Run(reader+"/"+expression, func(t *testing.T) {
				policy := map[string]any{"obj": map[string]any{}, "list": []any{"yes"}}
				bundle := semanticview.CloneBundleForPreview(loadTier8FixtureBundle(t, "test-boot-success"), policy)
				node, event, handler, ok := firstBundleHandler(bundle)
				if !ok {
					t.Fatal("missing handler")
				}
				check := "condition_expression_validation"
				if reader == "guard" {
					handler.Guard = &c.GuardSpec{Check: expression}
				} else {
					check = "emit_field_expression_validation"
					bundle.Events["work.result"] = c.EventCatalogEntry{Payload: c.EventPayloadSpec{Properties: map[string]c.EventFieldSpec{"accepted": {Type: "bool"}}, Required: []string{"accepted"}}}
					entry := bundle.Nodes[node.Key()]
					entry.Produces = []string{"work.result"}
					bundle.Nodes[node.Key()] = entry
					handler.Emit = c.EmitSpec{Event: "work.result", Fields: map[string]c.ExpressionValue{"accepted": c.CELExpression(expression)}}
				}
				writeBundleHandler(t, bundle, node, event, handler)
				failures := Run(context.Background(), semanticview.Wrap(bundle), Options{}).Errors()
				_, prepareErr := workflowexpr.PrepareValueExpression(expression, workflowexpr.ValueExpressionOptions{})
				value, runtimeErr := workflowexpr.EvalValueExpression(expression, workflowexpr.ValueContext{Policy: policy})
				if !tc.invalid {
					if prepareErr != nil || runtimeErr != nil || value != tc.want || len(failures) != 0 {
						t.Fatalf("positive control: preparation=%v execution=%v value=%v boot=%#v", prepareErr, runtimeErr, value, failures)
					}
					return
				}
				if prepareErr == nil || runtimeErr == nil || !strings.Contains(prepareErr.Error(), "invalid qualifier type") {
					t.Fatalf("preparation witness changed: %v, %v", prepareErr, runtimeErr)
				}
				if len(failures) != 1 || failures[0].CheckID != check || failures[0].Severity != SeverityHardInvalidity || !strings.Contains(failures[0].Message, "invalid qualifier type") || !strings.Contains(failures[0].Message, event) {
					t.Fatalf("preparation refusal lost at supported reader: %#v", failures)
				}
			})
		}
	}
}
