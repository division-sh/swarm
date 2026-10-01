package workflowexpr

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestR3PolicyDirectSelectorPreparationParity(t *testing.T) {
	policies := []map[string]any{{}, {"obj": nil}, {"obj": true}, {"obj": "text"}, {"obj": map[string]any{}}, {"obj": map[string]any{"child": nil}}, {"obj": map[string]any{"child": []any{true}}}, {"obj": []any{true}}}
	selectors := []string{`.child`, `["child"]`, `[0]`, `[0u]`, `[0.0]`, `[0.5]`, `[9u]`, `[18446744073709551615u]`, `[true]`, `[null]`, `[b"child"]`, `.?child`, `[?"child"]`, `[?0]`, `[?0u]`, `[?0.0]`, `[?0.5]`, `[?9u]`, `[?18446744073709551615u]`, `[?true]`, `[?null]`, `[?b"child"]`}
	for i, policy := range policies {
		for _, prefix := range []string{`policy.obj`, `policy.?obj`} {
			for _, selector := range selectors {
				expression := prefix + selector
				if prefix == `policy.?obj` || strings.Contains(selector, "?") {
					expression += `.orValue(null)`
				}
				t.Run(fmt.Sprintf("%d/%s", i, expression), func(t *testing.T) {
					_, prepareErr := PrepareValueExpression(expression, ValueExpressionOptions{})
					validationErr := ValidateValueExpression(expression)
					if (prepareErr == nil) != (validationErr == nil) {
						t.Fatalf("preparation=%v validation=%v", prepareErr, validationErr)
					}
					_, runtimeErr := EvalValueExpression(expression, ValueContext{Policy: policy})
					declarationErr := ValidatePolicyReferences(expression, policy, ValueExpressionOptions{})
					if (runtimeErr == nil) != (declarationErr == nil) {
						t.Fatalf("execution=%v declaration=%v", runtimeErr, declarationErr)
					}
				})
			}
		}
	}
}

func TestR3PolicyProgramConstructionBeforePresenceExemptions(t *testing.T) {
	for _, expression := range []string{
		`policy.?obj[null].orValue(false)`,
		`policy.?obj[b"child"].orValue(false)`,
		`policy.?missing[?null].orValue(false)`,
		`policy.?missing[?b"child"].orValue(false)`,
		`policy.missing[null] == true`,
		`has(policy.missing) && policy.obj[null] == true`,
		`!has(policy.missing) || policy.obj[b"child"] == true`,
		`has(policy.missing) ? policy.obj[b"child"] == true : false`,
		`false && policy.obj[null] == true`,
		`true ? false : policy.obj[b"child"] == true`,
	} {
		t.Run(expression, func(t *testing.T) {
			policy := map[string]any{"obj": map[string]any{}}
			for _, opts := range []ValueExpressionOptions{{}, {DeclaredPolicy: policy}} {
				_, prepareErr := PrepareValueExpression(expression, opts)
				validationErr := ValidateValueExpressionWithOptions(expression, opts)
				for _, err := range []error{prepareErr, validationErr} {
					var missing *PolicyReferenceError
					if err == nil || errors.As(err, &missing) || !strings.Contains(err.Error(), "invalid qualifier type") {
						t.Fatalf("expected program-construction refusal before declaration analysis, got %v", err)
					}
				}
			}
		})
	}
}

func TestR3PolicyPreparationPresenceMutationAndScope(t *testing.T) {
	for _, expression := range []string{
		`has(policy.obj) && policy.obj.child == true`,
		`!has(policy.obj) || policy.obj.child == true`,
		`has(policy.obj) ? policy.obj.child == true : false`,
		`(has(policy.obj) && has(policy.obj.child)) ? policy.obj.child == true : false`,
		`!has(policy.obj) || (has(policy.obj.child) && policy.obj.child == true)`,
	} {
		for i, policy := range []map[string]any{{}, {"obj": map[string]any{}}, {"obj": map[string]any{"child": true}}, {}, {"obj": nil}} {
			t.Run(fmt.Sprintf("%s/%d", expression, i), func(t *testing.T) {
				_, runtimeErr := EvalValueExpression(expression, ValueContext{Policy: policy})
				declarationErr := ValidatePolicyReferences(expression, policy, ValueExpressionOptions{})
				if (runtimeErr == nil) != (declarationErr == nil) {
					t.Fatalf("execution=%v declaration=%v", runtimeErr, declarationErr)
				}
			})
		}
	}
	for _, expression := range []string{`[{}].all(policy, !has(policy.obj) || policy.obj.child == true)`, `[{}].all(policy, true) && policy.?missing.child.orValue(false)`} {
		if err := ValidatePolicyReferences(expression, map[string]any{"obj": true}, ValueExpressionOptions{}); err != nil {
			t.Errorf("lexical scope changed: %s: %v", expression, err)
		}
	}
}
