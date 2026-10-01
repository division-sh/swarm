package workflowexpr

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestR3PolicyCheckedReferences(t *testing.T) {
	policy := map[string]any{"limit": int64(3), "config": map[string]any{"name": "ok"}, "null": nil, "a.b": true, "": true, " spaced ": true, "list": []any{"yes"}}
	payload := c.ResolvedCatalogType{Kind: c.CatalogTypeObject, Fields: []c.ResolvedCatalogField{{Name: "key", Type: c.ResolvedCatalogType{Kind: c.CatalogTypeText}}}}
	for _, tc := range []struct {
		expression string
		missing    []string
		admission  bool
	}{
		{expression: `policy.limit > 1`},
		{expression: `policy.missing > 1`, missing: []string{`policy["missing"]`}},
		{expression: `policy["missing key"]`, missing: []string{`policy["missing key"]`}},
		{expression: `policy.config.missing`, missing: []string{`policy["config"]["missing"]`}},
		{expression: `policy["a.b"] && policy[""] && policy[" spaced "]`},
		{expression: `policy["null"] == null`},
		{expression: `"policy.missing" == "policy.missing"`},
		{expression: `[{}].all(policy, has(policy.missing))`},
		{expression: `[{}].all(policy, policy.missing == null)`},
		{expression: `has(policy.missing) && policy.missing > 1`},
		{expression: `!has(policy.missing) || policy.missing > 1`},
		{expression: `has(policy.missing) ? policy.missing : 1`},
		{expression: `policy.?missing.orValue(1)`},
		{expression: `policy[?"missing"].orValue(1)`},
		{expression: `policy[?payload.key].orValue(false)`},
		{expression: `policy[payload.key]`, admission: true},
		{expression: `[{}].all(policy, true) && policy.missing > 1`, missing: []string{`policy["missing"]`}},
		{expression: `policy.list[0] == "yes"`},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			err := ValidatePolicyReferences(tc.expression, policy, ValueExpressionOptions{PayloadType: &payload})
			var missing *PolicyReferenceError
			if errors.As(err, &missing) {
				if !reflect.DeepEqual(missing.Paths, tc.missing) {
					t.Fatalf("missing=%v want=%v", missing.Paths, tc.missing)
				}
			} else if tc.admission {
				if err == nil {
					t.Fatal("expected existing structural rejection")
				}
			} else if err != nil || len(tc.missing) != 0 {
				t.Fatalf("error=%v want missing=%v", err, tc.missing)
			}
		})
	}
}

func TestR3PolicyOptionalAndGuardedDescendantParity(t *testing.T) {
	for _, tc := range []struct {
		name, expression string
		policy           map[string]any
		want             bool
		invalid          bool
	}{
		{"optional absent dot", `policy.?missing.child.orValue(false)`, nil, false, false},
		{"optional absent indexes", `policy[?"missing"]["child"].orValue(false)`, nil, false, false},
		{"optional absent double lookup", `policy[?"missing"][?"child"].orValue(false)`, nil, false, false},
		{"optional present dot", `policy.?config.child.orValue(false)`, map[string]any{"config": map[string]any{"child": true}}, true, false},
		{"optional present missing child", `policy.?config.child.orValue(false)`, map[string]any{"config": map[string]any{}}, false, false},
		{"optional later child", `policy.config.?child.orValue(false)`, map[string]any{"config": map[string]any{}}, false, false},
		{"optional does not protect required prefix", `policy.missing.?child.orValue(false)`, nil, false, true},
		{"optional null child", `policy.?config.child.orValue(null) == null`, map[string]any{"config": map[string]any{"child": nil}}, true, false},
		{"optional null parent", `policy.?config.child.orValue(false)`, map[string]any{"config": nil}, false, false},
		{"optional wrong parent shape", `policy.?config.child.orValue(false)`, map[string]any{"config": true}, false, false},
		{"AND absent parent", `has(policy.missing) && policy.missing.child == true`, nil, false, false},
		{"OR absent parent", `!has(policy.missing) || policy.missing.child == true`, nil, true, false},
		{"OR does not guard absent parent", `has(policy.missing) || policy.missing.child == true`, nil, false, true},
		{"negated AND does not guard absent parent", `!has(policy.missing) && policy.missing.child == true`, nil, false, true},
		{"conditional absent then", `has(policy.missing) ? policy.missing.child == true : false`, nil, false, false},
		{"conditional absent else", `!has(policy.missing) ? false : policy.missing.child == true`, nil, false, false},
		{"nested AND absent", `has(policy.missing) && has(policy.missing.child) && policy.missing.child.value == true`, nil, false, false},
		{"nested OR absent", `!has(policy.missing) || !has(policy.missing.child) || policy.missing.child.value == true`, nil, true, false},
		{"compound condition absent", `(has(policy.missing) && has(policy.missing.child)) ? policy.missing.child.value == true : false`, nil, false, false},
		{"present parent missing required child", `has(policy.config) && policy.config.child == true`, map[string]any{"config": map[string]any{}}, false, true},
		{"present parent OR missing required child", `!has(policy.config) || policy.config.child == true`, map[string]any{"config": map[string]any{}}, false, true},
		{"present parent conditional missing required child", `has(policy.config) ? policy.config.child == true : false`, map[string]any{"config": map[string]any{}}, false, true},
		{"present parent child guard", `has(policy.config.child) && policy.config.child.value == true`, map[string]any{"config": map[string]any{}}, false, false},
		{"present full path", `has(policy.config) && policy.config.child == true`, map[string]any{"config": map[string]any{"child": true}}, true, false},
		{"null is present", `has(policy.config) && policy.config.child == true`, map[string]any{"config": nil}, false, true},
		{"wrong shape is present", `has(policy.config) && policy.config.child == true`, map[string]any{"config": true}, false, true},
		{"unguarded missing descendant", `policy.missing.child == true`, nil, false, true},
		{"nested has required prefix", `has(policy.missing.child)`, nil, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, runtimeErr := EvalValueExpression(tc.expression, ValueContext{Policy: tc.policy})
			if tc.invalid {
				if runtimeErr == nil {
					t.Fatalf("negative runtime control unexpectedly succeeded: %#v", value)
				}
			} else if runtimeErr != nil || value != tc.want {
				t.Fatalf("runtime = %#v, %v; want %t", value, runtimeErr, tc.want)
			}
			err := ValidatePolicyReferences(tc.expression, tc.policy, ValueExpressionOptions{})
			var missing *PolicyReferenceError
			if errors.As(err, &missing) != tc.invalid || (!tc.invalid && err != nil) {
				t.Fatalf("declaration check = %v; want invalid %t", err, tc.invalid)
			}
		})
	}
}

func TestR3PolicyStaticLiteralSelectorParity(t *testing.T) {
	policy := map[string]any{"list": []any{"yes"}, "0": "yes", "nested": map[string]any{"items": []any{"yes"}}}
	for _, tc := range []struct {
		expression string
		invalid    bool
		want       bool
	}{
		{`policy.list[0u] == "yes"`, false, true},
		{`policy.list[9u] == "yes"`, true, false},
		{`policy.list[18446744073709551615u] == "yes"`, true, false},
		{`policy.list[4294967296u] == "yes"`, true, false},
		{`policy.list[9] == "yes"`, true, false},
		{`policy.list[-1] == "yes"`, true, false},
		{`policy.list[0.0] == "yes"`, false, true},
		{`policy.list[0.5] == "yes"`, true, false},
		{`policy.list[true] == "yes"`, true, false},
		{`policy.list[null] == "yes"`, true, false},
		{`policy[true] == "yes"`, true, false},
		{`policy[0u] == "yes"`, true, false},
		{`policy[0.0] == "yes"`, true, false},
		{`policy[b"0"] == "yes"`, true, false},
		{`policy[null] == "yes"`, true, false},
		{`policy.nested.items[0u] == "yes"`, false, true},
		{`policy.nested.items[9u] == "yes"`, true, false},
		{`policy.list[?9u].orValue("no") == "no"`, false, true},
		{`policy.list[?-1].orValue("no") == "no"`, false, true},
		{`policy.list[?0.0].orValue("no") == "yes"`, false, true},
		{`policy.list[?18446744073709551615u].orValue("no") == "no"`, true, false},
		{`policy.list[?0.5].orValue("no") == "no"`, true, false},
		{`policy.list[?true].orValue("no") == "no"`, true, false},
		{`policy[?true].orValue("no") == "no"`, false, true},
		{`policy[?0u].orValue("no") == "no"`, false, true},
		{`policy[?"missing"].list[9u].orValue("no") == "no"`, false, true},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			value, runtimeErr := EvalValueExpression(tc.expression, ValueContext{Policy: policy})
			if tc.invalid {
				if runtimeErr == nil {
					t.Fatalf("negative runtime control unexpectedly succeeded: %#v", value)
				}
			} else if runtimeErr != nil || value != tc.want {
				t.Fatalf("runtime = %#v, %v; want %t", value, runtimeErr, tc.want)
			}
			err := ValidatePolicyReferences(tc.expression, policy, ValueExpressionOptions{})
			var missing *PolicyReferenceError
			switch tc.expression {
			case `policy.list[null] == "yes"`, `policy[b"0"] == "yes"`, `policy[null] == "yes"`:
				if err == nil || errors.As(err, &missing) || !strings.Contains(err.Error(), "invalid qualifier type") {
					t.Fatalf("program-construction refusal = %v", err)
				}
				return
			}
			if errors.As(err, &missing) != tc.invalid || (!tc.invalid && err != nil) {
				t.Fatalf("declaration check = %v; want invalid %t", err, tc.invalid)
			}
		})
	}
}

func TestR3PolicyNearestKeyDiagnostic(t *testing.T) {
	for _, tc := range []struct {
		expression string
		policy     map[string]any
		want       string
	}{
		{`policy.investigat == true`, map[string]any{"investigate": true, "budget": false}, `policy["investigate"]`},
		{`policy.config.investigat == true`, map[string]any{"config": map[string]any{"investigate": true}}, `policy["config"]["investigate"]`},
		{`policy["cat"] == true`, map[string]any{"cut": true, "bat": true}, `policy["bat"]`},
		{`policy["a.b "] == true`, map[string]any{"a.b": true}, `policy["a.b"]`},
		{`policy["caf"] == true`, map[string]any{"caf\u00e9": true}, "policy[\"caf\u00e9\"]"},
		{`policy[" "] == true`, map[string]any{"": true}, `policy[""]`},
		{`policy["Investigate"] == true`, map[string]any{"investigate": true}, `policy["investigate"]`},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			var missing *PolicyReferenceError
			err := ValidatePolicyReferences(tc.expression, tc.policy, ValueExpressionOptions{})
			if !errors.As(err, &missing) || !strings.Contains(err.Error(), "did you mean "+tc.want+"?") || !strings.Contains(err.Error(), "declared root keys:") {
				t.Fatalf("teaching diagnostic = %v; want exact suggestion %s", err, tc.want)
			}
			if _, err := EvalValueExpression(tc.expression, ValueContext{Policy: tc.policy}); err == nil {
				t.Fatal("suggestion rewrote the authored runtime reference")
			}
		})
	}
}
