package workflowexpr

import (
	"errors"
	"reflect"
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
