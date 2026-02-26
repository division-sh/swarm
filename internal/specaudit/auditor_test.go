package specaudit

import (
	"encoding/json"
	"testing"
)

func TestValidate_EmptyAndInvalidJSON(t *testing.T) {
	if res := Validate("vertical_spec", nil); res.Passed {
		t.Fatalf("expected empty spec to fail: %+v", res)
	}
	if res := Validate("vertical_spec", []byte("{")); res.Passed {
		t.Fatalf("expected invalid json to fail: %+v", res)
	}
}

func TestValidate_VerticalSpec_Tier1PassesWithRecommendedWarnings(t *testing.T) {
	raw := []byte(`{
		"problem":"x",
		"target_user":"y",
		"core_workflow":"z",
		"features":["a","b","c"],
		"data_model":{"entities":["lead"]},
		"user_story":"as owner",
		"exclusions":["advanced analytics"]
	}`)
	res := Validate("vertical_spec", raw)
	if !res.Passed {
		t.Fatalf("expected Tier1 pass, got issues=%+v", res.Issues)
	}
}

func TestValidate_VerticalSpec_Tier1MissingRequiredFails(t *testing.T) {
	raw := []byte(`{
		"problem":"x",
		"features":["a","b","c"]
	}`)
	res := Validate("vertical_spec", raw)
	if res.Passed {
		t.Fatalf("expected fail for missing required fields, got %+v", res)
	}
}

func TestValidate_VerticalSpec_Tier1HighDoesNotFail(t *testing.T) {
	raw := []byte(`{
		"problem":"x",
		"core_workflow":"z",
		"features":["a","b","c","d","e","f"],
		"data_model":{"entities":["lead"]},
		"user_story":"as owner",
		"out_of_scope":["enterprise"],
		"pricing":{"tiers":["starter"]}
	}`)
	res := Validate("vertical_spec", raw)
	if !res.Passed {
		t.Fatalf("high severity should not fail Tier1 validation: %+v", res)
	}
	foundHigh := false
	for _, issue := range res.Issues {
		if issue.Code == "scope_creep_too_many_features" && issue.Severity == "high" {
			foundHigh = true
			break
		}
	}
	if !foundHigh {
		t.Fatalf("expected high scope_creep_too_many_features issue, got %+v", res.Issues)
	}
}

func TestValidate_Template_DeepValidation(t *testing.T) {
	// Construct a template envelope that hits:
	// - duplicate roles
	// - missing parent role
	// - parent cycle
	// - empty tool entry
	// - invalid route pattern
	// - unknown subscriber_role
	env := map[string]any{
		"version": "1.0.0",
		"agents": []any{
			map[string]any{"role": "a", "parent_role": "b", "tools": []any{"x", ""}},
			map[string]any{"role": "a", "parent_role": "a"},
			map[string]any{"role": "b", "parent_role": "a"},
		},
		"bootstrap_routes": []any{
			map[string]any{"event_pattern": "BAD PATTERN", "subscriber_role": "missing"},
			map[string]any{"event_pattern": "", "subscriber_id": ""},
			"not-an-object",
		},
		"seeded_routes": []any{
			map[string]any{"event_pattern": "ok.route", "subscriber_id": "explicit-id"},
		},
	}
	raw, _ := json.Marshal(env)
	res := Validate("template", raw)
	if res.Passed {
		t.Fatalf("expected template validation to fail")
	}
	if len(res.Issues) < 5 {
		t.Fatalf("expected many issues, got %d: %+v", len(res.Issues), res.Issues)
	}
}

func TestValidate_Template_UnknownProducerChecks(t *testing.T) {
	env := map[string]any{
		"version": "2.0.1",
		"agents": []any{
			map[string]any{
				"role":          "opco-ceo",
				"parent_role":   "",
				"subscriptions": []any{"nonexistent.event"},
			},
		},
		"bootstrap_routes": []any{
			map[string]any{"event_pattern": "unknown.event", "subscriber_role": "opco-ceo"},
		},
		"seeded_routes": []any{},
	}
	raw, _ := json.Marshal(env)
	res := Validate("template", raw)
	if res.Passed {
		t.Fatalf("expected unknown-producer checks to fail validation, got issues=%+v", res.Issues)
	}
	codes := map[string]bool{}
	for _, issue := range res.Issues {
		codes[issue.Code] = true
	}
	if !codes["unknown_subscription_producer"] {
		t.Fatalf("expected unknown_subscription_producer issue, got %+v", res.Issues)
	}
	if !codes["unknown_event_producer"] {
		t.Fatalf("expected unknown_event_producer issue, got %+v", res.Issues)
	}
}

func TestValidate_Template_UnconsumedRoutePattern(t *testing.T) {
	env := map[string]any{
		"version": "2.0.1",
		"agents": []any{
			map[string]any{
				"role":          "opco-ceo",
				"subscriptions": []any{"product_report"},
			},
		},
		"bootstrap_routes": []any{
			map[string]any{"event_pattern": "growth_report", "subscriber_role": "opco-ceo"},
		},
		"seeded_routes": []any{},
	}
	raw, _ := json.Marshal(env)
	res := Validate("template", raw)
	if res.Passed {
		t.Fatalf("expected unconsumed route to fail validation")
	}
	found := false
	for _, issue := range res.Issues {
		if issue.Code == "unconsumed_route_pattern" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected unconsumed_route_pattern issue, got %+v", res.Issues)
	}
}

func TestValidate_Template_MessageAuthority_UsesCanonicalRoleAliases(t *testing.T) {
	env := map[string]any{
		"version": "2.0.2",
		"agents": []any{
			map[string]any{
				"role": "head_of_product",
			},
		},
		"bootstrap_routes": []any{},
		"seeded_routes":    []any{},
	}
	raw, _ := json.Marshal(env)
	res := Validate("template", raw)

	// Canonical alias mapping should treat head_of_product as vp-product and
	// therefore validate message recipients from the commgraph authority table.
	found := false
	for _, issue := range res.Issues {
		if issue.Code == "unknown_message_recipient" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected unknown_message_recipient when canonical alias sender has missing recipients; got %+v", res.Issues)
	}
}

func TestValidate_TechnicalSpec_RequiresTechnicalSignals(t *testing.T) {
	raw := []byte(`{
		"version": "2.0.7",
		"agents": [{"role":"opco-ceo"}],
		"bootstrap_routes": []
	}`)
	res := Validate("technical_spec", raw)
	if res.Passed {
		t.Fatalf("expected technical_spec to fail without data_model/flow/implementation fields")
	}
}
