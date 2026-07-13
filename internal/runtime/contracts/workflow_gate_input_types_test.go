package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestWorkflowGateInputTypeOwnerAdmitsCanonicalScalarsEndToEnd(t *testing.T) {
	tests := []struct {
		kind  string
		value any
	}{
		{kind: "text", value: "reason"},
		{kind: "integer", value: float64(7)},
		{kind: "numeric", value: 7.5},
		{kind: "boolean", value: true},
		{kind: "timestamp", value: "2026-07-13T01:02:03Z"},
		{kind: "uuid", value: "00000000-0000-0000-0000-000000000123"},
	}
	for _, tc := range tests {
		t.Run(tc.kind, func(t *testing.T) {
			var field WorkflowGateInputField
			if err := yaml.Unmarshal([]byte("type: "+tc.kind), &field); err != nil {
				t.Fatalf("decode canonical gate input: %v", err)
			}
			if field.Type != tc.kind {
				t.Fatalf("decoded type = %q, want %q", field.Type, tc.kind)
			}
			if !WorkflowGateInputValueMatches(field.Type, tc.value) {
				t.Fatalf("canonical value %#v does not match %s", tc.value, tc.kind)
			}
		})
	}
}

func TestWorkflowGateInputTypeOwnerRejectsNonCanonicalAndStructuredTypes(t *testing.T) {
	for _, kind := range []string{"string", "int", "float", "jsonb", "uuid[]", "text[]", "object", "list"} {
		t.Run(kind, func(t *testing.T) {
			var field WorkflowGateInputField
			err := yaml.Unmarshal([]byte("type: "+kind), &field)
			if err == nil {
				t.Fatalf("gate input type %q was accepted", kind)
			}
		})
	}
}

func TestWorkflowGateInputTypeOwnerRejectsMalformedFormattedValues(t *testing.T) {
	for _, tc := range []struct {
		kind  string
		value any
	}{
		{kind: "integer", value: 1.5},
		{kind: "numeric", value: "1.5"},
		{kind: "boolean", value: "true"},
		{kind: "timestamp", value: "tomorrow"},
		{kind: "uuid", value: "not-a-uuid"},
	} {
		if WorkflowGateInputValueMatches(tc.kind, tc.value) {
			t.Fatalf("%s accepted malformed value %#v", tc.kind, tc.value)
		}
	}
}

func TestWorkflowGateInputTypeOwnerConsumesResolvedFieldSchema(t *testing.T) {
	for _, tc := range []struct {
		input  string
		schema map[string]any
	}{
		{input: "text", schema: map[string]any{"type": "string", "pattern": "^[a-z]+$"}},
		{input: "text", schema: map[string]any{"type": "string", "enum": []any{"fast", "deep"}}},
		{input: "integer", schema: map[string]any{"type": "integer", "minimum": 0}},
		{input: "numeric", schema: map[string]any{"type": "number", "maximum": 1}},
		{input: "boolean", schema: map[string]any{"type": "boolean"}},
		{input: "timestamp", schema: map[string]any{"type": "string", "format": "date-time"}},
		{input: "uuid", schema: map[string]any{"type": "string", "format": "uuid"}},
	} {
		if !WorkflowGateInputTypeCompatibleWithResolvedSchema(tc.input, tc.schema) {
			t.Fatalf("input %s is incompatible with resolved schema %#v", tc.input, tc.schema)
		}
	}
	if WorkflowGateInputTypeCompatibleWithResolvedSchema("text", map[string]any{"type": "string", "format": "uuid"}) {
		t.Fatal("plain text input was accepted for UUID field schema")
	}
}

func TestCanonicalWorkflowGateIdentityRejectsNormalizedDuplicateKeys(t *testing.T) {
	err := ValidateCanonicalWorkflowGatePlanIdentity(WorkflowGatePlan{
		Decision: "launch_review",
		Outcomes: map[string]WorkflowGateOutcomePlan{
			"approve":   {AdvancesTo: "operating"},
			" approve ": {AdvancesTo: "operating"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate normalized key") {
		t.Fatalf("ValidateCanonicalWorkflowGatePlanIdentity error = %v, want normalized-duplicate rejection", err)
	}
}
