package eventschema

import (
	"errors"
	"testing"
)

func TestOneOfCanonicalValidationPreservesExclusiveTypedBranches(t *testing.T) {
	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{"value": map[string]any{}}, "required": []string{"value"},
		"oneOf": []any{
			map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []string{"value"}},
			map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{"value": map[string]any{"type": "array", "items": map[string]any{"type": "integer"}}}, "required": []string{"value"}},
		},
	}
	for _, candidate := range []map[string]any{schema, CanonicalAcceptanceSchema(schema)} {
		for _, tc := range []struct {
			input map[string]any
			valid bool
		}{
			{map[string]any{"value": "text"}, true},
			{map[string]any{"value": []any{1}}, true},
			{map[string]any{"value": []any{"wrong item"}}, false},
			{map[string]any{"value": false}, false},
			{map[string]any{"value": "text", "unknown": nil}, false},
		} {
			if err := ValidatePayloadAgainstSchema(candidate, tc.input); (err == nil) != tc.valid {
				t.Fatalf("payload %#v valid=%v err=%v", tc.input, tc.valid, err)
			}
		}
	}
	for _, branches := range []any{nil, "wrong", []any{}, []any{nil}, []any{map[string]any{"type": "string"}, map[string]any{"type": "string"}}} {
		if err := ValidateValueAgainstSchema(map[string]any{"type": "string", "oneOf": branches}, "text"); err == nil {
			t.Fatalf("malformed or ambiguous oneOf accepted: %#v", branches)
		}
	}
	for _, branches := range [][]any{
		{map[string]any{"type": "null"}, map[string]any{"type": "array", "items": map[string]any{"type": "string"}}},
		{map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, map[string]any{"type": "null"}},
	} {
		optional := map[string]any{"oneOf": branches}
		for _, valid := range []any{nil, []any{"text"}} {
			if err := ValidateValueAgainstSchema(optional, valid); err != nil {
				t.Fatalf("optional-null accepted value changed: %v", err)
			}
		}
		err := ValidateValueAgainstSchema(optional, []any{"text", nil})
		var violation *Violation
		if !errors.As(err, &violation) || violation.Path != "$[1]" || violation.Constraint == "oneOf" {
			t.Fatalf("unique non-null branch lost nested violation: %v", err)
		}
		general := map[string]any{"oneOf": append(append([]any(nil), branches...), map[string]any{"type": "boolean"})}
		err = ValidateValueAgainstSchema(general, []any{"text", nil})
		if !errors.As(err, &violation) || violation.Constraint != "oneOf" || violation.Path != "$" {
			t.Fatalf("general alternatives selected an arbitrary nested violation: %v", err)
		}
	}
}
