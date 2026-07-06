package eventschema

import (
	"strings"
	"testing"
)

func TestValidatePayloadAgainstSchema_RejectsUnsupportedSchemaType(t *testing.T) {
	t.Parallel()

	err := ValidatePayloadAgainstSchema(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"mode": map[string]any{"type": "Mode"},
		},
		"additionalProperties": false,
	}, map[string]any{"mode": "fast"})
	if err == nil {
		t.Fatal("expected unsupported schema type failure")
	}
}

func TestValidatePayloadAgainstSchema_AllowsOnlyExplicitNullableNull(t *testing.T) {
	t.Parallel()

	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"nullable_value": map[string]any{"type": "boolean", "nullable": true},
			"strict_value":   map[string]any{"type": "boolean"},
		},
		"additionalProperties": false,
	}

	if err := ValidatePayloadAgainstSchema(schema, map[string]any{"nullable_value": nil, "strict_value": true}); err != nil {
		t.Fatalf("nullable value rejected: %v", err)
	}
	if err := ValidatePayloadAgainstSchema(schema, map[string]any{"nullable_value": "not-bool", "strict_value": true}); err == nil {
		t.Fatal("expected non-null nullable field to still enforce declared type")
	}
	if err := ValidatePayloadAgainstSchema(schema, map[string]any{"strict_value": nil}); err == nil {
		t.Fatal("expected non-nullable field to reject null")
	}
}

func TestValidatePayloadAgainstSchema_RejectsInvalidStringFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format string
		value  any
	}{
		{name: "uuid", format: "uuid", value: "not-a-uuid"},
		{name: "date-time", format: "date-time", value: "not-a-timestamp"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidatePayloadAgainstSchema(map[string]any{
				"type": "object",
				"properties": map[string]any{
					"value": map[string]any{
						"type":   "string",
						"format": tc.format,
					},
				},
				"required":             []any{"value"},
				"additionalProperties": false,
			}, map[string]any{"value": tc.value})
			if err == nil {
				t.Fatalf("expected %s format failure", tc.format)
			}
		})
	}
}

func TestValidatePayloadAgainstSchema_RejectsCaseVariantEnumValue(t *testing.T) {
	t.Parallel()

	err := ValidatePayloadAgainstSchema(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"mode": map[string]any{
				"type": "string",
				"enum": []any{"fast"},
			},
		},
		"required":             []any{"mode"},
		"additionalProperties": false,
	}, map[string]any{"mode": "FAST"})
	if err == nil {
		t.Fatal("expected case-variant enum value to fail")
	}
}

func TestValidatePayloadAgainstSchema_EnforcesSchemaRefinements(t *testing.T) {
	t.Parallel()

	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"source_ref": map[string]any{
				"type":      "string",
				"pattern":   "^[0-9a-f]{40}$",
				"minLength": 40,
				"maxLength": 40,
			},
			"label": map[string]any{
				"type":      "string",
				"minLength": 3,
			},
			"files": map[string]any{
				"type":     "array",
				"minItems": 1,
				"items":    map[string]any{"type": "string"},
			},
			"score": map[string]any{
				"type":    "number",
				"minimum": 0.5,
				"maximum": 1.0,
			},
			"component": map[string]any{"type": "string"},
			"owner": map[string]any{
				"type":            "string",
				"x-swarm-equalTo": "component",
			},
		},
		"required":             []any{"source_ref", "label", "files", "score", "component", "owner"},
		"additionalProperties": false,
	}

	valid := map[string]any{
		"source_ref": "0123456789abcdef0123456789abcdef01234567",
		"label":      "deploy",
		"files":      []any{"manifest.yaml"},
		"score":      0.9,
		"component":  "deploy",
		"owner":      "deploy",
	}
	if err := ValidatePayloadAgainstSchema(schema, valid); err != nil {
		t.Fatalf("valid payload rejected: %v", err)
	}

	for _, tc := range []struct {
		name    string
		mutate  func(map[string]any)
		wantErr string
	}{
		{name: "pattern", mutate: func(p map[string]any) { p["source_ref"] = "not-a-sha" }, wantErr: "pattern"},
		{name: "length", mutate: func(p map[string]any) { p["label"] = "x" }, wantErr: "length"},
		{name: "min items", mutate: func(p map[string]any) { p["files"] = []any{} }, wantErr: "length"},
		{name: "range", mutate: func(p map[string]any) { p["score"] = 1.5 }, wantErr: "<="},
		{name: "equality", mutate: func(p map[string]any) { p["owner"] = "other" }, wantErr: "must equal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cloneTestPayload(valid)
			tc.mutate(payload)
			err := ValidatePayloadAgainstSchema(schema, payload)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidatePayloadAgainstSchema error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func cloneTestPayload(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
