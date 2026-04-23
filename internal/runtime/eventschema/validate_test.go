package eventschema

import "testing"

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
