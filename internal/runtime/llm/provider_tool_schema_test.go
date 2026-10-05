package llm

import (
	"strings"
	"testing"
)

func TestValidateProviderToolSchemaChecksPropertyNames(t *testing.T) {
	for _, names := range []any{nil, true, "text", map[string]any{"type": "unsupported"}} {
		if err := ValidateProviderToolSchema("save_labels", map[string]any{"type": "object", "propertyNames": names}); err == nil {
			t.Fatalf("malformed propertyNames accepted: %#v", names)
		}
	}
	if err := ValidateProviderToolSchema("save_labels", map[string]any{"type": "object", "propertyNames": map[string]any{"type": "string", "format": "uuid"}}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateProviderToolSchemaChecksOneOfBranches(t *testing.T) {
	for _, raw := range []any{nil, []any{}, []any{nil}, []any{map[string]any{"type": "numeric(5,2)"}}, []any{map[string]any{"type": "object", "properties": map[string]any{"bad": map[string]any{"type": "unsupported"}}}}} {
		if err := ValidateProviderToolSchema("save_items", map[string]any{"type": "object", "oneOf": raw}); err == nil {
			t.Fatalf("malformed nested oneOf accepted: %#v", raw)
		}
	}
	if err := ValidateProviderToolSchema("save_items", map[string]any{"type": "object", "oneOf": []any{map[string]any{"type": "object", "additionalProperties": false}}}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateProviderToolSchemaRejectsUnsupportedNestedType(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"capabilities": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"automation_with_unlock": map[string]any{
						"type": "numeric(5,2)",
					},
				},
			},
		},
	}

	err := ValidateProviderToolSchema("emit_category_assessed", schema)
	if err == nil {
		t.Fatal("ValidateProviderToolSchema returned nil, want unsupported type error")
	}
	for _, want := range []string{
		"emit_category_assessed.input_schema.properties.capabilities.properties.automation_with_unlock.type",
		"numeric(5,2)",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want substring %q", err, want)
		}
	}
}

func TestValidateProviderToolSchemaChecksArrayItems(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"history": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "numeric(10,2)",
				},
			},
		},
	}

	err := ValidateProviderToolSchema("emit_spend_request", schema)
	if err == nil {
		t.Fatal("ValidateProviderToolSchema returned nil, want unsupported item type error")
	}
	if !strings.Contains(err.Error(), "emit_spend_request.input_schema.properties.history.items.type") {
		t.Fatalf("error = %q, want item path", err)
	}
}
