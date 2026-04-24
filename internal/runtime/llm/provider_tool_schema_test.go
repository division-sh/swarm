package llm

import (
	"strings"
	"testing"
)

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
