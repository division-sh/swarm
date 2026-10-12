package llm

import (
	"context"
	"strings"
	"testing"
)

func providerSchemaInvalidWireCases() []providerSchemaCase {
	return []providerSchemaCase{
		{name: "padded_type", schema: map[string]any{"type": " object "}},
		{name: "padded_nested_type", schema: map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": " string "}}}},
		{name: "padded_union_member", schema: map[string]any{"type": []any{"object", " null "}}},
		{name: "typed_nil_root", schema: map[string]any(nil)},
		{name: "typed_nil_properties", schema: map[string]any{"type": "object", "properties": map[string]any(nil)}},
		{name: "nil_property_node", schema: map[string]any{"type": "object", "properties": map[string]any{"q": nil}}},
		{name: "typed_nil_property_node", schema: map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any(nil)}}},
		{name: "nil_items", schema: map[string]any{"type": "object", "items": nil}},
		{name: "typed_nil_items", schema: map[string]any{"type": "object", "items": map[string]any(nil)}},
		{name: "typed_nil_property_names", schema: map[string]any{"type": "object", "propertyNames": map[string]any(nil)}},
		{name: "typed_nil_oneof_branch", schema: map[string]any{"type": "object", "oneOf": []any{map[string]any(nil)}}},
		{name: "typed_nil_additional", schema: map[string]any{"type": "object", "additionalProperties": map[string]any(nil)}},
		{name: "nested_type_array", schema: map[string]any{"type": []any{"object", []any{"null"}}}},
		{name: "duplicate_types", schema: map[string]any{"type": []any{"object", "object"}}},
		{name: "duplicate_string_types", schema: map[string]any{"type": []string{"object", "object"}}},
		{name: "empty_types", schema: map[string]any{"type": []any{}}},
		{name: "nil_types", schema: map[string]any{"type": []any(nil)}},
		{name: "nil_string_types", schema: map[string]any{"type": []string(nil)}},
		{name: "duplicate_required", schema: map[string]any{"type": "object", "required": []any{"q", "q"}}},
		{name: "duplicate_string_required", schema: map[string]any{"type": "object", "required": []string{"q", "q"}}},
		{name: "nil_required", schema: map[string]any{"type": "object", "required": []any(nil)}},
		{name: "nil_string_required", schema: map[string]any{"type": "object", "required": []string(nil)}},
		{name: "nil_enum", schema: map[string]any{"type": "object", "enum": []any(nil)}},
	}
}

func TestProviderSchemaWireShapeAdmission(t *testing.T) {
	for _, tc := range providerSchemaInvalidWireCases() {
		t.Run(tc.name, func(t *testing.T) {
			before := providerSchemaJSON(t, tc.schema)
			canonical := ValidateProviderToolSchema("bad", tc.schema)
			if canonical == nil {
				t.Error("canonical owner admitted invalid recognized wire shape")
			}
			session := &Session{Tools: []ToolDefinition{{Name: "bad", Schema: tc.schema}}}
			request, err := (&AnthropicAPIRuntime{}).buildRequest(session, Message{Content: "hello"}, "model")
			if err == nil || len(request.Tools) != 0 {
				t.Errorf("Anthropic admitted invalid wire: request=%s err=%v", providerSchemaJSON(t, request), err)
			} else if canonical != nil {
				requireProviderSchemaError(t, err, canonical.Error())
			}
			mock, err := buildMockRequest(context.Background(), session, Message{Content: "hello"})
			if err == nil || len(mock.Tools) != 0 || len(mock.Messages) != 0 {
				t.Errorf("Mock admitted invalid wire: input=%s err=%v", providerSchemaJSON(t, mock), err)
			} else if canonical != nil {
				requireProviderSchemaError(t, err, canonical.Error())
			}
			if after := providerSchemaJSON(t, tc.schema); after != before {
				t.Fatalf("admission silently normalized bytes: %s != %s", after, before)
			}
		})
	}
	for _, schema := range []map[string]any{
		{"type": "string"}, {"type": "array", "items": map[string]any{"type": "string"}},
		{"type": []any{"object", "null"}}, {},
	} {
		t.Run("anthropic_root/"+providerSchemaJSON(t, schema), func(t *testing.T) {
			if err := ValidateProviderToolSchema("bad", schema); err != nil {
				t.Fatalf("provider-specific root test failed generic subset admission: %v", err)
			}
			request, err := (&AnthropicAPIRuntime{}).buildRequest(&Session{Tools: []ToolDefinition{{Name: "bad", Schema: schema}}}, Message{Content: "hello"}, "model")
			if err == nil || !strings.Contains(err.Error(), `must declare type "object"`) || len(request.Tools) != 0 {
				t.Fatalf("Anthropic accepted a non-object input root: request=%s err=%v", providerSchemaJSON(t, request), err)
			}
		})
	}
}
