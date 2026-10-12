package llm

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProviderSchemaSourceAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  map[string]any
	}{
		{"required-enum", map[string]any{"type": "object", "properties": map[string]any{"op": map[string]any{"type": "string", "enum": []any{"read", "write"}}}, "required": []any{"op"}, "additionalProperties": false}},
		{"oneOf", map[string]any{"type": "object", "oneOf": []any{map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []any{"value"}}}}},
		{"propertyNames", map[string]any{"type": "object", "propertyNames": map[string]any{"type": "string", "enum": []any{"exact"}}}},
		{"items", map[string]any{"type": "array", "items": map[string]any{"type": "number", "minimum": -0.5}}},
	} {
		t.Run("imported/"+tc.name, func(t *testing.T) {
			input, err := contracts.AdmitToolInputSchemaMap(tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := contracts.NewToolSchemaEntry(contracts.WithToolHandler(contracts.ToolHandlerPlatformBuiltin), contracts.WithToolSchemas(input, contracts.ToolInputSchema{}))
			if err != nil {
				t.Fatal(err)
			}
			projection := entry.InputSchema().Projection()
			before := providerSchemaJSON(t, projection)
			if err := ValidateProviderToolSchema("imported", projection); err != nil {
				t.Fatal(err)
			}
			if providerSchemaJSON(t, projection) != before {
				t.Fatal("provider admission rewrote immutable-source projection")
			}
		})
	}
	for _, body := range []string{
		"type: object\nproperties:\n  q: {type: string}\nrequired: [q]\n",
		"type: object\npropertyNames: {type: string}\n",
	} {
		t.Run("authored/"+body, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			input, err := contracts.AdmitToolInputSchemaValue(snapshot.Document("tools.yaml").Root())
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateProviderToolSchema("authored", input.Projection()); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("upstream-refusal", func(t *testing.T) {
		bad := map[string]any{"type": "unsupported-provider-type"}
		if _, err := contracts.AdmitToolInputSchemaMap(bad); err == nil {
			t.Fatal("immutable source admitted unsupported type")
		}
		snapshot, err := yamlsource.Load([]byte("type: unsupported-provider-type\n"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := contracts.AdmitToolInputSchemaValue(snapshot.Document("tools.yaml").Root()); err == nil {
			t.Fatal("authored source admitted unsupported type")
		}
	})
}
