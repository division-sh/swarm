package workflowexpr

import (
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"testing"
)

func TestImportedDictionaryRecursivePresence(t *testing.T) {
	leaf := rc.MustToolInputSchema(rc.ToolSchemaObject,
		rc.ToolSchemaProperties(map[string]rc.ToolInputSchema{"note": rc.MustToolInputSchema(rc.ToolSchemaString)}),
		rc.ToolSchemaAdditionalPropertiesAllowed(false))
	dictionary := func(value rc.ToolInputSchema) rc.ToolInputSchema {
		return rc.MustToolInputSchema(rc.ToolSchemaObject, rc.ToolSchemaAdditionalPropertiesSchema(value))
	}
	for _, tc := range []struct {
		name         string
		schema       rc.ToolInputSchema
		value        any
		safe, unsafe string
	}{
		{"record", dictionary(leaf), map[string]any{"k": map[string]any{}},
			`payload.labels[?"k"].optFlatMap(v, v.?note).orValue("")`,
			`payload.labels[?"k"].optMap(v, v.note).orValue("")`},
		{"list of records", dictionary(rc.MustToolInputSchema(rc.ToolSchemaArray, rc.ToolSchemaItems(leaf))), map[string]any{"k": []any{map[string]any{}}},
			`payload.labels[?"k"].optMap(v, v.exists(r, r.?note.orValue("") != "")).orValue(false)`,
			`payload.labels[?"k"].optMap(v, v.exists(r, r.note != "")).orValue(false)`},
		{"nested dictionary", dictionary(dictionary(leaf)), map[string]any{"k": map[string]any{"inner": map[string]any{}}},
			`payload.labels[?"k"].optFlatMap(v, v[?"inner"]).optFlatMap(r, r.?note).orValue("")`,
			`payload.labels[?"k"].optFlatMap(v, v[?"inner"]).optMap(r, r.note).orValue("")`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := rc.CompileImportedEventSchema(".", "observed", rc.EventCatalogEntry{
				Payload: rc.EventPayloadSpec{Properties: map[string]rc.EventFieldSpec{"labels": {ExactSchema: &tc.schema}}, Required: []string{"labels"}},
			}, rc.CompiledEventSchemaSource{FlowPath: ".", Layer: "provider"})
			if err != nil {
				t.Fatal(err)
			}
			payload, ok := compiled.StructuralType()
			if !ok {
				t.Fatal("compiled schema has no structural type")
			}
			opts := ValueExpressionOptions{PayloadType: &payload}
			for _, expr := range []string{tc.safe, tc.unsafe} {
				err := ValidateValueExpressionWithOptions(expr, opts)
				if expr == tc.unsafe {
					if err == nil {
						t.Fatalf("unsafe imported shape admitted: %s", expr)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, labels := range []any{tc.value, map[string]any{}} {
					if _, err := EvalValueExpressionWithOptions(expr, ValueContext{Payload: map[string]any{"labels": labels}}, opts); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestUnknownRecordTraversalFailsClosed(t *testing.T) {
	for _, schema := range []map[string]any{
		{"type": "object", "additionalProperties": true},
		{"type": "object", "additionalProperties": map[string]any{}},
	} {
		unresolved, err := rc.ResolveJSONSchemaStructuralType(schema, "Unknown")
		if err != nil {
			t.Fatal(err)
		}
		payload := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "value", Type: unresolved}}}
		for _, expr := range []string{`payload.value.note`, `payload.value.?note.orValue("")`, `has(payload.value.note)`} {
			if err := ValidateValueExpressionWithOptions(expr, ValueExpressionOptions{PayloadType: &payload}); err == nil {
				t.Fatalf("untyped field traversal admitted: %s", expr)
			}
		}
	}
}
