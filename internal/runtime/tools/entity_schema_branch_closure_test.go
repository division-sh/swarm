package tools

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func TestEntityOperationSchemaClosurePreservesMapsAndBranchPresence(t *testing.T) {
	contract := entityruntime.Contract{
		EntityType: "case",
		Entity:     runtimecontracts.EntityContract{Fields: map[string]runtimecontracts.EntityFieldDecl{"labels": {Type: "map[text]Entry"}}},
		Types:      runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{"Entry": {Fields: map[string]runtimecontracts.TypeFieldSpec{"name": {Type: "text"}, "note": {Type: "text", IsOptional: true}}}}},
	}
	schema := roleScopedEntityToolSchemaEntry(contract, roleScopedEntityToolSpec{Kind: roleScopedEntityToolSaveField, Field: "labels"}).InputSchema
	closed := closeGeneratedJSONSchema(schema)
	if problems := validateGeneratedJSONSchema("map operation", closed); len(problems) != 0 {
		t.Fatalf("closed typed map: %v", problems)
	}
	for _, tc := range []struct {
		payload map[string]any
		valid   bool
	}{
		{map[string]any{"value": map[string]any{"first": map[string]any{"name": "one"}, "second": map[string]any{"name": "two"}}}, true},
		{map[string]any{"op": "set", "value": map[string]any{"first": map[string]any{"name": "one"}}}, true},
		{map[string]any{"op": "set", "key": "first", "value": map[string]any{"name": "one"}}, true},
		{map[string]any{"op": "set", "key": "first", "value": map[string]any{"name": "one", "unknown": "forbidden"}}, false},
		{map[string]any{"value": map[string]any{"first": map[string]any{"name": "one", "unknown": "forbidden"}}}, false},
		{map[string]any{"op": "set", "key": "first", "value": map[string]any{"name": "one"}, "unknown": nil}, false},
	} {
		for _, candidate := range []map[string]any{schema, closed} {
			if err := ValidatePayloadAgainstSchema(candidate, tc.payload); (err == nil) != tc.valid {
				t.Fatalf("payload %#v valid=%v err=%v", tc.payload, tc.valid, err)
			}
		}
	}
	bad := ObjectSchema(map[string]any{"value": map[string]any{}})
	bad["oneOf"] = []any{map[string]any{"type": "object", "additionalProperties": true}}
	if problems := validateGeneratedJSONSchema("bad branch", bad); len(problems) == 0 {
		t.Fatal("closure ignored open record inside oneOf")
	}
}
