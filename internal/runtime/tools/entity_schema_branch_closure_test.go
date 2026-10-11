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

func TestRoleScopedEntityWholeReadSchemaPreservesNullableLifecycle(t *testing.T) {
	contract := entityruntime.Contract{EntityType: "case", Entity: runtimecontracts.EntityContract{
		Fields: map[string]runtimecontracts.EntityFieldDecl{"current_state": {Type: "text"}},
	}}
	entry := roleScopedEntityToolSchemaEntry(contract, roleScopedEntityToolSpec{Kind: roleScopedEntityToolReadWhole})
	execution, err := admitBuiltinExecutionTool("read_case", entry)
	if err != nil {
		t.Fatal(err)
	}
	for _, schema := range []map[string]any{entry.OutputSchema, execution.OutputSchema()} {
		if problems := validateGeneratedJSONSchema("whole entity read", schema); len(problems) != 0 {
			t.Fatal(problems)
		}
		admitted, err := runtimecontracts.AdmitToolInputSchemaMap(schema)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name    string
			stage   any
			present bool
			valid   bool
		}{
			{"staged", "queued", true, true},
			{"actual unknown", "unknown", true, true},
			{"absent named stage", nil, true, true},
			{"missing stage key", nil, false, false},
			{"integer", int64(1), true, false},
			{"boolean", false, true, false},
			{"object", map[string]any{}, true, false},
			{"list", []any{}, true, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				row := map[string]any{"entity_id": "entity", "flow_instance": "review/one", "entity_type": "case",
					"fields": map[string]any{"current_state": "business"}}
				if tc.present {
					row["current_state"] = tc.stage
				}
				if err := admitted.Validate(row); (err == nil) != tc.valid {
					t.Fatalf("typed output valid=%t row=%#v: %v", tc.valid, row, err)
				}
				if err := ValidatePayloadAgainstSchema(schema, row); (err == nil) != tc.valid {
					t.Fatalf("schema output valid=%t row=%#v: %v", tc.valid, row, err)
				}
			})
		}
		for _, extra := range []string{"bookkeeping", "loops", "undeclared"} {
			row := map[string]any{"entity_id": "entity", "flow_instance": "review/one", "entity_type": "case",
				"current_state": nil, "fields": map[string]any{}, extra: nil}
			if err := admitted.Validate(row); err == nil {
				t.Fatalf("nullable stage admitted extra output key %s", extra)
			}
		}
	}
}
