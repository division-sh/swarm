package tools

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

func TestRoleScopedEntityWholeReadSchemaPreservesDeclaredFieldClosure(t *testing.T) {
	for _, tc := range []struct {
		name           string
		keyed, keyOnly bool
	}{{"keyed", true, false}, {"keyless", false, false}, {"key_only", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			state := "case_key: text\n"
			if !tc.keyOnly {
				state += "case_key_note: text\nother_key: text\n"
			}
			schema := "stages: []\n"
			if tc.keyed {
				schema = "instance: case_key\n" + schema
			}
			root := t.TempDir()
			manifest := "version: 1.0.0\nplatform_version: '*'\n"
			for name, body := range map[string]string{
				"manifest.yaml": manifest, "schema.yaml": "stages: []\n",
				tc.name + "/manifest.yaml": manifest, tc.name + "/schema.yaml": schema, tc.name + "/state.yaml": state,
				"other/manifest.yaml": manifest, "other/schema.yaml": "instance: other_key\nstages: []\n", "other/state.yaml": "other_key: text\n",
			} {
				path := filepath.Join(root, name)
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			repo := runtimepipeline.WorkflowRepoRoot()
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			contract, ok := entityruntime.ResolveForFlow(semanticview.Wrap(bundle), tc.name)
			if !ok {
				t.Fatal("missing exact flow-owned state contract")
			}
			output := roleScopedEntityWholeReadOutputSchema(contract)
			properties := output["properties"].(map[string]any)
			if _, present := properties["case_key"]; present == tc.keyed {
				t.Fatalf("instance-key schema presence = %t, keyed = %t", present, tc.keyed)
			}
			if tc.keyOnly && len(properties) != 0 {
				t.Fatal("key-only read schema is not empty")
			}
			if !tc.keyOnly && (properties["case_key_note"] == nil || properties["other_key"] == nil) {
				t.Fatal("same-named ordinary or foreign-key field was suppressed")
			}
			if err := ValidatePayloadAgainstSchema(output, map[string]any{}); err != nil {
				t.Fatal("empty read object refused", err)
			}
			if err := ValidatePayloadAgainstSchema(output, map[string]any{"case_key": "value"}); (err == nil) == tc.keyed {
				t.Fatalf("key output admission changed: keyed=%t err=%v", tc.keyed, err)
			}
		})
	}
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
			{"null is not authored text", nil, true, false},
			{"unassigned declared field", nil, false, true},
			{"integer", int64(1), true, false},
			{"boolean", false, true, false},
			{"object", map[string]any{}, true, false},
			{"list", []any{}, true, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				row := map[string]any{}
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
		for _, extra := range []string{"entity_id", "flow_instance", "entity_type", "fields", "bookkeeping", "loops", "undeclared"} {
			row := map[string]any{"current_state": "business", extra: nil}
			if err := admitted.Validate(row); err == nil {
				t.Fatalf("declared field admitted extra output key %s", extra)
			}
		}
	}
}
