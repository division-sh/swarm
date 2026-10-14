package durabledata_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
)

func TestResourceCodecMapValueOptionalFieldsUseCompiledCatalog(t *testing.T) {
	for _, cell := range []struct {
		name, field, input, want string
		refuse                   bool
		wantMessage              string
	}{
		{"record_control", "records: CodecProfile", `{"records":{"note":null}}`, `{"records":{}}`, false, ""},
		{"list_control", "records: [CodecProfile]", `{"records":[{"note":null}]}`, `{"records":[{}]}`, false, ""},
		{"map_record", "records: map[text]CodecProfile", `{"records":{"primary":{"note":null}}}`, `{"records":{"primary":{}}}`, false, ""},
		{"map_list_record", "records: map[text][CodecProfile]", `{"records":{"primary":[{"note":null}]}}`, `{"records":{"primary":[{}]}}`, false, ""},
		{"list_map_record", "records:\n    - map[text]CodecProfile", `{"records":[{"primary":{"note":null}}]}`, `{"records":[{"primary":{}}]}`, false, ""},
		{"nested_map_record", "records: map[text]map[text]CodecProfile", `{"records":{"outer":{"primary":{"note":null}}}}`, `{"records":{"outer":{"primary":{}}}}`, false, ""},
		{"required_null", "records: map[text]CodecRequiredProfile", `{"records":{"primary":{"note":null}}}`, `{"records":{"primary":{"note":1}}}`, true, "schema validation failed: $.records.primary.note must be integer"},
		{"map_value_null", "records: map[text]CodecProfile", `{"records":{"primary":null}}`, `{"records":{"primary":{}}}`, true, "schema validation failed: $.records.primary must be object"},
		{"undeclared_null", "records: map[text]CodecProfile", `{"records":{"primary":{"other":null}}}`, `{"records":{"primary":{}}}`, true, "schema validation failed: $.records.primary.other is not allowed"},
		{"map_list_null", "records: map[text][CodecProfile]", `{"records":{"primary":[null]}}`, `{"records":{"primary":[{}]}}`, true, "schema validation failed: $.records.primary[0] must be object"},
		{"list_map_null", "records:\n    - map[text]CodecProfile", `{"records":[{"primary":null}]}`, `{"records":[{"primary":{}}]}`, true, "schema validation failed: $.records[0].primary must be object"},
		{"nested_map_null", "records: map[text]map[text]CodecProfile", `{"records":{"outer":{"primary":null}}}`, `{"records":{"outer":{"primary":{}}}}`, true, "schema validation failed: $.records.outer.primary must be object"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			root := t.TempDir()
			for name, text := range map[string]string{
				"schema.yaml": "name: codec-normalization\n",
				"types.yaml":  "types:\n  CodecProfile:\n    note: integer?\n  CodecRequiredProfile:\n    note: integer\n",
				"events.yaml": "records.loaded:\n  " + cell.field + "\n",
			} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(text), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			repo, err := filepath.Abs(filepath.Join("..", ".."))
			if err != nil {
				t.Fatal(err)
			}
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := contracts.BuildDurableDataCatalog(bundle)
			if err != nil || len(catalog.Declarations) != 1 {
				t.Fatalf("compiled declarations=%d error=%v", len(catalog.Declarations), err)
			}
			declaration := catalog.Declarations[0]
			var schema, payload map[string]any
			if err := json.Unmarshal(declaration.CanonicalSchema, &schema); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(cell.input), &payload); err != nil {
				t.Fatal(err)
			}
			normalized, err := eventschema.NormalizeOptionalFieldNulls(schema, payload)
			if cell.refuse {
				if err == nil && eventschema.ValidatePayloadAgainstSchema(schema, normalized) == nil {
					t.Fatal("invalid null passed canonical event admission")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := eventschema.ValidatePayloadAgainstSchema(schema, normalized); err != nil {
					t.Fatal(err)
				}
				raw, err := canonicaljson.Bytes(normalized)
				if err != nil || string(raw) != cell.want {
					t.Fatalf("event normalization=%s error=%v, want %s", raw, err, cell.want)
				}
			}
			for name, compile := range map[string]func(durabledata.DeclarationRef, map[string]any, string, []byte) (durabledata.CompiledVersion, []durabledata.ValidationDefect){
				"import": durabledata.CompileJSONL, "stored": durabledata.CompileStoredJSONL,
			} {
				t.Run(name, func(t *testing.T) {
					control, defects := compile(declaration.Ref, schema, declaration.BusinessKey, []byte(cell.want))
					if len(defects) != 0 || string(control.CanonicalJSONL) != cell.want+"\n" || len(control.Rows) != 1 || control.VersionID == "" {
						t.Fatalf("normalized literal control=%+v defects=%+v", control, defects)
					}
					compiled, defects := compile(declaration.Ref, schema, declaration.BusinessKey, []byte(cell.input))
					if cell.refuse {
						want := []durabledata.ValidationDefect{{Row: 1, Path: "$", Code: "schema_rejected", Message: cell.wantMessage}}
						if !reflect.DeepEqual(defects, want) || !reflect.DeepEqual(compiled, durabledata.CompiledVersion{}) {
							t.Fatalf("invalid null produced candidate=%+v defects=%+v, want=%+v", compiled, defects, want)
						}
						return
					}
					if len(defects) != 0 || !reflect.DeepEqual(compiled, control) {
						t.Fatalf("normalized candidate=%+v defects=%+v, want literal version=%s", compiled, defects, control.VersionID)
					}
				})
			}
		})
	}
}

func TestResourceCodecMapDescentRequiresTypedChildAndPreservesPropertyPrecedence(t *testing.T) {
	ref, err := durabledata.ParseDeclarationRef(".", "records.loaded")
	if err != nil {
		t.Fatal(err)
	}
	optional := map[string]any{"type": "object", "properties": map[string]any{"note": map[string]any{"type": "integer"}}, "additionalProperties": false}
	required := map[string]any{"type": "object", "properties": map[string]any{"note": map[string]any{"type": "integer"}}, "required": []string{"note"}, "additionalProperties": false}
	for _, cell := range []struct {
		name        string
		additional  any
		property    map[string]any
		want        string
		refuse      bool
		wantMessage string
	}{
		{"open", true, nil, `{"records":{"primary":{"note":null}}}`, false, ""},
		{"absent", nil, nil, `{"records":{"primary":{"note":null}}}`, false, ""},
		{"untyped_child", map[string]any{}, nil, `{"records":{"primary":{"note":null}}}`, false, ""},
		{"closed", false, nil, "", true, "schema validation failed: $.records.primary is not allowed"},
		{"required_property_wins", optional, required, "", true, "schema validation failed: $.records.primary.note must be integer"},
		{"optional_property_wins", required, optional, `{"records":{"primary":{}}}`, false, ""},
	} {
		t.Run(cell.name, func(t *testing.T) {
			mapSchema := map[string]any{"type": "object"}
			if cell.additional != nil {
				mapSchema["additionalProperties"] = cell.additional
			}
			if cell.property != nil {
				mapSchema["properties"] = map[string]any{"primary": cell.property}
			}
			schema := map[string]any{"type": "object", "properties": map[string]any{"records": mapSchema}, "required": []string{"records"}, "additionalProperties": false}
			for name, compile := range map[string]func(durabledata.DeclarationRef, map[string]any, string, []byte) (durabledata.CompiledVersion, []durabledata.ValidationDefect){
				"import": durabledata.CompileJSONL, "stored": durabledata.CompileStoredJSONL,
			} {
				t.Run(name, func(t *testing.T) {
					compiled, defects := compile(ref, schema, "", []byte(`{"records":{"primary":{"note":null}}}`))
					if cell.refuse {
						want := []durabledata.ValidationDefect{{Row: 1, Path: "$", Code: "schema_rejected", Message: cell.wantMessage}}
						if !reflect.DeepEqual(defects, want) || !reflect.DeepEqual(compiled, durabledata.CompiledVersion{}) {
							t.Fatalf("expected refusal, got=%+v defects=%+v, want=%+v", compiled, defects, want)
						}
						return
					}
					if len(defects) != 0 || string(compiled.CanonicalJSONL) != cell.want+"\n" {
						t.Fatalf("payload=%s defects=%+v, want=%s", compiled.CanonicalJSONL, defects, cell.want)
					}
				})
			}
		})
	}
}
