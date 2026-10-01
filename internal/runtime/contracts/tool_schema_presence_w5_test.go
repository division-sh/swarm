package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestW5RecursiveSchemaFieldPresenceMatrix(t *testing.T) {
	states := []string{"missing", "null", "empty", "scalar", "empty_sequence", "sequence", "empty_mapping", "mapping"}
	for _, row := range []struct{ field, kind, scalar, sequence, mapping, admitted string }{
		{"description", "string", "intent", "[intent]", "{intent: true}", "missing,empty,scalar"},
		{"properties", "object", "value", "[value]", "{value: {type: string}}", "missing,empty_mapping,mapping"},
		{"required", "object", "value", "[value]", "{value: true}", "missing,empty_sequence,sequence"},
		{"items", "array", "string", "[{type: string}]", "{type: string}", "empty_mapping,mapping"},
		{"enum", "string", "value", "[value]", "{value: true}", "missing,sequence"},
		{"additionalProperties", "object", "false", "[false]", "{type: string}", "missing,scalar,empty_mapping,mapping"},
		{"minimum", "number", "0.25", "[0.25]", "{value: 0.25}", "missing,scalar"},
		{"maximum", "number", "1.75", "[1.75]", "{value: 1.75}", "missing,scalar"},
		{"pattern", "string", "'^a+$'", "[a]", "{pattern: a}", "missing,scalar"},
		{"format", "string", "uuid", "[uuid]", "{format: uuid}", "missing,scalar"},
		{"minLength", "string", "0", "[0]", "{value: 0}", "missing,scalar"},
		{"maxLength", "string", "3", "[3]", "{value: 3}", "missing,scalar"},
		{"minItems", "array", "0", "[0]", "{value: 0}", "missing,scalar"},
		{"maxItems", "array", "3", "[3]", "{value: 3}", "missing,scalar"},
	} {
		for _, state := range states {
			t.Run(row.field+"/"+state, func(t *testing.T) {
				body := "type: " + row.kind + "\n"
				if row.field == "required" {
					body += "properties: {value: {type: string}}\n"
				}
				if row.kind == "array" && row.field != "items" {
					body += "items: {type: string}\n"
				}
				if state != "missing" {
					value := map[string]string{"null": "null", "empty": "''", "scalar": row.scalar, "empty_sequence": "[]", "sequence": row.sequence, "empty_mapping": "{}", "mapping": row.mapping}[state]
					body += row.field + ": " + value + "\n"
				}
				source, err := yamlsource.Load([]byte(body))
				if err != nil {
					t.Fatal(err)
				}
				schema, err := AdmitToolInputSchemaValue(source.Document("tools.yaml").Root())
				want := strings.Contains(","+row.admitted+",", ","+state+",")
				if (err == nil) != want {
					t.Fatalf("admit=%t want=%t: %v", err == nil, want, err)
				}
				if err != nil {
					return
				}
				projection := schema.Projection()
				if state == "scalar" {
					if value, exists := projection[row.field]; !exists || value == nil {
						t.Fatalf("typed scalar disappeared: %#v", projection)
					}
				}
				if row.field == "items" && state == "empty_mapping" {
					item, ok := schema.ItemsSchema()
					if !ok || item.Kind() != ToolSchemaAny {
						t.Fatal("explicit Any item lost")
					}
				}
				if row.field == "properties" && state == "mapping" {
					property, ok := schema.Property("value")
					if !ok || property.Kind() != ToolSchemaString {
						t.Fatal("typed property lost")
					}
				}
			})
		}
	}
	for _, state := range states {
		t.Run("type/"+state, func(t *testing.T) {
			body := "description: intent\n"
			if state != "missing" {
				body += "type: " + map[string]string{"null": "null", "empty": "''", "scalar": "string", "empty_sequence": "[]", "sequence": "[string]", "empty_mapping": "{}", "mapping": "{type: string}"}[state] + "\n"
			}
			snapshot, err := yamlsource.Load([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			_, err = AdmitToolInputSchemaValue(snapshot.Document("tools.yaml").Root())
			if (err == nil) != (state == "scalar") {
				t.Fatalf("type/%s: %v", state, err)
			}
		})
	}
}

func TestW5PlatformInterfaceAuthoredSchemaRetainsLexicalEvidence(t *testing.T) {
	for _, row := range []struct {
		literal string
		allowed bool
	}{{"16", true}, {"0x10", false}, {"01", false}, {".nan", false}, {"16.0", true}} {
		t.Run(row.literal, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "platform-spec.yaml")
			body := "interfaces:\n  sample:\n    v1:\n      kind: channel\n      schemas:\n        number: {type: number, enum: [" + row.literal + "]}\n"
			if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			var spec PlatformSpecDocument
			err := loadYAMLFile(path, &spec)
			if (err == nil) != row.allowed {
				t.Fatalf("authored enum=%s: %v", row.literal, err)
			}
			if err == nil {
				if spec.Interfaces["sample"]["v1"].Schemas["number"].Validate(16) != nil {
					t.Fatal("admitted enum lost typed semantics")
				}
			} else if !strings.Contains(err.Error(), "schemas") || !strings.Contains(err.Error(), "platform-spec.yaml:6:") {
				t.Fatalf("schema evidence lost: %v", err)
			}
		})
	}
}

func TestW5ToolSchemaNestedDiagnosticsAndAliasCoordinates(t *testing.T) {
	for _, row := range []struct {
		body         string
		line, column int
		property     string
	}{
		{"worker:\n  input_schema:\n    type: object\n    properties:\n      value:\n        type: string\n        minLength: 1.5\n", 7, 20, "value"},
		{"worker:\n  input_schema:\n    type: object\n    properties:\n      value: &bad {type: string, minLength: 1.5}\n      other: *bad\n", 5, 45, "other"},
	} {
		_, err := admitW5Tools(t, row.body)
		if err == nil {
			t.Fatal("fractional nested length accepted")
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("tools.yaml:%d:%d", row.line, row.column)) || !strings.Contains(err.Error(), fmt.Sprintf(`properties[%q].minLength`, row.property)) {
			t.Fatalf("nested diagnostic: %v", err)
		}
		if row.property == "other" && !strings.Contains(err.Error(), "introduced at tools.yaml:6:14") {
			t.Fatalf("alias introduction lost: %v", err)
		}
	}
}

func TestW5SchemaEqualityPresenceMatrix(t *testing.T) {
	for _, state := range []string{"missing", "null", "empty", "scalar", "empty_sequence", "sequence", "empty_mapping", "mapping"} {
		t.Run(state, func(t *testing.T) {
			property := "{type: integer"
			if state != "missing" {
				property += ", x-swarm-equalTo: " + map[string]string{"null": "null", "empty": "''", "scalar": "left", "empty_sequence": "[]", "sequence": "[left]", "empty_mapping": "{}", "mapping": "{field: left}"}[state]
			}
			property += "}"
			source, err := yamlsource.Load([]byte("type: object\nproperties: {left: {type: integer}, right: " + property + "}\n"))
			if err != nil {
				t.Fatal(err)
			}
			schema, err := AdmitToolInputSchemaValue(source.Document("tools.yaml").Root())
			if (err == nil) != (state == "missing" || state == "scalar") {
				t.Fatalf("equality presence=%s: %v", state, err)
			}
			if err == nil {
				if schema.Validate(map[string]any{"left": 1, "right": 1}) != nil {
					t.Fatal("matching pair rejected")
				}
				if (schema.Validate(map[string]any{"left": 1, "right": 2}) == nil) != (state == "missing") {
					t.Fatal("equality constraint lost/invented")
				}
			}
		})
	}
}

func TestW5ToolsDocumentExpansionBudget(t *testing.T) {
	for _, merged := range []bool{false, true} {
		var body strings.Builder
		if merged {
			body.WriteString("a0: &a0 {description: intent}\n")
		} else {
			body.WriteString("a0: &a0 {response_mapping: [x, x]}\n")
		}
		for i := 1; i <= 15; i++ {
			if merged {
				fmt.Fprintf(&body, "a%d: &a%d {<<: [*a%d, *a%d]}\n", i, i, i-1, i-1)
			} else {
				fmt.Fprintf(&body, "a%d: &a%d {response_mapping: [*a%d, *a%d]}\n", i, i, i-1, i-1)
			}
		}
		body.WriteString("b: *a15\nc: *a15\n")
		if _, err := admitW5Tools(t, body.String()); err == nil || !strings.Contains(err.Error(), "YAML-EXPANSION-LIMIT") {
			t.Fatalf("aggregate merge=%t budget: %v", merged, err)
		}
	}
}

func TestW5ToolProvenanceComposesAndIsIdempotent(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: provenance\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "task.ready:\ntask.done:\n")
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  event_handlers:\n    task.ready:\n      emit: task.done\n")
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  intent: {inline: intent}\n")
	writeFixtureFile(t, filepath.Join(root, "tools.yaml"), "first: &tool\n  description: intent\n  input_schema: {type: string, minLength: 0}\nsecond:\n  <<: *tool\n")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	before := bundle.EffectiveProvenance().Entries()
	for _, family := range []string{"schemas[", "nodes[", "events[", "agents[", "tools["} {
		found := false
		for _, entry := range before {
			if strings.HasPrefix(entry.Path, family) {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing provenance family %s", family)
		}
	}
	populateEffectiveProvenance(bundle)
	if !reflect.DeepEqual(before, bundle.EffectiveProvenance().Entries()) {
		t.Fatal("finalizer replaced/composed ledger inconsistently")
	}
	for _, entry := range before {
		if strings.Contains(entry.Path, `second"].input_schema.minLength`) && (entry.Provenance.SourceLine != 5 || entry.Provenance.SourceColumn != 7) {
			t.Fatalf("merge introduction lost: %+v", entry)
		}
	}
}
