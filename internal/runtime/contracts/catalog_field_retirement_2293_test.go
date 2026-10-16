package contracts

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func Test2293Q2CatalogFieldsRefuseRetiredMetadata(t *testing.T) {
	for _, entrance := range []struct {
		name  string
		admit func(yamlsource.Value) error
	}{
		{"type", func(value yamlsource.Value) error { _, err := projectTypeFieldSpec(value); return err }},
		{"state", func(value yamlsource.Value) error { _, err := projectEntityFieldDecl(value); return err }},
	} {
		for _, key := range []string{"description", "pattern", "length", "range", "equal_to", "immutable", "citation"} {
			for _, value := range []string{"null", "''", "[]", "{}", "false"} {
				t.Run(entrance.name+"/"+key+"/"+value, func(t *testing.T) {
					err := entrance.admit(fieldRetirementValue2293(t, "{type: text, "+key+": "+value+"}"))
					diagnostic, ok := AsLoaderDiagnostic(err)
					if !ok || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Remediation, "YAML comment") {
						t.Fatalf("metadata %s needs source refusal/comment teaching: %v", key, err)
					}
				})
			}
		}
	}
}

func Test2293Q1StateFieldsRefuseRetiredProjectionKeys(t *testing.T) {
	for _, key := range []string{"materialize_from", "project"} {
		for _, value := range []string{"null", "''", "[]", "{}", "false", "collector.reviews"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				_, err := projectEntityFieldDecl(fieldRetirementValue2293(t, "{type: '[text]', "+key+": "+value+"}"))
				diagnostic, ok := AsLoaderDiagnostic(err)
				if !ok || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Remediation, "accumulate target") || !strings.Contains(diagnostic.Remediation, "op: append") {
					t.Fatalf("projection key needs list/append teaching refusal: %v", err)
				}
			})
		}
	}
}

func Test2293Q2CatalogFieldCommentsAndStateIndexingSurvive(t *testing.T) {
	state, err := projectEntityFieldDecl(fieldRetirementValue2293(t, "\n  type: text? # prose\n  indexed: true\n"))
	if err != nil || !state.IsOptional || !state.Indexed || state.Type != "text" {
		t.Fatalf("state indexing/presence changed: %#v error=%v", state, err)
	}
	field, err := projectTypeFieldSpec(fieldRetirementValue2293(t, "\n  type: text? # prose\n"))
	if err != nil || !field.IsOptional || field.Type != "text" {
		t.Fatalf("type/presence changed: %#v error=%v", field, err)
	}
	if _, err := projectTypeFieldSpec(fieldRetirementValue2293(t, "{type: text, indexed: true}")); err == nil {
		t.Fatal("state-only indexing admitted on a record field")
	}
	for _, entrance := range []struct {
		name  string
		admit func(yamlsource.Value) error
	}{
		{"type", func(value yamlsource.Value) error { _, err := projectNamedTypeDeclaration(value); return err }},
		{"state", func(value yamlsource.Value) error { _, err := projectEntityContract(value); return err }},
	} {
		t.Run(entrance.name+"_description", func(t *testing.T) {
			err := entrance.admit(fieldRetirementValue2293(t, "{_description: null, value: text}"))
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Remediation, "YAML comment") {
				t.Fatalf("_description needs typed comment teaching: %v", err)
			}
		})
		t.Run(entrance.name+"_business_fields", func(t *testing.T) {
			if err := entrance.admit(fieldRetirementValue2293(t, "{description: text, pattern: text, immutable: boolean}")); err != nil {
				t.Fatalf("business field names confused with metadata: %v", err)
			}
		})
	}
}

func Test2293Q2ExternalExactSchemaKeepsItsConstraints(t *testing.T) {
	external, err := AdmitToolInputSchemaMap(map[string]any{"type": "string", "pattern": "^[a-z]{3}$", "minLength": 3, "maxLength": 3})
	if err != nil {
		t.Fatal(err)
	}
	entry := EventCatalogEntry{Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{"value": {Type: "text", ExactSchema: &external}}, Required: []string{"value"}}}
	projected := eventSchemaFromCatalogEntry("external.record", entry, TypeCatalogDocument{})
	admitted, err := AdmitToolInputSchemaMap(projected.Schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := admitted.Validate(map[string]any{"value": "abc"}); err != nil {
		t.Fatalf("external valid payload refused: %v", err)
	}
	for _, payload := range []map[string]any{{"value": "ABC"}, {"value": "abcd"}, {"value": 3}, {}, {"value": "abc", "extra": true}} {
		if err := admitted.Validate(payload); err == nil {
			t.Fatalf("external constraints erased: %#v", payload)
		}
	}
}

func Test2293Q2CatalogFieldBundleRefusals(t *testing.T) {
	for _, tc := range []struct{ name, file, source string }{
		{"type_description", "types.yaml", "Row: {value: {type: text, description: prose}}\n"},
		{"type_refinement", "types.yaml", "Row: {value: {type: text, length: {min: 1}}}\n"},
		{"type_prose", "types.yaml", "Row: {_description: prose, value: text}\n"},
		{"state_immutable", "entities.yaml", "Subject: {value: {type: text, immutable: false}}\n"},
		{"state_prose", "entities.yaml", "Subject: {_description: prose, value: text}\n"},
		{"event_prose", "events.yaml", "row.received: {_description: prose, value: text}\n"},
		{"event_citation", "events.yaml", "row.received: {value: {type: text, citation: null}}\n"},
		{"state_projection", "entities.yaml", "Subject: {value: {type: '[text]', materialize_from: collector.values}}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: field-retirement\nstages: {done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, tc.file), tc.source)
			repo := repoRootForContractsTest(t)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			diagnostic, ok := AsLoaderDiagnostic(err)
			if bundle != nil || !ok || diagnostic.Code != "contract_loader.undefined_field" || diagnostic.Location.File == "" || diagnostic.Location.Line == 0 || diagnostic.Location.Column == 0 {
				t.Fatalf("bundle needs exact source-located field refusal: bundle=%v diagnostic=%#v error=%v", bundle != nil, diagnostic, err)
			}
		})
	}
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: field-retirement-control\nstages: {done: {final: true}}\n")
	writeFixtureFile(t, filepath.Join(root, "types.yaml"), "# descriptions are comments\nRow: {description: text, pattern: text, immutable: boolean}\n")
	writeFixtureFile(t, filepath.Join(root, "entities.yaml"), "Subject:\n  row: Row\n  label:\n    type: text?\n    indexed: true\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "row.received: {_embed: [Row]}\n")
	repo := repoRootForContractsTest(t)
	if _, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo)); err != nil {
		t.Fatalf("canonical field/embedding/indexing bundle refused: %v", err)
	}
}

func fieldRetirementValue2293(t *testing.T, source string) yamlsource.Value {
	t.Helper()
	snapshot, err := yamlsource.Load([]byte("field: " + source + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	field, err := snapshot.Document("types.yaml").Root().Lookup("field")
	if err != nil {
		t.Fatal(err)
	}
	return field.Value
}
