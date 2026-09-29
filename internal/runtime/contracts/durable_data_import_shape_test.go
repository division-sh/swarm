package contracts

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDurableDataImportShapeUsesCompiledStructuralFields(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: import-shape\n")
	writeFixtureFile(t, filepath.Join(root, "types.yaml"), "scalars:\n  ResumeText: text\n  CandidateUUID: uuid\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), `
candidate.created:
  key: id
  id: text
  resume: ResumeText
  cover: {type: "text?", pattern: "^Dear"}
  score: integer
  metadata: json?
  token: CandidateUUID
  stamp: timestamp?
`)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := BuildDurableDataImportShapeCatalog(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Shapes) != 1 {
		t.Fatalf("shapes = %#v", catalog.Shapes)
	}
	dataCatalog, err := BuildDurableDataCatalog(bundle)
	if err != nil || !reflect.DeepEqual(dataCatalog.ImportShapes, catalog.Shapes) {
		t.Fatalf("ordinary catalog sidecar = %#v, %v", dataCatalog.ImportShapes, err)
	}
	shape := catalog.Shapes[0]
	if shape.TextBusinessKey() != "id" || shape.SchemaDigest == "" || shape.BundleHash != catalog.BundleHash {
		t.Fatalf("exact shape binding = %#v", shape)
	}
	want := map[string]struct{ required, text bool }{
		"id": {true, true}, "resume": {true, true}, "cover": {false, true},
		"score": {true, false}, "metadata": {false, false},
		"token": {true, false}, "stamp": {false, false},
	}
	if len(shape.Fields) != len(want) {
		t.Fatalf("fields = %#v", shape.Fields)
	}
	for _, field := range shape.Fields {
		expected, ok := want[field.Name]
		if !ok || field.Required != expected.required || field.Text != expected.text {
			t.Fatalf("field = %#v, expected %#v", field, expected)
		}
	}
}

func TestDurableDataImportShapeFieldlessEventUsesEmptyArray(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: fieldless-import-shape\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "empty.ping:\n")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := BuildDurableDataImportShapeCatalog(bundle)
	if err != nil || len(catalog.Shapes) != 1 {
		t.Fatalf("fieldless catalog = %#v, %v", catalog, err)
	}
	shape := catalog.Shapes[0]
	if shape.Fields == nil || len(shape.Fields) != 0 {
		t.Fatalf("fieldless shape fields = %#v, want nonnil empty array", shape.Fields)
	}
	raw, err := json.Marshal(shape)
	if err != nil {
		t.Fatal(err)
	}
	var projection struct {
		Fields json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(raw, &projection); err != nil || string(projection.Fields) != "[]" {
		t.Fatalf("fieldless shape JSON = %s, %v", raw, err)
	}
}
