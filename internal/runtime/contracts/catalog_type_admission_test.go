package contracts

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func admissionTestCatalog() TypeCatalogDocument {
	return TypeCatalogDocument{
		Enums: map[string]EnumTypeDecl{"Status": {Values: []string{"open", "closed"}}},
		Types: map[string]NamedTypeDecl{
			"Item":  {Fields: map[string]TypeFieldSpec{"name": {Type: "text"}}},
			"Cycle": {Fields: map[string]TypeFieldSpec{"child": {Type: "Cycle"}}},
			"Left":  {Fields: map[string]TypeFieldSpec{"right": {Type: "[Right]"}}},
			"Right": {Fields: map[string]TypeFieldSpec{"left": {Type: "map[text]Left"}}},
		},
	}
}

func TestCatalogAdmissionRejectsInvalidReferences(t *testing.T) {
	for _, ref := range []string{"", "Missing", "[Missing]", "[[Missing]]", "map[text]Missing", "map[Missing]integer", "map[text][Missing]", "Item[]", "[]Item", "[Item[]]", "map[text][]Item", "Cycle", "[Left]", "map[integer]Item", "map[numeric]Item", "map[boolean]Item"} {
		t.Run(ref, func(t *testing.T) {
			_, err := (CatalogTypeReference{Type: ref, Catalog: admissionTestCatalog()}).Resolve()
			if err == nil {
				t.Fatalf("invalid reference %q was admitted", ref)
			}
			if _, ok := AsLoaderDiagnostic(err); !ok {
				t.Fatalf("reference %q: want typed diagnostic, got %T: %v", ref, err, err)
			}
			if strings.Contains(ref, "[]") && !strings.Contains(err.Error(), "[Item]") {
				t.Fatalf("retirement must teach [Item]: %v", err)
			}
			if ref == "Cycle" || ref == "[Left]" {
				if !strings.Contains(err.Error(), "recursive catalog type") || !strings.Contains(err.Error(), "is unsupported") || strings.Contains(err.Error(), "Wave 1") {
					t.Fatalf("recursion refusal must name the type without an internal phase label: %v", err)
				}
			}
		})
	}
}

func TestCatalogAdmissionNodeListTeaching(t *testing.T) {
	for _, ref := range []string{"numeric(5,2)[]", "[]numeric(5,2)", " numeric(5,2)[] ", " []numeric(5,2) "} {
		err := retiredCatalogListSpelling(ref)
		if _, ok := AsLoaderDiagnostic(err); !ok {
			t.Fatalf("retired node list lacks typed refusal: %v", err)
		}
		if !strings.Contains(err.Error(), "amounts:\n  - numeric(5,2)") || !strings.Contains(err.Error(), "has two items") {
			t.Fatalf("numeric retirement teaches an invalid YAML replacement: %v", err)
		}
	}
	for _, ref := range []string{" text[] ", " []text ", " DimensionScore[] ", " []DimensionScore "} {
		if err := retiredCatalogListSpelling(ref); err == nil {
			t.Fatalf("authored retired spelling with outer whitespace escaped refusal: %q", ref)
		}
	}
	snapshot, err := yamlsource.Load([]byte("amounts:\n  - numeric(5,2)\n"))
	if err != nil {
		t.Fatal(err)
	}
	fields, err := snapshot.Document("nodes.yaml").Root().Mapping()
	if err != nil || len(fields) != 1 {
		t.Fatalf("teaching field shape: %v, %v", fields, err)
	}
	items, err := fields[0].Value.Sequence()
	if err != nil || len(items) != 1 {
		t.Fatalf("teaching list is not one item: %v, %v", items, err)
	}
	scalar, err := items[0].Scalar()
	if err != nil || scalar.Value != "numeric(5,2)" || scalar.Style != 0 {
		t.Fatalf("teaching element did not preserve one plain numeric token: %+v, %v", scalar, err)
	}
}

func TestCatalogAdmissionScopedSourcesRejectInvalidPresence(t *testing.T) {
	for _, tc := range []struct{ name, file, source string }{
		{"unused_self", "types.yaml", "Cycle:\n  child: Cycle\n"},
		{"unused_mutual", "child/types.yaml", "Left:\n  right: [Right]\nRight:\n  left: map[text]Left\n"},
		{"unused_missing", "types.yaml", "Unused:\n  child: Missing\n"},
		{"unused_entity", "child/state.yaml", "child: Missing\n"},
		{"unused_event", "child/events.yaml", "test.ready:\n  child: Missing\n"},
		{"suffix", "types.yaml", "Unused:\n  child: text[]\n"},
		{"prefix", "state.yaml", "child: {type: '[]text', indexed: true}\n"},
		{"integer_key", "events.yaml", "test.ready:\n  child: map[integer]text\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "# fixture: admission\nstages: {done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, "child/manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
			writeFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
			writeFixtureFile(t, filepath.Join(root, "child/schema.yaml"), "# fixture: child\nstages: {done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, tc.file), tc.source)
			repo := repoRootForContractsTest(t)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err == nil || bundle != nil {
				t.Fatalf("invalid unused presence escaped admission: bundle=%v err=%v", bundle != nil, err)
			}
			if diagnostic, ok := AsLoaderDiagnostic(err); !ok || diagnostic.Location.File == "" {
				t.Fatalf("want typed source-located refusal, got %T: %v", err, err)
			}
		})
	}
}

func TestCatalogAdmissionConsumersPreserveDomains(t *testing.T) {
	types := admissionTestCatalog()
	primary := PrimaryEntityContract{Types: types, Contract: EntityContract{Fields: map[string]EntityFieldDecl{
		"labels": {Type: "[text]"}, "items": {Type: "map[text]Item"},
	}}}
	contained, err := SingletonContainedFields(primary)
	if err != nil || len(contained) != 2 {
		t.Fatalf("text collections: %v, %v", contained, err)
	}
	if got := templateInstanceFieldLeafKind(primary, "text"); got != "scalar" {
		t.Fatalf("text template identity = %q", got)
	}
	for _, ref := range []string{"[Item]", "map[text]Item", "map[uuid]Item", "map[Status]Item", "[map[text]Item]", "map[text][Item]"} {
		t.Run(ref, func(t *testing.T) {
			if _, err := (CatalogTypeReference{Type: ref, Catalog: types}).Resolve(); err != nil {
				t.Fatal(err)
			}
			if _, err := singletonCoordinatorContainedKind(primary, ref); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, tc := range []struct {
		ref    string
		schema map[string]any
	}{
		{"text", map[string]any{"type": "string"}},
		{"uuid", map[string]any{"type": "string", "format": "uuid"}},
		{"timestamp", map[string]any{"type": "string", "format": "date-time"}},
		{"Status", map[string]any{"type": "string", "enum": []any{"open", "closed"}}},
	} {
		if got := eventSchemaForResolvedType(tc.ref, types, map[string]struct{}{}); !reflect.DeepEqual(got, tc.schema) {
			t.Fatalf("schema %s = %#v, want %#v", tc.ref, got, tc.schema)
		}
		resolved, err := (CatalogTypeReference{Type: tc.ref, Catalog: types}).Resolve()
		if err != nil {
			t.Fatal(err)
		}
		if got := compiledFileTextField(ResolvedCatalogField{TypeRef: tc.ref, Type: resolved}, types); got != (tc.ref == "text") {
			t.Fatalf("file-text domain %s = %v", tc.ref, got)
		}
	}
}

func TestCatalogAdmissionComparableDomainsRequireResolvedEvidence(t *testing.T) {
	types := admissionTestCatalog()
	for _, ref := range []string{"[Missing]", "map[text]Missing", "Cycle", "[]Item"} {
		if got := schemaRefinementKind(types, ref); got != "unknown" {
			t.Errorf("invalid refinement reference %q classified as %q", ref, got)
		}
		if got := schemaRefinementComparableType(types, ref); got != "" {
			t.Errorf("invalid equality reference %q classified as %q", ref, got)
		}
	}
	for _, pair := range [][2]string{{"text", "uuid"}, {"text", "Status"}, {"text", "timestamp"}, {"integer", "numeric"}, {"json", "object"}} {
		if schemaRefinementComparableType(types, pair[0]) == schemaRefinementComparableType(types, pair[1]) {
			t.Errorf("distinct domains %v collapsed", pair)
		}
	}
}

func TestCatalogAdmissionSharedDAGAndAuthoredAliasPolicy(t *testing.T) {
	root := t.TempDir()
	for name, source := range map[string]string{
		"child/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"manifest.yaml":       "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":         "# fixture: dag\nstages: {done: {final: true}}\n",
		"types.yaml":          "Status: ['open', 'closed']\nLeaf:\n  value: text\nPair:\n  first: Leaf\n  second: Leaf?\nNested:\n  pairs: [Pair]\n  lookup: map[text]Pair\n",
		"state.yaml":          "state: Nested\n",
		"child/schema.yaml":   "# fixture: child\ninstance: id\nstages: {done: {final: true}}\n",
		"child/state.yaml":    "id: text\nitems: map[text]Pair\n",
		"child/events.yaml":   "test.ready:\n  items: [Pair]\n  keys: map[Status]uuid\n  stamp: timestamp\n",
	} {
		writeFixtureFile(t, filepath.Join(root, name), source)
	}
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bundle.ResolveFlowTemplateInstance("child"); err != nil {
		t.Fatal(err)
	}
	primary, err := bundle.ResolveFlowPrimaryEntity("child")
	if err != nil {
		t.Fatal(err)
	}
	if fields, err := SingletonContainedFields(primary); err != nil || len(fields) != 1 {
		t.Fatalf("acyclic repeated names or text domains lost: %v %v", fields, err)
	}
	writeFixtureFile(t, filepath.Join(root, "types.yaml"), "Label: text\n")
	if _, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo)); err == nil || !strings.Contains(err.Error(), "scalar aliases are not supported") {
		t.Fatalf("authored scalar alias was admitted: %v", err)
	}
}

func TestCatalogAdmissionCompiledEventRefusals(t *testing.T) {
	for _, ref := range []string{"", "Missing", "[Missing]", "Cycle", "[Left]", "Item[]", "map[numeric]Item"} {
		entry := EventCatalogEntry{Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{"value": {Type: ref}}}}
		compiled, err := newCompiledEventSchema(".", "test.ready", entry, admissionTestCatalog(), "", CompiledEventSchemaSource{})
		if _, typed := AsLoaderDiagnostic(err); !typed || compiled.value != nil {
			t.Errorf("invalid compiled reference %q gained authority: empty=%v err=%v", ref, compiled.value == nil, err)
		}
		if err := ValidateCatalogFieldSupply("test", map[string]any{}, EntityFieldDecl{Type: ref}, admissionTestCatalog()); err == nil {
			t.Errorf("invalid result target %q gained field-supply authority", ref)
		}
		projected, _ := eventSchemaForTypeRef(ref, admissionTestCatalog(), map[string]struct{}{})
		if _, err := AdmitToolInputSchemaMap(projected); err == nil {
			t.Errorf("invalid registry reference %q gained schema authority: %#v", ref, projected)
		}
	}
}

func TestCatalogAdmissionTextFamilyMapKeys(t *testing.T) {
	types := admissionTestCatalog()
	for _, key := range []string{"text", "Status", "uuid"} {
		resolved, err := (CatalogTypeReference{Type: "map[" + key + "]Item", Catalog: types}).Resolve()
		if err != nil || resolved.Key == nil || resolved.Key.Kind != CatalogTypeText {
			t.Fatalf("text-family key %s lost: %+v %v", key, resolved, err)
		}
		if key == "Status" && resolved.Key.Name != "Status" {
			t.Fatal("enum identity erased")
		}
		if key == "uuid" && resolved.Key.Format != "uuid" {
			t.Fatal("UUID validation format erased")
		}
	}
	for _, key := range []string{"integer", "numeric", "boolean", "timestamp", "json", "array", "Item"} {
		resolved, err := (CatalogTypeReference{Type: "map[" + key + "]Item", Catalog: types}).Resolve()
		if err == nil || resolved.Kind != "" || !strings.Contains(err.Error(), "map keys must be text, enum or uuid") {
			t.Errorf("invalid map key %s gained authority or lost teaching error: %+v %v", key, resolved, err)
		}
	}
}

func TestCatalogAdmissionNativePlatformArrays(t *testing.T) {
	for _, tc := range []struct {
		source, item, description string
		optional                  bool
	}{
		{"array<string>", "string", "", false},
		{"array<uuid>?", "string", "", true},
		{"'array<string> (exact labels)'", "string", "(exact labels)", false},
		{"{type: 'array<integer>', description: counts}", "integer", "counts", false},
		{"{type: 'array<string> (exact labels)', description: payload labels}", "string", "(exact labels). payload labels", false},
	} {
		t.Run(tc.source, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(tc.source))
			if err != nil {
				t.Fatal(err)
			}
			field, optional, err := admitPlatformEventFieldValue(snapshot.Document("native.yaml").Root())
			if err != nil || optional != tc.optional || field.Description != tc.description || !strings.HasPrefix(field.Type, "[") || strings.Contains(field.Type, "[]") {
				t.Fatalf("native list lowering: field=%+v optional=%v err=%v", field, optional, err)
			}
			schema := platformEventFieldJSONSchema(field)
			items, _ := schema["items"].(map[string]any)
			if schema["type"] != "array" || items["type"] != tc.item {
				t.Fatalf("native array item meaning lost: %#v", schema)
			}
			if tc.optional && items["format"] != "uuid" {
				t.Fatal("UUID item format lost")
			}
		})
	}
}

func TestCatalogAdmissionDurableTextPolicy(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "stages: []\n# fixture: import-domains\n")
	writeFixtureFile(t, filepath.Join(root, "types.yaml"), "Status: ['open', 'closed']\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "item.ready:\n  label: text\n  identity: uuid\n  stamp: timestamp?\n  status: Status\n  labels: [text]\n")
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := BuildDurableDataImportShapeCatalog(bundle)
	if err != nil || len(catalog.Shapes) != 1 {
		t.Fatalf("compiled import shape: %+v %v", catalog, err)
	}
	seen := make(map[string]bool)
	for _, field := range catalog.Shapes[0].Fields {
		seen[field.Name] = field.Text
	}
	if !reflect.DeepEqual(seen, map[string]bool{"label": true, "identity": false, "stamp": false, "status": false, "labels": false}) {
		t.Fatalf("compiled file-text eligibility erased declared domains: %v", seen)
	}
}
