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
		Scalars: map[string]ScalarTypeDecl{"Label": {Base: "text"}, "ID": {Base: "uuid"}, "Time": {Base: "timestamp"}},
		Enums:   map[string]EnumTypeDecl{"Status": {Values: []string{"open", "closed"}, Default: "open"}},
		Types: map[string]NamedTypeDecl{
			"Item":  {Fields: map[string]TypeFieldSpec{"name": {Type: "Label"}}},
			"Cycle": {Fields: map[string]TypeFieldSpec{"child": {Type: "Cycle"}}},
			"Left":  {Fields: map[string]TypeFieldSpec{"right": {Type: "[Right]"}}},
			"Right": {Fields: map[string]TypeFieldSpec{"left": {Type: "map[text]Left"}}},
		},
	}
}

func TestCatalogAdmissionRejectsInvalidReferences(t *testing.T) {
	for _, ref := range []string{"", "Missing", "[Missing]", "map[text]Missing", "Item[]", "[]Item", "[Item[]]", "map[text][]Item", "Cycle", "[Left]", "map[integer]Item", "map[numeric]Item", "map[boolean]Item"} {
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
		})
	}
}

func TestCatalogAdmissionScopedSourcesRejectInvalidPresence(t *testing.T) {
	for _, tc := range []struct{ name, file, source string }{
		{"unused_self", "types.yaml", "types:\n  Cycle:\n    child: Cycle\n"},
		{"unused_mutual", "child/types.yaml", "types:\n  Left:\n    right: '[Right]'\n  Right:\n    left: map[text]Left\n"},
		{"unused_missing", "types.yaml", "types:\n  Unused:\n    child: Missing\n"},
		{"unused_entity", "child/entities.yaml", "person:\n  child: Missing\n"},
		{"unused_event", "child/events.yaml", "test.ready:\n  child: Missing\n"},
		{"suffix", "types.yaml", "types:\n  Unused:\n    child: text[]\n"},
		{"prefix", "entities.yaml", "person:\n  child: '[]text'\n"},
		{"integer_key", "events.yaml", "test.ready:\n  child: map[integer]text\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: admission\nstages: {done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, "child/schema.yaml"), "name: child\nstages: {done: {final: true}}\n")
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
		"labels": {Type: "[Label]"}, "items": {Type: "map[Label]Item"},
	}}}
	contained, err := SingletonContainedFields(primary)
	if err != nil || len(contained) != 2 {
		t.Fatalf("text alias collections: %v, %v", contained, err)
	}
	if got := templateInstanceFieldLeafKind(primary, "Label"); got != "scalar" {
		t.Fatalf("text alias template identity = %q", got)
	}
	for _, ref := range []string{"[Item]", "map[Label]Item", "map[uuid]Item", "map[ID]Item", "map[Status]Item", "[map[text]Item]", "map[text][Item]"} {
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
		{"Label", map[string]any{"type": "string"}},
		{"ID", map[string]any{"type": "string", "format": "uuid"}},
		{"Time", map[string]any{"type": "string", "format": "date-time"}},
		{"Status", map[string]any{"type": "string", "enum": []any{"open", "closed"}}},
	} {
		if got := eventSchemaForResolvedType(tc.ref, types, map[string]struct{}{}); !reflect.DeepEqual(got, tc.schema) {
			t.Fatalf("schema %s = %#v, want %#v", tc.ref, got, tc.schema)
		}
		resolved, err := (CatalogTypeReference{Type: tc.ref, Catalog: types}).Resolve()
		if err != nil {
			t.Fatal(err)
		}
		if got := compiledFileTextField(ResolvedCatalogField{TypeRef: tc.ref, Type: resolved}, types); got != (tc.ref == "Label") {
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
	for _, pair := range [][2]string{{"Label", "text"}, {"ID", "uuid"}, {"Time", "timestamp"}, {"[Label]", "[text]"}, {"map[Label]Item", "map[text]Item"}} {
		if left, right := schemaRefinementComparableType(types, pair[0]), schemaRefinementComparableType(types, pair[1]); left == "" || left != right {
			t.Errorf("equivalent domains %v differ: %q %q", pair, left, right)
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
		"schema.yaml":         "name: dag\nstages: {done: {final: true}}\n",
		"types.yaml":          "scalars:\n  Label: text\n  ID: uuid\n  Time: timestamp\nenums:\n  Status: {values: [open, closed], default: open}\ntypes:\n  Leaf:\n    value: Label\n  Pair:\n    first: Leaf\n    second: Leaf?\n  Nested:\n    pairs: '[Pair]'\n    lookup: map[Label]Pair\n",
		"entities.yaml":       "person:\n  state: Nested\n",
		"child/schema.yaml":   "name: child\ninstance: id\nstages: {done: {final: true}}\n",
		"child/entities.yaml": "person:\n  id: Label\n  items: map[Label]Pair\n",
		"child/events.yaml":   "test.ready:\n  items: '[Pair]'\n  keys: map[Status]ID\n  stamp: Time\n",
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
		t.Fatalf("acyclic repeated names or text aliases lost: %v %v", fields, err)
	}
	writeFixtureFile(t, filepath.Join(root, "types.yaml"), "scalars:\n  Label: text\n  Chain: Label\n")
	if _, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo)); err == nil || !strings.Contains(err.Error(), "supported built-in scalar") {
		t.Fatalf("authored alias-chain restriction changed: %v", err)
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
	for _, key := range []string{"text", "Label", "Status", "uuid", "ID"} {
		resolved, err := (CatalogTypeReference{Type: "map[" + key + "]Item", Catalog: types}).Resolve()
		if err != nil || resolved.Key == nil || resolved.Key.Kind != CatalogTypeText {
			t.Fatalf("text-family key %s lost: %+v %v", key, resolved, err)
		}
		if key == "Status" && resolved.Key.Name != "Status" {
			t.Fatal("enum identity erased")
		}
		if (key == "uuid" || key == "ID") && resolved.Key.Format != "uuid" {
			t.Fatal("UUID validation format erased")
		}
	}
	for _, key := range []string{"integer", "numeric", "boolean", "timestamp", "Time", "json", "array", "Item"} {
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
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: import-domains\n")
	writeFixtureFile(t, filepath.Join(root, "types.yaml"), "scalars:\n  Label: text\n  ID: uuid\n  Time: timestamp\nenums:\n  Status: {values: [open, closed], default: open}\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "item.ready:\n  label: {type: Label, pattern: '^valid', length: {min: 5}}\n  identity: ID\n  stamp: Time?\n  status: Status\n  labels: '[Label]'\n")
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

func TestCatalogAdmissionRefinementPolicy(t *testing.T) {
	types := admissionTestCatalog()
	types.Enums["OtherStatus"] = EnumTypeDecl{Values: []string{"open", "closed"}, Default: "open"}
	for _, tc := range []struct {
		name, left, right string
		accepted          bool
	}{
		{"text_alias", "Label", "text", true},
		{"uuid_alias", "ID", "uuid", true},
		{"time_alias", "Time", "timestamp", true},
		{"same_enum", "Status", "Status", true},
		{"different_enum", "Status", "OtherStatus", false},
		{"enum_text", "Status", "text", false},
		{"uuid_text", "ID", "text", false},
		{"integer_numeric", "integer", "numeric", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			errs := validateSchemaRefinementFields("declared fields", types, map[string]schemaRefinementField{
				"left":  {Name: "left", TypeRef: tc.left, Refinements: SchemaRefinements{EqualTo: "right"}},
				"right": {Name: "right", TypeRef: tc.right},
			})
			if (len(errs) == 0) != tc.accepted {
				t.Fatalf("same-declared equality %s vs %s: %v", tc.left, tc.right, errs)
			}
		})
	}
	for _, tc := range []struct {
		ref      string
		accepted bool
	}{
		{"Label", true}, {"[Label]", false}, {"integer", false}, {"Time", false},
	} {
		errs := validateSchemaRefinementFields("pattern", types, map[string]schemaRefinementField{"value": {Name: "value", TypeRef: tc.ref, Refinements: SchemaRefinements{Pattern: "^valid"}}})
		if (len(errs) == 0) != tc.accepted {
			t.Fatalf("pattern eligibility %s: %v", tc.ref, errs)
		}
	}
}
