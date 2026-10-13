package contracts

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
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
	}
}
