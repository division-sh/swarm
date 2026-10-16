package contracts

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func Test2293Q2CatalogRootDispatchesByNodeKind(t *testing.T) {
	source := "Status: [\"open\", 'closed']\nRow: {label: text, status: Status}\ntypes: {value: text}\nscalars: {value: integer}\nenums: {value: boolean}\n"
	catalog, err := projectTypeCatalogDocument(catalogQ2Source(t, source))
	if err != nil {
		t.Fatalf("bare-root type declarations refused: %v", err)
	}
	if got := strings.Join(catalog.Enums["Status"].Values, ","); got != "open,closed" {
		t.Fatalf("quoted enum literals changed: %q", got)
	}
	for _, name := range []string{"Row", "types", "scalars", "enums"} {
		if _, ok := catalog.Types[name]; !ok {
			t.Fatalf("mapping %q was interpreted as a category container, not a record", name)
		}
	}
}

func Test2293Q2CatalogRefusesNonCanonicalDeclarations(t *testing.T) {
	for _, tc := range []struct{ name, source, teaching string }{
		{"scalar_alias", "Label: text\n", "scalar aliases are not supported"},
		{"null", "Row:\n", "record"},
		{"boolean_root", "Flag: true\n", "record"},
		{"integer_root", "Count: 3\n", "record"},
		{"empty_enum", "Status: []\n", "at least one"},
		{"duplicate_enum", "Status: [\"open\", \"open\"]\n", "duplicate"},
		{"plain_enum", "Status: [open, closed]\n", "quote"},
		{"numeric_enum", "Status: [1, 2]\n", "quote"},
		{"boolean_enum", "Status: [true, false]\n", "quote"},
		{"null_enum", "Status: [null]\n", "quote"},
		{"mixed_enum", "Status: [\"open\", closed]\n", "quote"},
		{"list_alias", "Tags: [text]\n", "declare the list on the field"},
		{"same_file_duplicate", "Row: {id: text}\nRow: {id: integer}\n", "duplicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := projectTypeCatalogDocument(catalogQ2Source(t, tc.source))
			if err == nil || !strings.Contains(err.Error(), tc.teaching) || !strings.Contains(err.Error(), "types.yaml:") {
				t.Fatalf("want source-located refusal teaching %q, got %v", tc.teaching, err)
			}
		})
	}
}

func Test2293Q2BareCatalogBundleAdmission(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: bare-catalog\nstages: {done: {final: true}}\n")
	writeFixtureFile(t, filepath.Join(root, "types.yaml"), "Status: [\"open\", \"closed\"]\nRow: {label: text, status: Status}\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "row.received: {row: Row}\n")
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("supported bundle entrance refused bare catalog: %v", err)
	}
	if bundle == nil || len(bundle.RootTypeCatalog().Enums["Status"].Values) != 2 {
		t.Fatal("admitted bundle lost the bare enum")
	}
}

func Test2293Q2BareCatalogNamespaceComposition(t *testing.T) {
	for _, tc := range []struct{ name, root, child string }{
		{"root_enum_child_record", "Shared: [\"open\"]\n", "Shared: {value: integer}\n"},
		{"root_record_child_enum", "Shared: {value: integer}\n", "Shared: [\"open\"]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := loadNamespaceTestBundle(t, tc.root, tc.child)
			diagnostic, ok := AsLoaderDiagnostic(err)
			if bundle != nil || !ok || diagnostic.Code != "contract_loader.declaration_name_collision" {
				t.Fatalf("expected typed composition refusal, got bundle=%v error=%v", bundle != nil, err)
			}
			for _, fragment := range []string{"types.yaml:1:1", "child/types.yaml:1:1", `enum "Shared"`, `named type "Shared"`} {
				if !strings.Contains(err.Error(), fragment) {
					t.Fatalf("composition refusal lost declaration %q: %v", fragment, err)
				}
			}
			if diagnostic.Location.YAMLPath != "Shared" {
				t.Fatalf("fabricated old category path: %s", diagnostic.Location.YAMLPath)
			}
		})
	}
	for _, tc := range []struct{ name, root, child string }{
		{"record_override", "Shared: {value: text}\n", "Shared: {value: integer}\n"},
		{"enum_override", "Shared: [\"root\"]\n", "Shared: [\"child\"]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := loadNamespaceTestBundle(t, tc.root, tc.child)
			if err != nil {
				t.Fatalf("same-category override refused: %v", err)
			}
			catalog := bundle.ResolvedTypeCatalogForFlow("child")
			if tc.name == "enum_override" {
				if got := strings.Join(catalog.Enums["Shared"].Values, ","); got != "child" {
					t.Fatalf("enum override lost: %q", got)
				}
			} else if got := catalog.Types["Shared"].Fields["value"].Type; got != "integer" {
				t.Fatalf("record override lost: %q", got)
			}
		})
	}
	for _, source := range []string{"text: {value: integer}\n", "integer: [\"one\"]\n"} {
		t.Run(source, func(t *testing.T) {
			bundle, err := loadNamespaceTestBundle(t, source, "")
			diagnostic, ok := AsLoaderDiagnostic(err)
			if bundle != nil || !ok || diagnostic.Code != "contract_loader.declaration_name_collision" || !strings.Contains(err.Error(), "built-in scalar") || !strings.Contains(err.Error(), "types.yaml:1:1") {
				t.Fatalf("built-in collision lost typed/source refusal: %v", err)
			}
		})
	}
}

func catalogQ2Source(t *testing.T, source string) yamlsource.Value {
	t.Helper()
	snapshot, err := yamlsource.Load([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.Document("types.yaml").Root()
}
