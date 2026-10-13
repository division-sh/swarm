package contracts

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func namespaceTestDeclaration(category, name, body string) string {
	return category + ":\n  " + name + ": " + body + "\n"
}

func loadNamespaceTestBundle(t *testing.T, rootTypes, flowTypes string) (*WorkflowContractBundle, error) {
	t.Helper()
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: namespace\nstages: {done: {final: true}}\n")
	writeFixtureFile(t, filepath.Join(root, "child/schema.yaml"), "name: child\nstages: {done: {final: true}}\n")
	if rootTypes != "" {
		writeFixtureFile(t, filepath.Join(root, "types.yaml"), rootTypes)
	}
	if flowTypes != "" {
		writeFixtureFile(t, filepath.Join(root, "child/types.yaml"), flowTypes)
	}
	repo := repoRootForContractsTest(t)
	return LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
}

func requireNamespaceCollision(t *testing.T, bundle *WorkflowContractBundle, err error, descriptions ...string) {
	t.Helper()
	diagnostic, ok := AsLoaderDiagnostic(err)
	if bundle != nil || !ok || diagnostic.Code != "contract_loader.declaration_name_collision" {
		t.Fatalf("want typed namespace refusal and no bundle, got bundle=%v err=%v", bundle != nil, err)
	}
	if !strings.HasSuffix(diagnostic.Location.File, "types.yaml") || diagnostic.Location.Line <= 0 || diagnostic.Location.Column <= 0 || diagnostic.Location.YAMLPath == "" {
		t.Fatalf("collision lacks authored key location: %+v", diagnostic.Location)
	}
	for _, description := range append(descriptions, "TYPE-NAME-COLLISION", "Rename one") {
		if !strings.Contains(err.Error(), description) {
			t.Fatalf("collision=%v, missing declaration/teaching %q", err, description)
		}
	}
}

func TestCatalogTypeNamespaceRefusesCrossCategoryShadowing(t *testing.T) {
	for _, pair := range []struct{ left, body, description string }{
		{"scalars", "text", `scalar alias "Shared"`},
		{"enums", "{values: [open, closed], default: open}", `enum "Shared"`},
	} {
		for _, body := range []string{"{value: Missing}", "{next: Shared}", "{value: integer}"} {
			for _, placement := range []string{"local", "root_scalar_flow_type", "root_type_flow_scalar"} {
				t.Run(pair.left+"/"+body+"/"+placement, func(t *testing.T) {
					left := namespaceTestDeclaration(pair.left, "Shared", pair.body)
					right := namespaceTestDeclaration("types", "Shared", body)
					root, flow := left+right, ""
					if placement == "root_scalar_flow_type" {
						root, flow = left, right
					}
					if placement == "root_type_flow_scalar" {
						// The root is independently admitted before flow composition.
						root, flow = namespaceTestDeclaration("types", "Shared", "{value: integer}"), left
					}
					bundle, err := loadNamespaceTestBundle(t, root, flow)
					requireNamespaceCollision(t, bundle, err, pair.description, `named type "Shared"`, "types.yaml:2:3")
					if placement != "local" && !strings.Contains(err.Error(), "child/types.yaml:2:3") {
						t.Fatalf("composition omitted flow declaration: %v", err)
					}
				})
			}
		}
	}
	for _, placement := range []string{"local", "root_scalar", "root_enum"} {
		t.Run("scalar_enum/"+placement, func(t *testing.T) {
			left := namespaceTestDeclaration("scalars", "Shared", "text")
			right := namespaceTestDeclaration("enums", "Shared", "{values: [open], default: open}")
			root, flow := left+right, ""
			if placement == "root_scalar" {
				root, flow = left, right
			}
			if placement == "root_enum" {
				root, flow = right, left
			}
			bundle, err := loadNamespaceTestBundle(t, root, flow)
			requireNamespaceCollision(t, bundle, err, `scalar alias "Shared"`, `enum "Shared"`)
		})
	}
}

func TestCatalogTypeNamespaceRefusesBuiltinShadowing(t *testing.T) {
	for _, name := range []string{"text", "integer", "numeric", "boolean", "timestamp", "uuid"} {
		base := "text"
		if name == base {
			base = "integer"
		}
		for _, category := range []struct{ name, body, description string }{
			{"scalars", base, "scalar alias"},
			{"enums", "{values: [open], default: open}", "enum"},
			{"types", "{value: " + base + "}", "named type"},
		} {
			for _, flow := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/flow=%t", name, category.name, flow), func(t *testing.T) {
					root, child := namespaceTestDeclaration(category.name, name, category.body), ""
					if flow {
						root, child = "", root
					}
					bundle, err := loadNamespaceTestBundle(t, root, child)
					requireNamespaceCollision(t, bundle, err, "built-in scalar "+fmt.Sprintf("%q", name), category.description+" "+fmt.Sprintf("%q", name), "types.yaml:2:3")
				})
			}
		}
	}
}

func TestCatalogTypeNamespacePreservesSameCategoryOverridesAndBodyValidation(t *testing.T) {
	for _, category := range []struct{ name, root, child string }{
		{"scalars", "text", "integer"},
		{"enums", "{values: [root], default: root}", "{values: [child], default: child}"},
		{"types", "{root_value: text}", "{child_value: integer}"},
	} {
		t.Run(category.name, func(t *testing.T) {
			bundle, err := loadNamespaceTestBundle(t, namespaceTestDeclaration(category.name, "Shared", category.root), namespaceTestDeclaration(category.name, "Shared", category.child))
			if err != nil {
				t.Fatal(err)
			}
			root, child := bundle.RootTypeCatalog(), bundle.ResolvedTypeCatalogForFlow("child")
			switch category.name {
			case "scalars":
				if root.Scalars["Shared"].Base != "text" || child.Scalars["Shared"].Base != "integer" {
					t.Fatal("scalar override changed")
				}
			case "enums":
				if root.Enums["Shared"].Default != "root" || child.Enums["Shared"].Default != "child" {
					t.Fatal("enum override changed")
				}
			case "types":
				if _, ok := root.Types["Shared"].Fields["root_value"]; !ok {
					t.Fatal("root body changed")
				}
				if len(child.Types["Shared"].Fields) != 1 || child.Types["Shared"].Fields["child_value"].Type != "integer" {
					t.Fatal("named override merged instead of replacing")
				}
			}
		})
	}
	for _, rootInvalid := range []bool{false, true} {
		t.Run(fmt.Sprintf("unused_overridden_body/root_invalid=%t", rootInvalid), func(t *testing.T) {
			valid := namespaceTestDeclaration("types", "Shared", "{value: text}")
			invalid := namespaceTestDeclaration("types", "Shared", "{value: Missing}")
			root, child := valid, invalid
			if rootInvalid {
				root, child = invalid, valid
			}
			bundle, err := loadNamespaceTestBundle(t, root, child)
			diagnostic, ok := AsLoaderDiagnostic(err)
			if bundle != nil || !ok || diagnostic.Code != "contract_loader.invalid_type_reference" || !strings.Contains(err.Error(), `unknown catalog type "Missing"`) {
				t.Fatalf("unused declaration body escaped validation: %v", err)
			}
		})
	}
	if _, err := loadNamespaceTestBundle(t, namespaceTestDeclaration("types", "Text", "{value: integer}"), ""); err != nil {
		t.Fatalf("exact built-in reservation was widened to different spelling: %v", err)
	}
}

func TestCatalogTypeNamespaceChecksBodiesBeforeReferenceSyntax(t *testing.T) {
	for _, name := range []string{"[text]", "map[text]text", "Unused"} {
		t.Run(name, func(t *testing.T) {
			source := fmt.Sprintf("types:\n  %q: {value: Missing}\n", name)
			bundle, err := loadNamespaceTestBundle(t, source, "")
			diagnostic, ok := AsLoaderDiagnostic(err)
			if bundle != nil || !ok || diagnostic.Code != "contract_loader.invalid_type_reference" || !strings.Contains(err.Error(), `unknown catalog type "Missing"`) {
				t.Fatalf("reference syntax bypassed unused declaration body: bundle=%v err=%v", bundle != nil, err)
			}
		})
	}
}
