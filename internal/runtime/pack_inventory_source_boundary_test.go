package runtime_test

import (
	"go/ast"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestPlatformPackInventoryHasOneSourceAndFiniteProductionConsumers(t *testing.T) {
	allowedCalls := map[string]map[string]struct{}{
		"NewSwarmWorkflowModule": pathSet(),
		"LoadPlatformPackInventoryFS": pathSet(
			"internal/packartifact/development.go",
			"internal/packartifact/embedded.go",
			"internal/testutil/packfixture/packfixture.go",
		),
		"LoadEmbeddedPlatformPackInventory": pathSet(
			"internal/cliapp/cli.go",
			"internal/cliapp/pack_commands.go",
			"internal/cliapp/provider_trigger_packs.go",
			"internal/runtime/contracts/workflow_contract_loading.go",
		),
		"LoadDevelopmentPlatformPackInventory": pathSet(
			"internal/cliapp/provider_trigger_packs.go",
			"internal/testutil/packfixture/packfixture.go",
		),
		"NewEffectivePackInventory": pathSet(
			"internal/cliapp/doctor.go",
			"internal/runtime/contracts/workflow_contract_loading.go",
			"internal/testutil/packfixture/packfixture.go",
		),
		"NewPackRegistryFromInventory": pathSet(
			"internal/packadmission/admission.go",
			"internal/testutil/packfixture/packfixture.go",
		),
		"NewCatalogSnapshotFromInventory": pathSet(
			"internal/packadmission/admission.go",
			"internal/testutil/packfixture/packfixture.go",
		),
		"LoadChannelPacks": pathSet(
			"internal/packadmission/admission.go",
			"internal/testutil/packfixture/packfixture.go",
		),
	}
	forbiddenOwners := map[string]struct{}{
		"BuiltinTool": {}, "DefaultPackRegistry": {}, "LoadBuiltinPackRegistry": {},
		"LoadPlatformPackDirs": {}, "LoadChannelPackDirs": {}, "NewCatalogSnapshotFromPackDirs": {},
		"NewRuntimeContextManagerWithAdmission": {}, "ProcessAdmissionState": {},
		"SourceWithConnectorPackImportsFromRegistry": {}, "compileProcessAdmissionCandidate": {},
	}
	seen := map[string]map[string]struct{}{}
	inspectProductionGo(t, func(path string, file *ast.File) {
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.FuncDecl:
				if _, forbidden := forbiddenOwners[typed.Name.Name]; forbidden {
					t.Errorf("%s declares retired pack authority %s", path, typed.Name.Name)
				}
			case *ast.GenDecl:
				for _, specification := range typed.Specs {
					if typeSpec, ok := specification.(*ast.TypeSpec); ok {
						if _, forbidden := forbiddenOwners[typeSpec.Name.Name]; forbidden {
							t.Errorf("%s declares retired pack authority %s", path, typeSpec.Name.Name)
						}
					}
				}
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := calledFunctionName(call.Fun)
			if _, forbidden := forbiddenOwners[name]; forbidden {
				t.Errorf("%s calls retired pack authority %s", path, name)
			}
			allowed, watched := allowedCalls[name]
			if !watched {
				return true
			}
			if _, ok := allowed[path]; !ok {
				t.Errorf("%s calls pack inventory owner %s outside the explicit consumer census", path, name)
				return true
			}
			if seen[name] == nil {
				seen[name] = map[string]struct{}{}
			}
			seen[name][path] = struct{}{}
			return true
		})
	})
	for name, paths := range allowedCalls {
		if missing := missingPaths(paths, seen[name]); len(missing) > 0 {
			t.Errorf("pack inventory consumer census for %s has stale entries: %s", name, strings.Join(missing, ", "))
		}
	}
}

func TestPackPublishingSurfacesCarryExplicitBaseAndAdmissionOwners(t *testing.T) {
	emptyLoadOptionOwners := pathSet(
		"internal/runtime/contracts/bundle_registration_upload.go",
		"internal/runtime/contracts/workflow_contract_loading.go",
	)
	inspectProductionGo(t, func(path string, file *ast.File) {
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			name := calledFunctionName(literal.Type)
			fields := compositeLiteralFields(literal)
			switch name {
			case "WorkflowContractLoadOptions":
				if len(fields) == 0 {
					if _, allowed := emptyLoadOptionOwners[path]; !allowed {
						t.Errorf("%s constructs empty workflow pack load options outside the pure contract-loader owners", path)
					}
					return true
				}
				if _, ok := fields["AdmitPackInventory"]; !ok {
					t.Errorf("%s workflow pack load options omit canonical body admission", path)
				}
				_, hasBase := fields["PlatformPackBase"]
				_, hasBases := fields["PlatformPackBases"]
				if !hasBase && !hasBases {
					t.Errorf("%s workflow pack load options omit an explicit selected-base owner", path)
				}
			}
			return true
		})
	})
}

func TestPlatformPackBodiesHaveOneEmbedOwnerAndNoRetiredTeachingConfig(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	var bodyEmbeds []string
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(body)
		if strings.Contains(text, "//go:embed") && (strings.Contains(text, "provider-triggers") || strings.Contains(text, "provider-connectors") || strings.Contains(text, "channels/*") || strings.Contains(text, "inventory.yaml")) {
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			bodyEmbeds = append(bodyEmbeds, filepath.ToSlash(relative))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(bodyEmbeds)
	if strings.Join(bodyEmbeds, ",") != "packs/embed.go" {
		t.Fatalf("platform pack body embed owners = %v, want [packs/embed.go]", bodyEmbeds)
	}

	for _, root := range []string{".github/fixtures", "examples"} {
		err := filepath.WalkDir(filepath.Join(repoRoot, root), func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml" && filepath.Ext(path) != ".md") {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			assertNoRetiredPackConfig(t, path, string(body))
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", root, err)
		}
	}
	for _, path := range []string{"swarm.example.yaml", "internal/cliapp/unified_config_example.go"} {
		body, err := os.ReadFile(filepath.Join(repoRoot, path))
		if err != nil {
			t.Fatal(err)
		}
		assertNoRetiredPackConfig(t, path, string(body))
	}
}

func pathSet(paths ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		result[path] = struct{}{}
	}
	return result
}

func missingPaths(want, got map[string]struct{}) []string {
	var missing []string
	for path := range want {
		if _, ok := got[path]; !ok {
			missing = append(missing, path)
		}
	}
	sort.Strings(missing)
	return missing
}

func compositeLiteralFields(literal *ast.CompositeLit) map[string]struct{} {
	fields := make(map[string]struct{}, len(literal.Elts))
	for _, element := range literal.Elts {
		keyed, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		identifier, ok := keyed.Key.(*ast.Ident)
		if ok {
			fields[identifier.Name] = struct{}{}
		}
	}
	return fields
}

func assertNoRetiredPackConfig(t *testing.T, path, body string) {
	t.Helper()
	for _, retired := range []string{"external_dirs:", "provider_triggers:\n", "provider_triggers:\r\n", "channels:\n  packs:", "channels:\r\n  packs:"} {
		if strings.Contains(body, retired) {
			t.Errorf("%s retains retired per-kind pack configuration %q", path, retired)
		}
	}
}
