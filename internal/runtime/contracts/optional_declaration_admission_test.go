package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type optionalDeclarationRoleTestCase struct {
	name     string
	fileName string
	valid    string
	merged   string
	blank    string
	collide  string
	load     func(string) (int, error)
}

func optionalDeclarationRoleTestCases() []optionalDeclarationRoleTestCase {
	return []optionalDeclarationRoleTestCase{
		{
			name: "agents", fileName: "agents.yaml", valid: "worker: {}\n", merged: "<<: &declarations\n  worker:\n    intent: {inline: test intent}\n    model: regular\n", blank: "\"\": {}\n", collide: "worker: {}\n\" worker \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalAgentDeclarations(path)
				return len(value), err
			},
		},
		{
			name: "entities", fileName: "entities.yaml", valid: "item: {}\n", merged: "<<: &declarations\n  item: {}\n", blank: "\"\": {}\n", collide: "item: {}\n\" item \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalEntityDeclarations(path)
				return len(value), err
			},
		},
		{
			name: "events", fileName: "events.yaml", valid: "item.created: {}\n", merged: "<<: &declarations\n  item.created: {}\n", blank: "\"\": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalEventCatalog(path)
				return len(value), err
			},
		},
		{
			name: "nodes", fileName: "nodes.yaml", valid: "worker: {}\n", merged: "<<: &declarations\n  worker: {}\n", blank: "\"\": {}\n", collide: "worker: {}\n\" worker \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalNodeDeclarations(path)
				return len(value), err
			},
		},
		{
			name: "policy", fileName: "policy.yaml", valid: "limit: {}\n", merged: "<<: &declarations\n  limit:\n    value: 7\n", blank: "\"\": {}\n", collide: "limit: {}\n\" limit \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalPolicyDeclarations(path)
				return len(value.Values) + len(value.Criteria) + len(value.Validation) + len(value.Modules), err
			},
		},
		{
			name: "tools", fileName: "tools.yaml", valid: "lookup: {}\n", merged: "<<: &declarations\n  lookup: {}\n", blank: "\"\": {}\n", collide: "lookup: {}\n\" lookup \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalToolDeclarations(path)
				return len(value), err
			},
		},
		{
			name: "types", fileName: "types.yaml", valid: "types:\n  Item: {}\n", merged: "<<: &catalog\n  types:\n    Item: {}\n", blank: "types:\n  \"\": {}\n", collide: "types:\n  Item: {}\n  \" Item \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalTypeDeclarations(path)
				return len(value.Scalars) + len(value.Enums) + len(value.Types), err
			},
		},
	}
}

func TestOptionalDeclarationAdmissionOwnsAbsentPresentAndRootShapeMatrix(t *testing.T) {
	states := []struct {
		name         string
		body         string
		wantCode     string
		wantPresence string
	}{
		{name: "empty mapping", body: "{}\n", wantCode: "contract_loader.optional_declaration_file_empty"},
		{name: "null", body: "null\n", wantCode: "contract_loader.optional_declaration_file_empty"},
		{name: "tilde null", body: "~\n", wantCode: "contract_loader.optional_declaration_file_empty"},
		{name: "explicit empty document", body: "---\n", wantCode: "contract_loader.optional_declaration_file_empty"},
		{name: "comment only", body: "# no declarations\n", wantCode: "contract_loader.yaml_parse"},
		{name: "scalar", body: "value\n", wantCode: "contract_loader.optional_declaration_file_shape", wantPresence: "scalar"},
		{name: "double-quoted empty scalar", body: "\"\"\n", wantCode: "contract_loader.optional_declaration_file_shape", wantPresence: "empty_scalar"},
		{name: "single-quoted empty scalar", body: "''\n", wantCode: "contract_loader.optional_declaration_file_shape", wantPresence: "empty_scalar"},
		{name: "empty sequence", body: "[]\n", wantCode: "contract_loader.optional_declaration_file_shape", wantPresence: "empty_sequence"},
		{name: "sequence", body: "- value\n", wantCode: "contract_loader.optional_declaration_file_shape", wantPresence: "sequence"},
	}
	for _, role := range optionalDeclarationRoleTestCases() {
		role := role
		t.Run(role.name, func(t *testing.T) {
			t.Parallel()
			if count, err := role.load(""); err != nil || count != 0 {
				t.Fatalf("absent result = (%d, %v), want zero declarations", count, err)
			}
			root := t.TempDir()
			validPath := filepath.Join(root, role.fileName)
			writeFixtureFile(t, validPath, role.valid)
			if count, err := role.load(validPath); err != nil || count != 1 {
				t.Fatalf("valid result = (%d, %v), want one declaration", count, err)
			}
			for _, state := range states {
				state := state
				t.Run(state.name, func(t *testing.T) {
					path := filepath.Join(t.TempDir(), role.fileName)
					writeFixtureFile(t, path, state.body)
					_, err := role.load(path)
					assertOptionalDeclarationRootDiagnostic(t, err, path, role.fileName, state.wantCode, state.wantPresence)
				})
			}
		})
	}
}

func assertOptionalDeclarationRootDiagnostic(t *testing.T, err error, path, fileName, code, presence string) {
	t.Helper()
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok {
		t.Fatalf("diagnostic = %#v, %v, want %s at %s", diagnostic, err, code, path)
	}
	wantProblem := ""
	wantRemediation := ""
	wantYAMLPath := fileName
	switch code {
	case "contract_loader.optional_declaration_file_empty":
		wantProblem = fileName + " declares nothing - delete the file (absent means empty)."
		wantRemediation = "Delete the file. Optional workflow declaration files exist only when they contain at least one declaration."
	case "contract_loader.optional_declaration_file_shape":
		wantProblem = fileName + " must contain a declaration mapping, found " + presence + "."
		wantRemediation = "Use a mapping keyed by declaration name, or delete the optional file when it has no declarations."
	case "contract_loader.yaml_parse":
		wantProblem = "contract YAML could not be parsed."
		wantRemediation = "Fix the YAML syntax, then run the command again."
		wantYAMLPath = ""
	default:
		t.Fatalf("unsupported diagnostic expectation %q", code)
	}
	if diagnostic.Code != code ||
		filepath.Clean(diagnostic.Location.File) != filepath.Clean(path) ||
		diagnostic.Location.YAMLPath != wantYAMLPath ||
		diagnostic.Problem != wantProblem ||
		diagnostic.Remediation != wantRemediation {
		t.Fatalf("diagnostic = %#v, want code=%q file=%q yaml_path=%q problem=%q remediation=%q", diagnostic, code, path, wantYAMLPath, wantProblem, wantRemediation)
	}
}

func TestOptionalDeclarationAdmissionUsesMergeExpandedProjectionForAllRoles(t *testing.T) {
	for _, role := range optionalDeclarationRoleTestCases() {
		role := role
		t.Run(role.name+"/merge-only", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), role.fileName)
			writeFixtureFile(t, path, "<<: &empty {}\n")
			_, err := role.load(path)
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok || diagnostic.Code != "contract_loader.optional_declaration_file_empty" || filepath.Clean(diagnostic.Location.File) != filepath.Clean(path) {
				t.Fatalf("merge-only diagnostic = %#v, %v", diagnostic, err)
			}
		})
		t.Run(role.name+"/positive-merge", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), role.fileName)
			writeFixtureFile(t, path, role.merged)
			count, err := role.load(path)
			if err != nil || count != 1 {
				t.Fatalf("merged result = (%d, %v), want one real declaration", count, err)
			}
			assertMergedDeclarationIdentity(t, role.name, path)
		})
	}
}

func assertMergedDeclarationIdentity(t *testing.T, role, path string) {
	t.Helper()
	switch role {
	case "agents":
		value, err := loadOptionalAgentDeclarations(path)
		_, hasDeclaration := value["worker"]
		_, hasPseudoKey := value["<<"]
		if err != nil || !hasDeclaration || hasPseudoKey {
			t.Fatalf("merged agents = %#v, %v", value, err)
		}
	case "entities":
		value, err := loadOptionalEntityDeclarations(path)
		_, hasDeclaration := value["item"]
		_, hasPseudoKey := value["<<"]
		if err != nil || !hasDeclaration || hasPseudoKey {
			t.Fatalf("merged entities = %#v, %v", value, err)
		}
	case "events":
		value, err := loadOptionalEventCatalog(path)
		_, hasDeclaration := value["item.created"]
		_, hasPseudoKey := value["<<"]
		if err != nil || !hasDeclaration || hasPseudoKey {
			t.Fatalf("merged events = %#v, %v", value, err)
		}
	case "nodes":
		value, err := loadOptionalNodeDeclarations(path)
		if err != nil {
			t.Fatalf("merged nodes = %#v, %v", value, err)
		}
		if _, ok := value["worker"]; !ok {
			t.Fatalf("merged nodes = %#v, want worker", value)
		}
		if _, ok := value["<<"]; ok {
			t.Fatalf("merged nodes published pseudo-key: %#v", value)
		}
	case "policy":
		value, err := loadOptionalPolicyDeclarations(path)
		_, hasPseudoKey := value.Values["<<"]
		if err != nil || value.Values["limit"].Value != 7 || hasPseudoKey {
			t.Fatalf("merged policy = %#v, %v", value, err)
		}
	case "tools":
		value, err := loadOptionalToolDeclarations(path)
		if err != nil {
			t.Fatalf("merged tools = %#v, %v", value, err)
		}
		if _, ok := value["lookup"]; !ok {
			t.Fatalf("merged tools = %#v, want lookup", value)
		}
		if _, ok := value["<<"]; ok {
			t.Fatalf("merged tools published pseudo-key: %#v", value)
		}
	case "types":
		value, err := loadOptionalTypeDeclarations(path)
		if err != nil {
			t.Fatalf("merged types = %#v, %v", value, err)
		}
		if _, ok := value.Types["Item"]; !ok {
			t.Fatalf("merged types = %#v, want Item", value)
		}
		if _, ok := value.Types["<<"]; ok {
			t.Fatalf("merged types published pseudo-key: %#v", value)
		}
	}
}

func TestOptionalDeclarationAdmissionRejectsBlankAndNormalizedCollidingNames(t *testing.T) {
	for _, role := range optionalDeclarationRoleTestCases() {
		role := role
		t.Run(role.name+"/blank", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), role.fileName)
			writeFixtureFile(t, path, role.blank)
			_, err := role.load(path)
			if err == nil {
				t.Fatal("blank declaration name admitted")
			}
			if role.name != "events" {
				diagnostic, ok := AsLoaderDiagnostic(err)
				if !ok || diagnostic.Code != "contract_loader.declaration_name_invalid" || diagnostic.Location.Line == 0 {
					t.Fatalf("blank diagnostic = %#v, %v", diagnostic, err)
				}
			}
		})
		if role.collide == "" {
			continue
		}
		t.Run(role.name+"/collision", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), role.fileName)
			writeFixtureFile(t, path, role.collide)
			_, err := role.load(path)
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok || diagnostic.Code != "contract_loader.declaration_name_collision" || !strings.Contains(err.Error(), "collide") {
				t.Fatalf("collision diagnostic = %#v, %v", diagnostic, err)
			}
		})
	}
}

func TestOptionalDeclarationAdmissionRejectsEmptyTypedContainers(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		body     string
		load     func(string) error
	}{
		{
			name: "types empty maps", fileName: "types.yaml", body: "scalars: {}\nenums: {}\ntypes: {}\n",
			load: func(path string) error { _, err := loadOptionalTypeDeclarations(path); return err },
		},
		{
			name: "types null containers", fileName: "types.yaml", body: "scalars: null\nenums: null\ntypes: null\n",
			load: func(path string) error { _, err := loadOptionalTypeDeclarations(path); return err },
		},
		{
			name: "policy empty maps", fileName: "policy.yaml", body: "criteria: {}\nvalidation: {}\nmodules: {}\n",
			load: func(path string) error { _, err := loadOptionalPolicyDeclarations(path); return err },
		},
		{
			name: "policy null containers", fileName: "policy.yaml", body: "criteria: null\nvalidation: null\nmodules: null\n",
			load: func(path string) error { _, err := loadOptionalPolicyDeclarations(path); return err },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), test.fileName)
			writeFixtureFile(t, path, test.body)
			err := test.load(path)
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok || diagnostic.Code != "contract_loader.optional_declaration_file_empty" {
				t.Fatalf("diagnostic = %#v, %v", diagnostic, err)
			}
		})
	}
}

func TestOptionalDeclarationAdmissionPreservesSpecificTypedDiagnostics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "types.yaml")
	writeFixtureFile(t, path, "unsupported: {}\n")
	_, err := loadOptionalTypeDeclarations(path)
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok || diagnostic.Code != "contract_loader.undefined_field" {
		t.Fatalf("diagnostic = %#v, %v, want typed undefined-field result", diagnostic, err)
	}
}

func TestOptionalDeclarationAdmissionIsConsumedAtSupportedScopes(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	for _, scope := range []struct {
		name          string
		supportedRole func(string) bool
		targetRoot    func(string) string
	}{
		{
			name:          "root package",
			supportedRole: func(string) bool { return true },
			targetRoot:    func(root string) string { return root },
		},
		{
			name: "nested package",
			supportedRole: func(role string) bool {
				return role != "types" && role != "entities"
			},
			targetRoot: func(root string) string { return filepath.Join(root, "packages", "child") },
		},
		{
			name:          "flow",
			supportedRole: func(string) bool { return true },
			targetRoot:    func(root string) string { return filepath.Join(root, "flows", "child") },
		},
	} {
		for _, role := range optionalDeclarationRoleTestCases() {
			if !scope.supportedRole(role.name) {
				continue
			}
			scope, role := scope, role
			for _, document := range []struct {
				name string
				body string
			}{
				{name: "direct-empty", body: "{}\n"},
				{name: "merge-only", body: "<<: &empty {}\n"},
			} {
				document := document
				t.Run(scope.name+"/"+role.name+"/"+document.name, func(t *testing.T) {
					root := t.TempDir()
					writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: optional-declaration-scope
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: packages/child
flows:
  - {id: child, flow: child, mode: static}
`)
					writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: optional-declaration-scope\n")
					packageRoot := filepath.Join(root, "packages", "child")
					writeFixtureFile(t, filepath.Join(packageRoot, "package.yaml"), "name: child-package\nversion: \"1.0.0\"\nflows: []\n")
					flowRoot := filepath.Join(root, "flows", "child")
					writeFixtureFile(t, filepath.Join(flowRoot, "package.yaml"), "name: child\nversion: \"1.0.0\"\nflows: []\n")
					writeFixtureFile(t, filepath.Join(flowRoot, "schema.yaml"), "name: child\nmode: static\ninitial_state: active\nstates: [active]\n")
					targetRoot := scope.targetRoot(root)
					target := filepath.Join(targetRoot, role.fileName)
					writeFixtureFile(t, target, document.body)

					bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
					diagnostic, ok := AsLoaderDiagnostic(err)
					if bundle != nil || !ok || diagnostic.Code != "contract_loader.optional_declaration_file_empty" || filepath.Clean(diagnostic.Location.File) != filepath.Clean(target) {
						t.Fatalf("bundle = %#v, diagnostic = %#v, %v, want exact %s failure", bundle, diagnostic, err, target)
					}
				})
			}
		}
	}
}

func TestMergeOnlyCustomDocumentsFailBeforeBundlePublicationAtEverySupportedScope(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	for _, test := range []struct {
		name   string
		target func(string) string
	}{
		{name: "root/entities", target: func(root string) string { return filepath.Join(root, "entities.yaml") }},
		{name: "root/types", target: func(root string) string { return filepath.Join(root, "types.yaml") }},
		{name: "root/policy", target: func(root string) string { return filepath.Join(root, "policy.yaml") }},
		{name: "project/policy", target: func(root string) string { return filepath.Join(root, "packages", "child", "policy.yaml") }},
		{name: "flow/entities", target: func(root string) string { return filepath.Join(root, "flows", "child", "entities.yaml") }},
		{name: "flow/types", target: func(root string) string { return filepath.Join(root, "flows", "child", "types.yaml") }},
		{name: "flow/policy", target: func(root string) string { return filepath.Join(root, "flows", "child", "policy.yaml") }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			root := writeMergeProjectionBundleSkeleton(t)
			target := test.target(root)
			writeFixtureFile(t, target, "<<: &empty {}\n")

			bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
			diagnostic, ok := AsLoaderDiagnostic(err)
			if bundle != nil || !ok || diagnostic.Code != "contract_loader.optional_declaration_file_empty" || filepath.Clean(diagnostic.Location.File) != filepath.Clean(target) {
				t.Fatalf("bundle = %#v, diagnostic = %#v, error = %v", bundle, diagnostic, err)
			}
		})
	}
}

func TestPositiveMergedDeclarationsPublishRealRootIdentities(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	for _, role := range optionalDeclarationRoleTestCases() {
		role := role
		t.Run(role.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "package.yaml"), "name: merge-publication\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n")
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: merge-publication\n")
			writeFixtureFile(t, filepath.Join(root, role.fileName), role.merged)

			bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			assertMergedBundleIdentity(t, role.name, bundle)
		})
	}
}

func writeMergeProjectionBundleSkeleton(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: merge-projection
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: packages/child
flows:
  - {id: child, flow: child, mode: static}
`)
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: merge-projection\n")
	writeFixtureFile(t, filepath.Join(root, "packages", "child", "package.yaml"), "name: child-package\nversion: \"1.0.0\"\nflows: []\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "child", "package.yaml"), "name: child\nversion: \"1.0.0\"\nflows: []\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), "name: child\nmode: static\ninitial_state: active\nstates: [active]\n")
	return root
}

func assertMergedBundleIdentity(t *testing.T, role string, bundle *WorkflowContractBundle) {
	t.Helper()
	switch role {
	case "agents":
		assertRegistryIdentity(t, bundle.Agents, "worker")
	case "entities":
		assertRegistryIdentity(t, bundle.RootEntities, "item")
	case "events":
		assertRegistryIdentity(t, bundle.Events, "item.created")
	case "nodes":
		assertRegistryIdentity(t, bundle.Nodes, "worker")
	case "policy":
		policy := rootWorkflowPolicy(bundle)
		assertRegistryIdentity(t, policy.Values, "limit")
		if policy.Values["limit"].Value != 7 {
			t.Fatalf("merged policy limit = %#v, want 7", policy.Values["limit"].Value)
		}
	case "tools":
		assertRegistryIdentity(t, bundle.Tools, "lookup")
	case "types":
		assertRegistryIdentity(t, bundle.RootTypes.Types, "Item")
	}
}

func assertRegistryIdentity[T any](t *testing.T, registry map[string]T, want string) {
	t.Helper()
	if _, ok := registry[want]; !ok {
		t.Fatalf("registry = %#v, want %q", registry, want)
	}
	if _, ok := registry["<<"]; ok {
		t.Fatalf("registry published merge pseudo-key: %#v", registry)
	}
}

func TestPresentZeroPackageScopedTypesAndEntitiesRemainRetired(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	for _, fileName := range []string{"types.yaml", "entities.yaml"} {
		fileName := fileName
		t.Run(fileName, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: retired-package-contract-scope
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: packages/child
flows: []
`)
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: retired-package-contract-scope\n")
			packageRoot := filepath.Join(root, "packages", "child")
			writeFixtureFile(t, filepath.Join(packageRoot, "package.yaml"), "name: child\nversion: \"1.0.0\"\nflows: []\n")
			writeFixtureFile(t, filepath.Join(packageRoot, fileName), "{}\n")

			_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
			if err == nil || !strings.Contains(err.Error(), "RETIRED: package-scoped "+fileName+" is not supported") {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want retired %s rejection", err, fileName)
			}
		})
	}
}

func TestRepositoryContainsNoPresentZeroOptionalDeclarationFiles(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	roles := make(map[string]optionalDeclarationRoleTestCase)
	for _, role := range optionalDeclarationRoleTestCases() {
		roles[role.fileName] = role
	}
	checked := 0
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		role, ok := roles[entry.Name()]
		if !ok {
			return nil
		}
		checked++
		_, loadErr := role.load(path)
		diagnostic, ok := AsLoaderDiagnostic(loadErr)
		if ok && diagnostic.Code == "contract_loader.optional_declaration_file_empty" {
			return fmt.Errorf("%s: %w", strings.TrimPrefix(path, repoRoot+string(filepath.Separator)), loadErr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("optional declaration corpus census found no files")
	}
}
