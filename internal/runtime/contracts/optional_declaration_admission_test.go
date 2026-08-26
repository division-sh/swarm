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
	blank    string
	collide  string
	load     func(string) (int, error)
}

func optionalDeclarationRoleTestCases() []optionalDeclarationRoleTestCase {
	return []optionalDeclarationRoleTestCase{
		{
			name: "agents", fileName: "agents.yaml", valid: "worker: {}\n", blank: "\"\": {}\n", collide: "worker: {}\n\" worker \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalAgentDeclarations(path)
				return len(value), err
			},
		},
		{
			name: "entities", fileName: "entities.yaml", valid: "item: {}\n", blank: "\"\": {}\n", collide: "item: {}\n\" item \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalEntityDeclarations(path)
				return len(value), err
			},
		},
		{
			name: "events", fileName: "events.yaml", valid: "item.created: {}\n", blank: "\"\": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalEventCatalog(path)
				return len(value), err
			},
		},
		{
			name: "nodes", fileName: "nodes.yaml", valid: "worker: {}\n", blank: "\"\": {}\n", collide: "worker: {}\n\" worker \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalNodeDeclarations(path)
				return len(value), err
			},
		},
		{
			name: "policy", fileName: "policy.yaml", valid: "limit: {}\n", blank: "\"\": {}\n", collide: "limit: {}\n\" limit \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalPolicyDeclarations(path)
				return len(value.Values) + len(value.Criteria) + len(value.Validation) + len(value.Modules), err
			},
		},
		{
			name: "tools", fileName: "tools.yaml", valid: "lookup: {}\n", blank: "\"\": {}\n", collide: "lookup: {}\n\" lookup \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalToolDeclarations(path)
				return len(value), err
			},
		},
		{
			name: "types", fileName: "types.yaml", valid: "types:\n  Item: {}\n", blank: "types:\n  \"\": {}\n", collide: "types:\n  Item: {}\n  \" Item \": {}\n",
			load: func(path string) (int, error) {
				value, err := loadOptionalTypeDeclarations(path)
				return len(value.Scalars) + len(value.Enums) + len(value.Types), err
			},
		},
	}
}

func TestOptionalDeclarationAdmissionOwnsAbsentPresentAndRootShapeMatrix(t *testing.T) {
	states := []struct {
		name string
		body string
	}{
		{name: "empty mapping", body: "{}\n"},
		{name: "null", body: "null\n"},
		{name: "tilde null", body: "~\n"},
		{name: "comment only", body: "# no declarations\n"},
		{name: "scalar", body: "value\n"},
		{name: "empty sequence", body: "[]\n"},
		{name: "sequence", body: "- value\n"},
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
					if _, err := role.load(path); err == nil {
						t.Fatalf("%s admitted", state.name)
					}
				})
			}
		})
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

func TestOptionalDeclarationAdmissionIsConsumedAtProjectAndFlowScopes(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	for _, scope := range []string{"project", "flow"} {
		for _, role := range optionalDeclarationRoleTestCases() {
			scope, role := scope, role
			t.Run(scope+"/"+role.name, func(t *testing.T) {
				root := t.TempDir()
				writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: optional-declaration-scope
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - {id: child, flow: child, mode: static}
`)
				writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: optional-declaration-scope\n")
				flowRoot := filepath.Join(root, "flows", "child")
				writeFixtureFile(t, filepath.Join(flowRoot, "package.yaml"), "name: child\nversion: \"1.0.0\"\nflows: []\n")
				writeFixtureFile(t, filepath.Join(flowRoot, "schema.yaml"), "name: child\nmode: static\ninitial_state: active\nstates: [active]\n")
				targetRoot := root
				if scope == "flow" {
					targetRoot = flowRoot
				}
				target := filepath.Join(targetRoot, role.fileName)
				writeFixtureFile(t, target, "{}\n")

				_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
				diagnostic, ok := AsLoaderDiagnostic(err)
				if !ok || diagnostic.Code != "contract_loader.optional_declaration_file_empty" || filepath.Clean(diagnostic.Location.File) != filepath.Clean(target) {
					t.Fatalf("diagnostic = %#v, %v, want exact %s failure", diagnostic, err, target)
				}
			})
		}
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
