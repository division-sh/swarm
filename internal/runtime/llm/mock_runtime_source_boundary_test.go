package llm

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestProductionDoesNotImportCatalogOrFixtureRuntimeOwners(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve mock runtime source boundary test path")
	}
	repo := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	violations, err := mockRuntimeImportViolations(repo)
	if err != nil {
		t.Fatal(err)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
}

func TestMockRuntimeImportCensusExcludesNestedCheckout(t *testing.T) {
	repo := t.TempDir()
	var locals []string
	for _, rootName := range []string{"cmd", "internal"} {
		root := filepath.Join(repo, rootName)
		local := filepath.Join(root, "current", "hostile.go")
		foreign := filepath.Join(root, "foreign")
		for _, path := range []string{local, filepath.Join(foreign, "hostile.go")} {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("package hostile\nimport _ \"github.com/division-sh/swarm/internal/runtime/cataloge2e\"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(foreign, ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		locals = append(locals, local)
	}
	violations, err := mockRuntimeImportViolations(repo)
	if err != nil || len(violations) != len(locals) {
		t.Fatalf("mock-runtime import violations = %v, %v; want %d current-local imports", violations, err, len(locals))
	}
	for _, path := range locals {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	violations, err = mockRuntimeImportViolations(repo)
	if err != nil || len(violations) != 0 {
		t.Fatalf("foreign-only mock-runtime imports were rejected: %v, %v", violations, err)
	}
}

func mockRuntimeImportViolations(repo string) ([]string, error) {
	var violations []string
	for _, root := range []string{filepath.Join(repo, "cmd"), filepath.Join(repo, "internal")} {
		err := checkoutsource.WalkDir(repo, root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				rel, err := filepath.Rel(repo, path)
				if err != nil {
					return err
				}
				rel = filepath.ToSlash(rel)
				if rel == "internal/runtime/cataloge2e" || rel == "internal/runtime/testfixtures" {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, imported := range parsed.Imports {
				value, err := strconv.Unquote(imported.Path.Value)
				if err != nil {
					return err
				}
				if strings.Contains(value, "/internal/runtime/cataloge2e") || strings.Contains(value, "/internal/runtime/testfixtures/") {
					violations = append(violations, "production source "+filepath.ToSlash(strings.TrimPrefix(path, repo+string(filepath.Separator)))+" imports private test owner "+value)
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return violations, nil
}
