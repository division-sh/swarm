package userfacing

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestForbiddenTermsAreScopedAndGlobalTermsUseStableBoundaries(t *testing.T) {
	if found := FindForbidden(ProfileOperatorOutput, "Wave 1 contracts"); len(found) != 1 || found[0] != "Wave 1" {
		t.Fatalf("Wave 1 matches = %v", found)
	}
	if found := FindForbidden(ProfileOperatorOutput, "load the unified config"); len(found) != 1 || found[0] != "unified" {
		t.Fatalf("unified matches = %v", found)
	}
	if found := FindForbidden(ProfileOperatorOutput, "unified_config_owner"); len(found) != 0 {
		t.Fatalf("internal identifier should not match whole-word user-facing term: %v", found)
	}
	if found := FindForbidden(ProfileStatusDetail, "next_cursor"); len(found) != 1 || found[0] != "next_cursor" {
		t.Fatalf("status-detail matches = %v", found)
	}
	if found := FindForbidden(ProfileOperatorOutput, "next_cursor"); len(found) != 0 {
		t.Fatalf("status-detail-only term leaked into operator profile: %v", found)
	}
}

func TestProductionUserFacingStringLiteralsAvoidGlobalForbiddenTerms(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	relativeRoots := []string{"cmd/swarm", "internal/cliapp", "internal/serveapp", "internal/runtime/bootverify", "internal/runtime/contracts"}
	failures, err := forbiddenProductionStringLiterals(repoRoot, relativeRoots)
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range failures {
		t.Error(failure)
	}
}

func TestProductionUserFacingStringGuardRejectsEachRelocatedOwner(t *testing.T) {
	for _, relativeRoot := range []string{"internal/cliapp", "internal/serveapp"} {
		t.Run(relativeRoot, func(t *testing.T) {
			repoRoot := t.TempDir()
			root := filepath.Join(repoRoot, relativeRoot)
			if err := os.MkdirAll(root, 0o755); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "forbidden_fixture.go")
			if err := os.WriteFile(path, []byte("package fixture\nconst output = \"Wave 1 contracts\"\n"), 0o600); err != nil {
				t.Fatal(err)
			}

			failures, err := forbiddenProductionStringLiterals(repoRoot, []string{relativeRoot})
			if err != nil {
				t.Fatal(err)
			}
			if len(failures) != 1 || !strings.Contains(failures[0], "Wave 1") {
				t.Fatalf("failures = %v, want one Wave 1 rejection", failures)
			}
		})
	}
}

func forbiddenProductionStringLiterals(repoRoot string, relativeRoots []string) ([]string, error) {
	var failures []string
	for _, relativeRoot := range relativeRoots {
		root := filepath.Join(repoRoot, relativeRoot)
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fileSet := token.NewFileSet()
			file, err := parser.ParseFile(fileSet, path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					failures = append(failures, fmt.Sprintf("unquote %s: %v", fileSet.Position(literal.Pos()), err))
					return true
				}
				if found := FindForbidden(ProfileOperatorOutput, value); len(found) > 0 {
					failures = append(failures, fmt.Sprintf("%s user-facing string literal %q contains globally forbidden terms %v", fileSet.Position(literal.Pos()), value, found))
				}
				return true
			})
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", relativeRoot, err)
		}
	}
	return failures, nil
}
