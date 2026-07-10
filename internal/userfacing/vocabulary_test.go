package userfacing

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
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
	for _, relativeRoot := range []string{"cmd/swarm", "internal/runtime/bootverify", "internal/runtime/contracts"} {
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
					t.Errorf("unquote %s: %v", fileSet.Position(literal.Pos()), err)
					return true
				}
				if found := FindForbidden(ProfileOperatorOutput, value); len(found) > 0 {
					t.Errorf("%s user-facing string literal %q contains globally forbidden terms %v", fileSet.Position(literal.Pos()), value, found)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", relativeRoot, err)
		}
	}
}
