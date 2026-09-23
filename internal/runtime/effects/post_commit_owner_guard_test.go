package effects

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestPostCommitMutationErrorHasOnlyCanonicalProductionProducers(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		aliases := map[string]bool{}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if importPath != "github.com/division-sh/swarm/internal/runtime/effects" {
				continue
			}
			if spec.Name != nil && spec.Name.Name == "." {
				t.Errorf("%s: dot-import of effect commit marker owner is forbidden", fset.Position(spec.Pos()))
				continue
			}
			alias := "effects"
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			aliases[alias] = true
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "NewPostCommitMutationError" {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if ok && aliases[pkg.Name] && !strings.Contains(filepath.ToSlash(path), "/store/internal/backend/effectpersistence/") {
				t.Errorf("%s: effect commit marker must originate in the persistence owner", fset.Position(call.Pos()))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
