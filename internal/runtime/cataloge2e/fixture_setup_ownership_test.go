package cataloge2e

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestCatalogScenarioRunFixturesPrecedeRuntimeConstruction(t *testing.T) {
	dir := filepath.Join(repoRootFromCatalogE2E(t), "internal/runtime/cataloge2e")
	files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, path := range files {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		aliases := map[string]bool{}
		for _, imp := range file.Imports {
			value, err := strconv.Unquote(imp.Path.Value)
			if err == nil && value == "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture" {
				alias := "runlifecyclefixture"
				if imp.Name != nil {
					alias = imp.Name.Name
				}
				if alias == "." {
					t.Fatal("scenario fixture import must retain explicit ownership")
				}
				aliases[alias] = true
			}
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var construction token.Pos
			var fixtureCalls []*ast.CallExpr
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				owner, ok := selector.X.(*ast.Ident)
				if !ok {
					return true
				}
				if selector.Sel.Name == "NewValidationHarnessRuntime" {
					construction = call.Pos()
				}
				if aliases[owner.Name] && (strings.HasPrefix(selector.Sel.Name, "Require") || selector.Sel.Name == "Materialize") {
					fixtureCalls = append(fixtureCalls, call)
					seen[selector.Sel.Name]++
				}
				return true
			})
			for _, call := range fixtureCalls {
				if filepath.Base(path) != "runtime_harness_test.go" || fn.Name.Name != "newRuntimeHarnessWithTerminalProvider" ||
					!construction.IsValid() || call.Pos() >= construction {
					t.Errorf("%s:%s admits scenario fixtures outside pre-construction setup", filepath.Base(path), fn.Name.Name)
				}
			}
		}
	}
	if len(seen) != 2 || seen["RequirePostgres"] != 1 || seen["RequireSQLite"] != 1 {
		t.Fatalf("scenario fixture writers=%v, want the two pre-construction backend calls", seen)
	}
}
