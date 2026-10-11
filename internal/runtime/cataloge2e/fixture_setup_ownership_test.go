package cataloge2e

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
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
		runtimeAliases := map[string]bool{}
		for _, imp := range file.Imports {
			value, err := strconv.Unquote(imp.Path.Value)
			if err == nil && value == "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture" {
				t.Errorf("%s retains raw scenario fixture authority", filepath.Base(path))
			}
			if err == nil && value == "github.com/division-sh/swarm/internal/store/storetest" {
				alias := "storetest"
				if imp.Name != nil {
					alias = imp.Name.Name
				}
				if alias == "." {
					t.Fatal("scenario fixture import must retain explicit ownership")
				}
				aliases[alias] = true
			}
			if err == nil && value == "github.com/division-sh/swarm/internal/runtime" {
				alias := "runtime"
				if imp.Name != nil {
					alias = imp.Name.Name
				}
				if alias == "." {
					t.Fatal("runtime import must retain explicit ownership")
				}
				runtimeAliases[alias] = true
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
				if runtimeAliases[owner.Name] && selector.Sel.Name == "NewRuntime" {
					construction = call.Pos()
				}
				if aliases[owner.Name] && (selector.Sel.Name == "RequireRun" || selector.Sel.Name == "MaterializeRun") {
					fixtureCalls = append(fixtureCalls, call)
				}
				return true
			})
			// Pure readback fixtures do not construct executable runtime work.
			if !construction.IsValid() {
				continue
			}
			for _, call := range fixtureCalls {
				seen[call.Fun.(*ast.SelectorExpr).Sel.Name]++
				if filepath.Base(path) != "runtime_harness_test.go" || fn.Name.Name != "newRuntimeHarnessWithTerminalProvider" ||
					!construction.IsValid() || call.Pos() >= construction {
					t.Errorf("%s:%s admits scenario fixtures outside pre-construction setup", filepath.Base(path), fn.Name.Name)
				}
			}
		}
	}
	if len(seen) != 1 || seen["RequireRun"] != 1 {
		t.Fatalf("scenario fixture writers=%v, want one pre-construction selected lifecycle owner call", seen)
	}
}
