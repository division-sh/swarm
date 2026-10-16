package serveapp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestServeActivityRecoveryPrecedesBothDispatchStartupPaths(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var run *ast.FuncDecl
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == "Run" {
			run = function
		}
	}
	if run == nil {
		t.Fatal("serve entrypoint is absent")
	}
	positions := map[string][]token.Pos{}
	ast.Inspect(run.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch function := call.Fun.(type) {
		case *ast.Ident:
			positions[function.Name] = append(positions[function.Name], call.Pos())
		case *ast.SelectorExpr:
			positions[function.Sel.Name] = append(positions[function.Sel.Name], call.Pos())
		}
		return true
	})
	for _, name := range []string{"installServeSourceSet", "recoverServeDisplacedHTTPActivities", "buildSelectedAPICapabilities", "RecoverSelectedForkContexts", "prepareServeRuntimeContexts"} {
		if len(positions[name]) != 1 {
			t.Fatalf("serve must compose exactly one %s before dispatch", name)
		}
	}
	recovery := positions["recoverServeDisplacedHTTPActivities"][0]
	if positions["installServeSourceSet"][0] >= recovery {
		t.Fatal("activity recovery preceded authoritative source installation")
	}
	for _, name := range []string{"buildSelectedAPICapabilities", "RecoverSelectedForkContexts", "prepareServeRuntimeContexts"} {
		if recovery >= positions[name][0] {
			t.Fatalf("activity recovery followed %s", name)
		}
	}
}
