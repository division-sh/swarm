package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"
)

func workflowLifecycleFunctionSource(t *testing.T, path, name string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != name {
			continue
		}
		start := set.Position(function.Pos()).Offset
		end := set.Position(function.End()).Offset
		return string(raw[start:end])
	}
	t.Fatalf("function %s not found in %s", name, path)
	return ""
}
