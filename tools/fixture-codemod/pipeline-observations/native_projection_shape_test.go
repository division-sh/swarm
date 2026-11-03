package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func projectionShapeFunction(t *testing.T, source string) *ast.FuncDecl {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "shape.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	return file.Decls[0].(*ast.FuncDecl)
}

func projectionShapeStatement(t *testing.T, source string) ast.Stmt {
	t.Helper()
	return projectionShapeFunction(t, "func statement(){"+source+"}").Body.List[0]
}
