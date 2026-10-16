package main

import (
	"go/ast"
	"go/parser"
	"go/token"
)

func sourceOutcomeAST(source string) *ast.File {
	file, err := parser.ParseFile(token.NewFileSet(), "outcome.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		panic(err)
	}
	return file
}

func sourceOutcomePreparationCalls(source, name string) []string {
	var out []string
	ast.Inspect(sourceOutcomeAST(source), func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		var called string
		switch function := call.Fun.(type) {
		case *ast.Ident:
			called = function.Name
		case *ast.SelectorExpr:
			called = function.Sel.Name
		}
		if called == name {
			out = append(out, family145NodeString(call))
		}
		return true
	})
	return out
}
