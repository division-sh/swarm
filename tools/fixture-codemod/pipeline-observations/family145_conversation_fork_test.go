package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

func family145ConversationForkOwnerSource(t *testing.T, path, function string) string {
	t.Helper()
	data, err := os.ReadFile("../../../internal/store/" + path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == function {
			return servedSetupEarlierSource(t, filepath.ToSlash(filepath.Clean(filepath.Join("internal/store", path))), function, family145NodeString(fn))
		}
	}
	t.Fatalf("missing original row-family owner port: %s", function)
	return ""
}
