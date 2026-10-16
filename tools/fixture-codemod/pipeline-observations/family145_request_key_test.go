package main

import (
	_ "embed"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

//go:embed family145-request-key-preimages.json
var family145RequestKeyBytes []byte

func family145RequestKeyCallerInverse(function, current string) string {
	pins, err := requestkey.Decode(family145RequestKeyBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			current = family145RequestKeyCurrentPrevious(pin.File, function, current)
			return requestkey.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func requestKeyDeclaration(t *testing.T, raw []byte, name string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "request-key.go", raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && requestkey.Key(fn) == name {
			return requestkey.Node(fn)
		}
	}
	return ""
}
