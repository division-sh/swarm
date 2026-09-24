package bus

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestCommittedPublicationOutcomeHasOneBusInterpreter(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "WithCommitOutcome" {
					return true
				}
				count++
				if function.Name.Name != "finalizeCommittedPublicationConsequences" {
					t.Errorf("%s: %s independently interprets committed publication outcome", name, function.Name.Name)
				}
				return true
			})
		}
	}
	if count != 1 {
		t.Errorf("committed publication outcome interpreters=%d, want one", count)
	}
}
