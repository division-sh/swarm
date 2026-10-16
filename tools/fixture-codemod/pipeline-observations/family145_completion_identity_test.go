package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145CompletionCardinalityAssertion(source string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "completion.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return err
	}
	var found bool
	ast.Inspect(file, func(node ast.Node) bool {
		conditional, ok := node.(*ast.IfStmt)
		if !ok || family145NodeString(conditional.Cond) != "len(ids) != 1" {
			return true
		}
		want := `t.Fatalf("events %s for run %s = %#v, want exactly one", eventName, runID, ids)`
		found = len(conditional.Body.List) == 1 && family145NodeString(conditional.Body.List[0]) == want
		return false
	})
	if !found {
		return fmt.Errorf("complete zero/multiple physical identity refusal changed")
	}
	return nil
}
