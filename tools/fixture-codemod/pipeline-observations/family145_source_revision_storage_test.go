package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
)

func family145SourceProjectedMarker(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	fn, err := uniqueFunction(file, "ReadSourceRevisionDiagnosticStorageTx")
	if err != nil {
		return err
	}
	assignments := 0
	var correct bool
	ast.Inspect(fn, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || len(assignment.Lhs) != 1 || family145NodeString(assignment.Lhs[0]) != "row.Projected" {
			return true
		}
		assignments++
		correct = len(assignment.Rhs) == 1 && family145NodeString(assignment.Rhs[0]) == "at != nil"
		return true
	})
	if assignments != 1 || !correct {
		return fmt.Errorf("physical projected-at NULL presence must have one exact source")
	}
	return nil
}
