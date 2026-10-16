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

func family145TimerStartupOwnerError(source string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "timer-startup.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	counts := map[string]int{}
	var refusal error
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch selector.Sel.Name {
		case "Database", "DatabaseForTest", "AdmitPostgresRuntimeStore", "NewPostgresStoreForTest", "StartPostgres", "QueryRowContext", "QueryContext", "ExecContext", "BeginTx":
			refusal = fmt.Errorf("timer startup fixture retains %s", selector.Sel.Name)
		}
		counts[selector.Sel.Name]++
		return true
	})
	if refusal != nil {
		return refusal
	}
	if counts["StartPostgresRuntimeStore"] != 1 || counts["StartSQLiteRuntimeStore"] != 1 || counts["open"] != 4 {
		return fmt.Errorf("timer startup callers did not converge on both native owners: %v", counts)
	}
	return nil
}
