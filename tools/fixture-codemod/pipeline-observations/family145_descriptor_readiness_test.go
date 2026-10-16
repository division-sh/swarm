package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
)

func family145DescriptorBusinessAssertions(source string) ([]string, error) {
	// The exact caller propagation removes only the DB/dialect carrier. The
	// physical five-column snapshot and its complete-row comparison remain.
	source = strings.ReplaceAll(source, "flowReadinessStampSnapshot(t, ctx, db, sqlite, runID, path)", "flowReadinessStampSnapshot(t, ctx, selected, runID, path)")
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var result []string
	ast.Inspect(file, func(node ast.Node) bool {
		conditional, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		condition := family145NodeString(conditional.Cond)
		// Exact simple error gates are delegated to the canonical selected owner.
		// Expected refusals (err == nil), cardinalities, clocks and identities stay.
		if condition == "err != nil" || condition == "marshalErr != nil" {
			return false
		}
		for _, stmt := range conditional.Body.List {
			expression, ok := stmt.(*ast.ExprStmt)
			if !ok {
				continue
			}
			call, ok := expression.X.(*ast.CallExpr)
			if !ok {
				continue
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				continue
			}
			owner, ok := selector.X.(*ast.Ident)
			if ok && owner.Name == "t" && slices.Contains([]string{"Fatal", "Fatalf", "Error", "Errorf", "Fail", "FailNow"}, selector.Sel.Name) {
				result = append(result, condition+" => "+family145NodeString(call))
			}
		}
		return true
	})
	return result, nil
}

func family145DescriptorReadinessEscape(data []byte) error {
	if err := family145SupplementalEscape(data); err != nil {
		return err
	}
	file, err := parser.ParseFile(token.NewFileSet(), "descriptor.go", data, parser.AllErrors)
	if err != nil {
		return err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		member, ok := node.(*ast.SelectorExpr)
		if ok && slices.Contains([]string{"Exec", "ExecContext", "Begin", "BeginTx", "Database"}, member.Sel.Name) {
			err = fmt.Errorf("raw member %s", member.Sel.Name)
		}
		return true
	})
	return err
}
