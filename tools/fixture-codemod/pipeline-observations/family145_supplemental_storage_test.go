package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
)

func family145SupplementalHistoryProofError(before, after string) error {
	oldAssertions, oldErr := family145NestedBusinessAssertions(before)
	newAssertions, newErr := runSetupAssertions(after)
	if oldErr != nil || newErr != nil || !slices.Equal(oldAssertions, newAssertions) {
		return fmt.Errorf("supplemental history lost a business assertion: %v/%v", oldErr, newErr)
	}
	for _, cut := range []string{
		"storetest.ReadSupplementalFanOutObligationHistory(ctx, selected, runID)",
		"key, raw := row.Key, row.Fact", "if fact.Kind != \"intent\"", "if key != previous",
		"previous != \"\" && versions != 2", "if versions == 1", "wantCursor = cardinality",
		"versions >= 2 || fact.Cursor != wantCursor || fact.Cardinality != cardinality",
		"observed != intents || versions != 2",
	} {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("supplemental history lost exact owner/key/order/cardinality cut: %s", cut)
		}
	}
	return nil
}

func family145SupplementalEscape(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "supplemental.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		member, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if slices.Contains([]string{"QueryRow", "QueryRowContext", "Query", "QueryContext", "AdmitPostgresRuntimeStore", "DatabaseForTest"}, member.Sel.Name) {
			err = fmt.Errorf("raw member %s", member.Sel.Name)
		}
		if owner, ok := member.X.(*ast.Ident); ok && ((owner.Name == "f" && member.Sel.Name == "db") || (owner.Name == "sql" && member.Sel.Name == "DB")) {
			err = fmt.Errorf("raw carrier %s", family145NodeString(member))
		}
		return true
	})
	return err
}
