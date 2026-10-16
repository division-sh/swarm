package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
)

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
