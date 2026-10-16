package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

func removePublicRollbackDeclaration(raw []byte, name, expected string) ([]byte, error) {
	current, start, end, err := deliveryStatusSourceDeclaration(raw, name)
	if err != nil {
		return nil, err
	}
	if canonicalDeliveryStatusDeclaration(current) != canonicalDeliveryStatusDeclaration(expected) || end >= len(raw) || raw[end] != '\n' {
		return nil, fmt.Errorf("unreviewed rollback declaration addition: %s", name)
	}
	// The addition occupied one declaration plus its trailing newline; deleting
	// that exact span preserves the historical spacing of adjacent declarations.
	return append(append([]byte{}, raw[:start]...), raw[end+1:]...), nil
}

func canonicalDeliveryStatusDeclaration(raw string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", "package proof\n"+raw, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "invalid declaration: " + raw
	}
	return requestkey.Node(file.Decls[0])
}

func deliveryStatusSourceDeclaration(raw []byte, name string) (string, int, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.AllErrors)
	if err != nil {
		return "", 0, 0, err
	}
	var matched ast.Decl
	for _, declaration := range file.Decls {
		if !deliveryStatusDeclarationNamed(declaration, name) {
			continue
		}
		if matched != nil {
			return "", 0, 0, fmt.Errorf("ambiguous delivery status amendment: %s", name)
		}
		matched = declaration
	}
	if matched == nil {
		return "", 0, 0, fmt.Errorf("missing delivery status amendment: %s", name)
	}
	start, end := fset.Position(matched.Pos()).Offset, fset.Position(matched.End()).Offset
	return string(raw[start:end]), start, end, nil
}

func deliveryStatusDeclarationNamed(declaration ast.Decl, name string) bool {
	if fn, ok := declaration.(*ast.FuncDecl); ok {
		return fn.Recv == nil && fn.Name.Name == name
	}
	if group, ok := declaration.(*ast.GenDecl); ok && group.Tok == token.TYPE && len(group.Specs) == 1 {
		typeSpec, ok := group.Specs[0].(*ast.TypeSpec)
		return ok && typeSpec.Name.Name == name
	}
	return false
}
