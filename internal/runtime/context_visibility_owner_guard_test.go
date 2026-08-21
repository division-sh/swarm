package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestRuntimeContextLoadedStateHasOneVisibilityPublisher(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "context_manager.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil || function.Name.Name == "publishRuntimeContextVisibilityLocked" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if ok {
				entryType, isEntry := literal.Type.(*ast.Ident)
				if isEntry && entryType.Name == "runtimeContextEntry" {
					for _, element := range literal.Elts {
						field, keyed := element.(*ast.KeyValueExpr)
						if !keyed {
							continue
						}
						name, named := field.Key.(*ast.Ident)
						value, identified := field.Value.(*ast.Ident)
						if named && identified && name.Name == "state" && value.Name == "RuntimeContextStateLoaded" {
							t.Errorf("%s constructs a loaded runtime context outside publishRuntimeContextVisibilityLocked at %s", function.Name.Name, fset.Position(field.Pos()))
						}
					}
				}
			}
			assignment, ok := node.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, left := range assignment.Lhs {
				selector, ok := left.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == "state" {
					t.Errorf("%s mutates runtime context loaded state outside publishRuntimeContextVisibilityLocked at %s", function.Name.Name, fset.Position(selector.Pos()))
				}
			}
			return true
		})
	}
}
