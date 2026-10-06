package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"testing"
)

func TestInboundRecipesChangeOnlyTheUnusedSeedPool(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "inbound-unused-pool" {
			continue
		}
		count++
		t.Run(row.File+"/"+row.Function, func(t *testing.T) {
			assertInboundRecipeChangesOnlyUnusedPool(t, row)
		})
	}
	if count != 19 || len(rows)-count != 28 {
		t.Fatalf("family count=%d, prior recipes=%d", count, len(rows)-count)
	}
}

func assertInboundRecipeChangesOnlyUnusedPool(t *testing.T, row recipe) {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, row.File, "package runtime_test\n"+row.Before, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, row.Function)
	if err != nil {
		t.Fatal(err)
	}
	switch row.Function {
	case "seedPostgresInboundGatewayRuntime":
		assertAndRemoveUnusedSeedParameter(t, fn)
	case "seedProviderRawSettlementRuntime":
		removeOnlyInboundSeedCallArgument(t, fn)
		fn.Type.Params.List = append(fn.Type.Params.List[:3], fn.Type.Params.List[4:]...)
	case "openProviderRawSettlementStore":
		removeUnusedProviderPoolReturn(t, fn)
	case "TestInboundGatewayProviderRawSettlementSQLitePostgres":
		removeUnusedProviderPoolPropagation(t, fn)
	default:
		removeOnlyInboundSeedCallArgument(t, fn)
	}
	var expected bytes.Buffer
	if err := format.Node(&expected, token.NewFileSet(), fn); err != nil {
		t.Fatal(err)
	}
	actual, err := canonicalFunction(row.After)
	if err != nil || actual != expected.String() {
		t.Fatalf("recipe changed other fixture/workload/assertion statements: %v", err)
	}
}

func removeUnusedProviderPoolReturn(t *testing.T, fn *ast.FuncDecl) {
	t.Helper()
	if fn.Type.Results.NumFields() != 2 {
		t.Fatal("provider fixture's original result changed")
	}
	fn.Type.Results.List = fn.Type.Results.List[:1]
	fn.Type.Results.Opening, fn.Type.Results.Closing = token.NoPos, token.NoPos
	count := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if result, ok := node.(*ast.ReturnStmt); ok {
			if len(result.Results) != 2 {
				t.Fatal("provider fixture's original returns changed")
			}
			result.Results = result.Results[:1]
			count++
		}
		return true
	})
	if count != 2 {
		t.Fatalf("provider fixture has %d returns, want two", count)
	}
}

func removeUnusedProviderPoolPropagation(t *testing.T, fn *ast.FuncDecl) {
	t.Helper()
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "seedProviderRawSettlementRuntime" {
				if len(call.Args) != 10 {
					t.Fatal("provider seed arity changed")
				}
				call.Args = append(call.Args[:3], call.Args[4:]...)
			}
		}
		if assignment, ok := node.(*ast.AssignStmt); ok && len(assignment.Rhs) == 1 {
			if call, ok := assignment.Rhs[0].(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "openProviderRawSettlementStore" {
					if len(assignment.Lhs) != 2 {
						t.Fatal("provider fixture binding changed")
					}
					assignment.Lhs = assignment.Lhs[:1]
				}
			}
		}
		return true
	})
}

func assertAndRemoveUnusedSeedParameter(t *testing.T, fn *ast.FuncDecl) {
	t.Helper()
	if fn.Type.Params.NumFields() != 11 {
		t.Fatal("seed's original pool arity changed")
	}
	parameter := fn.Type.Params.List[2]
	if len(parameter.Names) != 1 || parameter.Names[0].Name != "db" {
		t.Fatal("seed's original pool argument changed")
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if identifier, ok := node.(*ast.Ident); ok && identifier.Name == "db" {
			t.Fatal("seed pool is live; its removal is not authorized")
		}
		return true
	})
	fn.Type.Params.List = append(fn.Type.Params.List[:2], fn.Type.Params.List[3:]...)
}

func removeOnlyInboundSeedCallArgument(t *testing.T, fn *ast.FuncDecl) {
	t.Helper()
	count := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		callee, ok := call.Fun.(*ast.Ident)
		if !ok || callee.Name != "seedPostgresInboundGatewayRuntime" {
			return true
		}
		if len(call.Args) != 11 {
			t.Fatal("seed call's original arity changed")
		}
		argument, ok := call.Args[2].(*ast.Ident)
		if !ok || argument.Name != "db" {
			t.Fatal("cannot discard an effectful or different pool expression")
		}
		call.Args = append(call.Args[:2], call.Args[3:]...)
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("exactly one seed call expected, got %d", count)
	}
}
