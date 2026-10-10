package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func lookupMissRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var out []recipe
	for _, row := range rows {
		if row.Family == "native-lookup-miss" {
			out = append(out, row)
		}
	}
	if len(out) != 2 {
		t.Fatal("lookup cohort must include root and exact count helper")
	}
	return out
}
func TestNativeLookupMissRecipePreservesRequestedKeysCallbackAndCardinality(t *testing.T) {
	for _, row := range lookupMissRecipes(t) {
		if row.Function == "workflowInstanceRowCount" {
			continue
		}
		if lookupMissWorkload(t, row.Before, true) != lookupMissWorkload(t, row.After, false) {
			t.Fatal("lookup miss workload or assertions changed")
		}
	}
}
func lookupMissWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	body := fn.Body
	if predecessor {
		loop := body.List[1].(*ast.RangeStmt)
		call := loop.Body.List[1].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = call.Args[1].(*ast.FuncLit).Body
		body.List = body.List[1:]
	} else {
		body.List = body.List[2:]
	}
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch formattedNativeReadNode(call.Fun) {
		case "store.mutateE":
			call.Fun = mutationSeedExpression(t, "fixture.Persistence.store.mutateE")
		case "workflowInstanceRowCount":
			if predecessor {
				call.Args[2] = ast.NewIdent("fixture")
			}
		}
		return true
	})
	return formattedNativeReadNode(body)
}
func TestNativeLookupHeaderCountKeepsOriginalWholePhysicalQuery(t *testing.T) {
	var helper recipe
	for _, row := range lookupMissRecipes(t) {
		if row.Function == "workflowInstanceRowCount" {
			helper = row
		}
	}
	original := projectionShapeFunction(t, helper.Before)
	guard := original.Body.List[2].(*ast.IfStmt)
	scan := guard.Init.(*ast.AssignStmt).Rhs[0].(*ast.CallExpr)
	query := scan.Fun.(*ast.SelectorExpr).X.(*ast.CallExpr)
	if formattedNativeReadNode(query.Fun) != "db.QueryRowContext" || len(query.Args) != 2 || formattedNativeReadNode(query.Args[0]) != "ctx" || len(scan.Args) != 1 || formattedNativeReadNode(scan.Args[0]) != "&count" {
		t.Fatal("original count context or scan changed")
	}
	sql, err := strconv.Unquote(query.Args[1].(*ast.BasicLit).Value)
	if err != nil {
		t.Fatal(err)
	}
	if sql != "SELECT COUNT(*) FROM flow_instances" {
		t.Fatal("original physical cardinality changed")
	}
	source, err := os.ReadFile(filepath.Join("..", "..", "..", "internal/store/internal/backend/pipelinepersistence/workflow_projection_observation.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "count.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "CountWorkflowInstanceHeadersForTest" {
			continue
		}
		found = true
		statement := fn.Body.List[1].(*ast.IfStmt)
		call := statement.Init.(*ast.AssignStmt).Rhs[0].(*ast.CallExpr).Fun.(*ast.SelectorExpr).X.(*ast.CallExpr)
		value, err := strconv.Unquote(call.Args[1].(*ast.BasicLit).Value)
		if err != nil {
			t.Fatal(err)
		}
		if value != sql || formattedNativeReadNode(call.Fun) != "tx.QueryRowContext" || len(call.Args) != 2 || formattedNativeReadNode(call.Args[0]) != "ctx" {
			t.Fatal("bounded native count added filtering or changed original scope")
		}
	}
	if !found {
		t.Fatal("missing count owner")
	}
	after := projectionShapeFunction(t, helper.After)
	expected := projectionShapeFunction(t, `func count(){t.Helper();count,err:=fixture.CountHeaders(ctx);if err!=nil{t.Fatalf("count workflow instances: %v",err)};return count}`)
	if formattedNativeReadNode(after.Body) != formattedNativeReadNode(expected.Body) {
		t.Fatal("count consumer lost error or value assertion")
	}
}
func TestNativeLookupMissOracleRejectsDroppedCallbackAndTypedScopeAssertions(t *testing.T) {
	for _, row := range lookupMissRecipes(t) {
		if row.Function == "workflowInstanceRowCount" {
			continue
		}
		for _, condition := range []string{"!errors.As(err, &miss)", "miss.RequestedKey != wantKey", "callbackRan", "after != before"} {
			if !strings.Contains(row.After, condition) {
				t.Fatal("missing original lookup assertion")
			}
			changed := strings.Replace(row.After, "if "+condition, "if false", 1)
			if changed == row.After {
				changed = strings.Replace(row.After, condition, "false", 1)
			}
			if lookupMissWorkload(t, row.Before, true) == lookupMissWorkload(t, changed, false) {
				t.Fatalf("weakened lookup assertion accepted: %s", condition)
			}
		}
	}
}
