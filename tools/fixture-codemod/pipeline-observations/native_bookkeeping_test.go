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

func bookkeepingRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Family == "native-bookkeeping-carrier" {
			return row
		}
	}
	t.Fatal("missing bookkeeping cohort")
	return recipe{}
}
func TestNativeBookkeepingRecipePreservesCarrierWorkloadAndHeaderOnlyFault(t *testing.T) {
	row := bookkeepingRecipe(t)
	if bookkeepingWorkload(t, row.Before, true) != bookkeepingWorkload(t, row.After, false) {
		t.Fatal("bookkeeping carrier workload or assertion changed")
	}
	original := projectionShapeFunction(t, row.Before)
	var queries []string
	ast.Inspect(original, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || formattedNativeReadNode(assign.Lhs[0]) != "update" {
			return true
		}
		value, err := strconv.Unquote(assign.Rhs[0].(*ast.BasicLit).Value)
		if err != nil {
			t.Fatal(err)
		}
		value = strings.ReplaceAll(value, "::jsonb", "")
		value = strings.ReplaceAll(value, "$2::uuid", "$2")
		if strings.Contains(value, "?") {
			value = strings.Replace(value, "?", "$1", 1)
			value = strings.Replace(value, "?", "$2", 1)
		}
		value = strings.ReplaceAll(value, "WHERE instance_path = $1 AND run_id = $2", "WHERE run_id = $2 AND instance_path = $1")
		queries = append(queries, value)
		return true
	})
	if len(queries) != 2 || queries[0] != queries[1] {
		t.Fatal("original bookkeeping payload/scopes disagree")
	}
	source, err := os.ReadFile(filepath.Join("..", "..", "..", "internal/store/internal/backend/pipelinepersistence/workflow_projection_shape_fault.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "bookkeeping.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "SetWorkflowProjectionPlatformBookkeepingForTest" {
			continue
		}
		found = true
		call := fn.Body.List[0].(*ast.ReturnStmt).Results[0].(*ast.CallExpr).Args[0].(*ast.CallExpr)
		value, err := strconv.Unquote(call.Args[1].(*ast.BasicLit).Value)
		if err != nil {
			t.Fatal(err)
		}
		if value != queries[0] || formattedNativeReadNode(call.Fun) != "tx.ExecContext" || len(call.Args) != 4 || formattedNativeReadNode(call.Args[0]) != "ctx" || formattedNativeReadNode(call.Args[2]) != "path" || formattedNativeReadNode(call.Args[3]) != "run" {
			t.Fatal("bounded fault changed header-only columns, payload or exact run/path")
		}
	}
	if !found {
		t.Fatal("missing native bookkeeping fault owner")
	}
}
func bookkeepingWorkload(t *testing.T, source string, predecessor bool) string {
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
		case "store.create":
			call.Fun = mutationSeedExpression(t, "fixture.Construct")
		case "store.Load":
			call.Fun = mutationSeedExpression(t, "fixture.Persistence.LoadWorkflowInstance")
		case "store.mutateE":
			call.Fun = mutationSeedExpression(t, "fixture.Persistence.store.mutateE")
		}
		return true
	})
	if predecessor {
		start := -1
		for i, statement := range body.List {
			if strings.HasPrefix(formattedNativeReadNode(statement), "update := ") {
				start = i
				break
			}
		}
		if start < 0 || start+5 > len(body.List) {
			t.Fatal("original header-only fault disappeared")
		}
		exec := body.List[start+2].(*ast.AssignStmt).Rhs[0].(*ast.CallExpr)
		if formattedNativeReadNode(exec) != `store.testDB().ExecContext(ctx, update, route.Route.InstancePath, runtimecorrelation.RunIDFromContext(ctx))` {
			t.Fatal("original bookkeeping context or coordinates changed")
		}
		count := body.List[start+4].(*ast.IfStmt)
		if formattedNativeReadNode(count.Cond) != "err != nil || changed != 1" {
			t.Fatal("original one-row requirement changed")
		}
		raw := `func replacement(){changed,err:=fixture.SetPlatformBookkeeping(ctx,runtimecorrelation.RunIDFromContext(ctx),route.Route.InstancePath);if err!=nil{t.Fatalf("seed existing platform bookkeeping: %v",err)};if changed!=1{t.Fatalf("seed exact constructed header: rows=%d err=%v",changed,err)}}`
		replacement := projectionShapeFunction(t, raw).Body.List
		body.List = append(append(body.List[:start:start], replacement...), body.List[start+5:]...)
	}
	return formattedNativeReadNode(body)
}
func TestNativeBookkeepingOracleRejectsLostPlatformFactFieldsGatesAndRowCount(t *testing.T) {
	row := bookkeepingRecipe(t)
	for _, condition := range []string{`changed != 1`, `created.Bookkeeping["platform_fact"] != "preserve"`, `loaded.Fields["status"] != "after"`, `loaded.Bookkeeping["platform_fact"] != "preserve"`, `!loaded.Gates["ready"]`} {
		if !strings.Contains(row.After, condition) {
			t.Fatal("missing original bookkeeping assertion")
		}
		changed := strings.Replace(row.After, condition, "false", 1)
		if bookkeepingWorkload(t, row.Before, true) == bookkeepingWorkload(t, changed, false) {
			t.Fatalf("weakened bookkeeping assertion accepted: %s", condition)
		}
	}
}
