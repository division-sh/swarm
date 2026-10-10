package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeQueryRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-execution-flow-query" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 2 {
		t.Fatal("query cohort must include both exact-flow roots")
	}
	return selected
}

func TestNativeQueryRecipesPreserveEverySourcePredicateAndAssertion(t *testing.T) {
	for _, row := range nativeQueryRecipes(t) {
		if nativeQueryWorkload(t, row, row.Before, true) != nativeQueryWorkload(t, row, row.After, false) {
			t.Fatalf("query source/predicate/case/assertion changed: %s", row.Function)
		}
	}
}

func nativeQueryWorkload(t *testing.T, row recipe, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor && row.Function == "TestPipelineExpressionPreservesExactExecutionFlowOnBothStores" {
		last := len(body.List) - 1
		loop := body.List[last].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body.List = append(body.List[:last], call.Args[1].(*ast.FuncLit).Body.List...)
	}
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if nativeQuerySetup(t, cursor, predecessor) || nativeEngineReadSetup(t, cursor, row.Function, predecessor) {
			return false
		}
		if predecessor {
			nativeQueryBindings(t, cursor)
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func nativeQuerySetup(t *testing.T, cursor *astutil.Cursor, predecessor bool) bool {
	if _, ok := cursor.Node().(ast.Stmt); !ok {
		return false
	}
	source := formattedNativeReadNode(cursor.Node())
	if source == "db, store := openHandlerEntityRequirementStore(t, backend)" || source == "ctx := correlation.WithRunID(fixture.Context, testPipelineRunID)" {
		cursor.Delete()
		return true
	}
	conditional, ok := cursor.Node().(*ast.IfStmt)
	if predecessor && ok && formattedNativeReadNode(conditional.Cond) == `backend == "sqlite"` {
		expected := projectionShapeStatement(t, `if backend=="sqlite"{sqliteExactOnceRunContext(t,db)}else{testPipelineRunContext(t,db)}`)
		if source != formattedNativeReadNode(expected) {
			t.Fatal("original query run setup changed")
		}
		cursor.Delete()
		return true
	}
	return false
}

func nativeQueryBindings(t *testing.T, cursor *astutil.Cursor) {
	t.Helper()
	nativeEngineReadBindings(t, cursor.Node())
	call, ok := cursor.Node().(*ast.CallExpr)
	if !ok {
		return
	}
	switch formattedNativeReadNode(call.Fun) {
	case "testPipelineCoordinatorRunContext":
		cursor.Replace(ast.NewIdent("ctx"))
	case "pc.workflowStore.upsert":
		call.Fun = mutationSeedExpression(t, "fixture.Construct")
	case "newPostgresPipelineCoordinatorForTest":
		call.Fun = mutationSeedExpression(t, "fixture.NewCoordinator")
		call.Args = call.Args[2:]
	default:
		handlerNativeRootCall(t, call, true)
	}
}

func TestNativeQueryOracleRejectsWeakenedCountFlowAndSource(t *testing.T) {
	for _, row := range nativeQueryRecipes(t) {
		for _, change := range [][2]string{{"count == 1", "count >= 0"}, {"count == 0", "count >= 0"}, {`FlowID:  "child"`, `FlowID:  "."`}, {"display-name-is-not-a-flow", "different-display"}, {"if !ok {", "if false {"}, {"err != nil || !ok", "false"}} {
			if !strings.Contains(row.After, change[0]) {
				continue
			}
			changed := strings.Replace(row.After, change[0], change[1], 1)
			if nativeQueryWorkload(t, row, row.Before, true) == nativeQueryWorkload(t, row, changed, false) {
				t.Fatalf("weakened query workload accepted: %s", change[0])
			}
		}
	}
}
