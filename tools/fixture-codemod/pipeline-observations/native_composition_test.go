package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeCompositionRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range rows {
		if row.Family == "native-composition-business" {
			found = append(found, row)
		}
	}
	if len(found) != 1 {
		t.Fatal("composition cohort must have its complete repeated-write root")
	}
	return found[0]
}

func TestNativeCompositionRecipePreservesAllConstructionAndBusinessAssertions(t *testing.T) {
	row := nativeCompositionRecipe(t)
	if nativeCompositionWorkload(t, row.Before, true) != nativeCompositionWorkload(t, row.After, false) {
		t.Fatal("composition source/construction/write/assertion changed")
	}
}

func nativeCompositionWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		loop := body.List[0].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = call.Args[1].(*ast.FuncLit).Body
	}
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if handlerNativeSetupStatement(t, cursor, predecessor) || nativeEngineReadSetup(t, cursor, "", predecessor) || nativeQuerySetup(t, cursor, false) {
			return false
		}
		if stmt, ok := cursor.Node().(*ast.ExprStmt); ok && formattedNativeReadNode(stmt) == "seedExactOnceEvent(t, store, ctx, event)" {
			cursor.Delete()
			return false
		}
		if predecessor {
			nativeCompositionCoordinator(t, cursor.Node())
			if call, ok := cursor.Node().(*ast.CallExpr); ok {
				nativeCompositionCalls(t, call)
			}
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func nativeCompositionCoordinator(t *testing.T, node ast.Node) {
	t.Helper()
	ret, ok := node.(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return
	}
	literal, ok := ret.Results[0].(*ast.UnaryExpr)
	if !ok {
		return
	}
	options := literal.X.(*ast.CompositeLit)
	if formattedNativeReadNode(options.Type) != "PipelineCoordinator" {
		return
	}
	var fields []ast.Expr
	for _, field := range options.Elts {
		entry := field.(*ast.KeyValueExpr)
		if formattedNativeReadNode(entry.Key) == "module" {
			entry.Key = ast.NewIdent("Module")
			fields = append(fields, entry)
		}
	}
	if len(fields) != 1 {
		t.Fatal("composition coordinator lost its exact source module")
	}
	options.Type, options.Elts = ast.NewIdent("PipelineCoordinatorOptions"), fields
	ret.Results[0] = &ast.CallExpr{Fun: mutationSeedExpression(t, "fixture.NewCoordinator"), Args: []ast.Expr{options}}
}

func nativeCompositionCalls(t *testing.T, call *ast.CallExpr) {
	t.Helper()
	handlerNativeRootCall(t, call, true)
	switch formattedNativeReadNode(call.Fun) {
	case "store.create":
		call.Fun = mutationSeedExpression(t, "fixture.Construct")
	case "store.list":
		call.Fun = mutationSeedExpression(t, "pc.ListWorkflowInstances")
	case "handlerTestRootIngress":
		if len(call.Args) != 10 || formattedNativeReadNode(call.Args[7]) != `""` {
			t.Fatal("composition event identity changed")
		}
		call.Fun = mutationSeedExpression(t, "eventtest.ExistingRunRootIngressWithRoutingSource")
		call.Args = append(append([]ast.Expr{}, call.Args[:7]...), call.Args[8], mutationSeedExpression(t, "eventtest.RootRoutingSource(testPipelineRunID)"), call.Args[9])
	case "pc.executeNodeContractHandler":
		if len(call.Args) != 5 || formattedNativeReadNode(call.Args[3]) != "workflowTriggerContext{Event: event}" || formattedNativeReadNode(call.Args[4]) != "false" {
			t.Fatal("composition execution context changed")
		}
		call.Fun = ast.NewIdent("executeNativeWorkflowScenarioHandlerForTest")
		call.Args = []ast.Expr{ast.NewIdent("t"), ast.NewIdent("fixture"), ast.NewIdent("pc"), call.Args[0], call.Args[1], call.Args[2], ast.NewIdent("event")}
	}
}

func TestNativeCompositionOracleRejectsLostBusinessAndRepeatedInitializationAssertions(t *testing.T) {
	row := nativeCompositionRecipe(t)
	for _, original := range []string{"[]int{42, 99}", `"unrelated-%d"`, `current.EntityID != testPipelineRunID`, `current.Fields["status"] != "pending"`, `current.CurrentState != "active"`, "len(instances) != 1"} {
		if !strings.Contains(row.After, original) {
			t.Fatalf("required original business assertion missing: %s", original)
		}
		replacement := "false"
		if original == "[]int{42, 99}" {
			replacement = "[]int{42}"
		} else if original == `"unrelated-%d"` {
			replacement = `"other-%d"`
		}
		changed := strings.Replace(row.After, original, replacement, 1)
		if nativeCompositionWorkload(t, row.Before, true) == nativeCompositionWorkload(t, changed, false) {
			t.Fatalf("weakened composition workload accepted: %s", original)
		}
	}
}
