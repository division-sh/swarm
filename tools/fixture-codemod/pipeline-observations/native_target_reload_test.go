package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeTargetReloadRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range rows {
		if row.Family == "native-delivery-target-reload" {
			found = append(found, row)
		}
	}
	if len(found) != 1 {
		t.Fatal("target reload cohort must preserve its complete root")
	}
	return found[0]
}

func TestNativeTargetReloadRecipePreservesCurrentStateAndDetachedSnapshotAssertions(t *testing.T) {
	row := nativeTargetReloadRecipe(t)
	if nativeTargetReloadWorkload(t, row.Before, true) != nativeTargetReloadWorkload(t, row.After, false) {
		t.Fatal("target application/update/reload workload changed")
	}
}

func nativeTargetReloadWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		loop := body.List[0].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = call.Args[1].(*ast.FuncLit).Body
	}
	upserts := 0
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if handlerNativeSetupStatement(t, cursor, predecessor) || nativeEngineReadSetup(t, cursor, "", predecessor) {
			return false
		}
		if predecessor {
			nativeTargetReloadBinding(t, cursor.Node(), &upserts)
		}
		return true
	}, nil)
	if predecessor && upserts != 2 {
		t.Fatal("original target reload must retain construction and subsequent update")
	}
	return formattedNativeReadNode(body)
}

func nativeTargetReloadBinding(t *testing.T, node ast.Node, upserts *int) {
	t.Helper()
	if pair, ok := node.(*ast.KeyValueExpr); ok && formattedNativeReadNode(pair.Key) == "WorkflowVersion" && formattedNativeReadNode(pair.Value) == `"1"` {
		pair.Value = mutationSeedExpression(t, "source.WorkflowVersion()")
	}
	call, ok := node.(*ast.CallExpr)
	if !ok {
		return
	}
	if formattedNativeReadNode(call.Fun) == "store.upsert" {
		*upserts = *upserts + 1
		if *upserts == 1 {
			call.Fun = mutationSeedExpression(t, "fixture.Construct")
		} else {
			if len(call.Args) != 2 || formattedNativeReadNode(call.Args[0]) != "ctx" || formattedNativeReadNode(call.Args[1]) != "persisted" {
				t.Fatal("original target update input changed")
			}
			*call = *mutationSeedExpression(t, `fixture.Persistence.store.mutateE(ctx,testRunScopedWorkflowInstanceFromContext(ctx,instancePath),func(current *WorkflowInstance)error{current.Fields["marker"]=persisted.Fields["marker"];return nil})`).(*ast.CallExpr)
		}
	}
	handlerNativeRootCall(t, call, true)
}

func TestNativeTargetReloadOracleRejectsLostGateFieldAndDetachedReadAssertions(t *testing.T) {
	row := nativeTargetReloadRecipe(t)
	for _, original := range []string{`!snapshot.StateCarrier.Gates["approved"]`, `!snapshot.StateCarrier.Gates["./approved"]`, `snapshot.StateCarrier.Fields["marker"] != "current"`, `snapshot.StateCarrier.Gates["approved"] = false`, `snapshot.StateCarrier.Fields["marker"] = "mutated"`, `!fresh.StateCarrier.Gates["approved"]`, `fresh.StateCarrier.Fields["marker"] != "current"`, `stored.Gates["approved"]`, `!stored.Gates["./approved"]`, `stored.Fields["marker"] != "current"`} {
		if !strings.Contains(row.After, original) {
			t.Fatalf("required reload assertion missing: %s", original)
		}
		replacement := "false"
		if strings.Contains(original, " = ") {
			replacement = "unrelated = false"
		}
		changed := strings.Replace(row.After, original, replacement, 1)
		if nativeTargetReloadWorkload(t, row.Before, true) == nativeTargetReloadWorkload(t, changed, false) {
			t.Fatalf("weakened reload assertion accepted: %s", original)
		}
	}
}
