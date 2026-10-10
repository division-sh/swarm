package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeCompositionTargetRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range rows {
		if row.Family == "native-composition-target" {
			found = append(found, row)
		}
	}
	if len(found) != 1 {
		t.Fatal("composition target cohort must retain its complete root")
	}
	return found[0]
}

func TestNativeCompositionTargetRecipePreservesIdentityRestartAndHostileSiblingAssertions(t *testing.T) {
	row := nativeCompositionTargetRecipe(t)
	if nativeCompositionTargetWorkload(t, row.Before, true) != nativeCompositionTargetWorkload(t, row.After, false) {
		t.Fatal("composition target appearance/restart/conflict/sibling proof changed")
	}
}

func nativeCompositionTargetWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		loop := body.List[0].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = call.Args[1].(*ast.FuncLit).Body
	}
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if handlerNativeSetupStatement(t, cursor, predecessor) || nativeEngineReadSetup(t, cursor, "", predecessor) {
			return false
		}
		if predecessor {
			nativeCompositionTargetFault(t, cursor.Node())
			nativeEngineReadBindings(t, cursor.Node())
			if pair, ok := cursor.Node().(*ast.KeyValueExpr); ok && formattedNativeReadNode(pair.Key) == "WorkflowVersion" && formattedNativeReadNode(pair.Value) == `"1"` {
				pair.Value = mutationSeedExpression(t, "source.WorkflowVersion()")
			}
			if call, ok := cursor.Node().(*ast.CallExpr); ok {
				handlerNativeRootCall(t, call, true)
			}
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func nativeCompositionTargetFault(t *testing.T, node ast.Node) {
	t.Helper()
	conditional, ok := node.(*ast.IfStmt)
	if !ok || conditional.Init == nil || len(conditional.Body.List) != 1 {
		return
	}
	if formattedNativeReadNode(conditional.Body.List[0]) != `t.Fatalf("seed exact target conflict: %v", err)` {
		return
	}
	if formattedNativeReadNode(conditional.Init) != "err := store.upsert(ctx, exact)" {
		t.Fatal("original target-conflict input changed")
	}
	conditional.Init = projectionShapeStatement(t, "changed, err := fixture.ConflictingEntityType(ctx, testPipelineRunID, exact.EntityID)")
	conditional.Cond = mutationSeedExpression(t, "err != nil || changed != 1")
}

func TestNativeCompositionTargetOracleRejectsLostExactOwnerAndSiblingAssertions(t *testing.T) {
	row := nativeCompositionTargetRecipe(t)
	for _, original := range []string{"!errors.Is(err, runtimeengine.ErrUnconstructedWorkflowTarget)", "application.State().EntityID != identity.EntityID", "application.Route().InstancePath != identity.InstancePath", "application.EntityID() != identity.EntityID", "field row disagrees with constructed header contract", "sibling.EntityID != siblingEntityID", `sibling.Fields["account_id"] != "account-1"`} {
		if !strings.Contains(row.After, original) {
			t.Fatalf("required exact-target assertion missing: %s", original)
		}
		replacement := "false"
		if original == "field row disagrees with constructed header contract" {
			replacement = "different conflict"
		}
		changed := strings.Replace(row.After, original, replacement, 1)
		if nativeCompositionTargetWorkload(t, row.Before, true) == nativeCompositionTargetWorkload(t, changed, false) {
			t.Fatalf("weakened composition-target assertion accepted: %s", original)
		}
	}
}
