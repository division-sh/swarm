package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeEngineDriftRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range rows {
		if row.Family == "native-engine-contract-drift" {
			found = append(found, row)
		}
	}
	if len(found) != 1 {
		t.Fatal("contract drift cohort must preserve its complete root")
	}
	return found[0]
}

func TestNativeEngineDriftRecipePreservesMalformedTypeAndCompleteState(t *testing.T) {
	row := nativeEngineDriftRecipe(t)
	if nativeEngineDriftWorkload(t, row.Before, true) != nativeEngineDriftWorkload(t, row.After, false) {
		t.Fatal("contract drift source/type/candidate/refusal/state changed")
	}
}

func nativeEngineDriftWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		loop := body.List[0].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = call.Args[1].(*ast.FuncLit).Body
	} else {
		last := body.List[len(body.List)-1].(*ast.IfStmt)
		expected := projectionShapeStatement(t, `if counts["entity_state"] != 1 || counts["flow_instances"] != 1 || counts["entity_mutations"] < 1 {t.Fatalf("native rejected-state readback lost physical evidence: %#v",counts)}`)
		if formattedNativeReadNode(last) != formattedNativeReadNode(expected) {
			t.Fatal("positive physical-count bridge proof changed")
		}
		body.List = body.List[:len(body.List)-1]
	}
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if nativeEngineRefusalSetup(cursor) || handlerNativeSetupStatement(t, cursor, predecessor) || nativeEngineReadSetup(t, cursor, "", predecessor) {
			return false
		}
		if predecessor {
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

func TestNativeEngineDriftOracleRejectsLostRefusalAndUnchangedStateAssertions(t *testing.T) {
	row := nativeEngineDriftRecipe(t)
	for _, original := range []string{`EntityType: "wrong_entity"`, `stored.EntityType != "wrong_entity"`, `stored.CurrentState != "active"`, "stored.Revision != 1", `stored.Fields["marker"] != "unchanged"`} {
		if !strings.Contains(row.After, original) {
			t.Fatalf("required original drift fact missing: %s", original)
		}
		replacement := "false"
		if strings.HasPrefix(original, "EntityType:") {
			replacement = `EntityType: "test_entity"`
		}
		changed := strings.Replace(row.After, original, replacement, 1)
		if nativeEngineDriftWorkload(t, row.Before, true) == nativeEngineDriftWorkload(t, changed, false) {
			t.Fatalf("weakened contract drift proof accepted: %s", original)
		}
	}
}
