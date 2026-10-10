package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeClosedEngineRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-closed-engine-owner" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 2 {
		t.Fatal("closed engine cohort must retain state and accumulator witnesses")
	}
	return selected
}

func nativeClosedEngineWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if nativeClosedEngineSetup(cursor) {
			return false
		}
		if !predecessor {
			return true
		}
		call, ok := cursor.Node().(*ast.CallExpr)
		if !ok {
			return true
		}
		switch formattedNativeReadNode(call.Fun) {
		case "newPostgresWorkflowInstanceStoreForTest":
			cursor.Replace(mutationSeedExpression(t, "fixture.Persistence.store"))
			return false
		case "testPipelineRunContextNoSeed":
			cursor.Replace(mutationSeedExpression(t, "fixture.Context"))
			return false
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func nativeClosedEngineSetup(cursor *astutil.Cursor) bool {
	if _, ok := cursor.Node().(ast.Stmt); !ok {
		return false
	}
	text := formattedNativeReadNode(cursor.Node())
	if text == "_, db, _ := testutil.StartPostgres(t)" || text == "fixture := open(t)" || strings.HasPrefix(text, "closedErr := closeWorkflowNativeOwnerForTest(") {
		cursor.Delete()
		return true
	}
	if strings.HasPrefix(text, "if err := db.Close();") || strings.HasPrefix(text, "if err.Error() != closedErr.Error()") {
		cursor.Delete()
		return true
	}
	return false
}

func TestNativeClosedEngineRecipesPreserveCallsAndOriginalErrorAssertions(t *testing.T) {
	for _, row := range nativeClosedEngineRecipes(t) {
		if nativeClosedEngineWorkload(t, row.Before, true) != nativeClosedEngineWorkload(t, row.After, false) {
			t.Fatalf("closed-owner operation/candidate/error assertion changed: %s", row.Function)
		}
		if !strings.Contains(row.After, "closeWorkflowNativeOwnerForTest(") || !strings.Contains(row.After, "err.Error() != closedErr.Error()") {
			t.Fatal("closed-owner cause must be independently observed and compared")
		}
	}
}

func TestNativeClosedEngineOracleRejectsWeakenedCallsAndErrorAssertions(t *testing.T) {
	for _, row := range nativeClosedEngineRecipes(t) {
		changed := strings.Replace(row.After, "if err == nil {", "if false {", 1)
		if nativeClosedEngineWorkload(t, row.Before, true) == nativeClosedEngineWorkload(t, changed, false) {
			t.Fatalf("lost original error assertion admitted: %s", row.Function)
		}
	}
}
