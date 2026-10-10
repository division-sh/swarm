package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeEngineBucketRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-engine-malformed-bucket" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 1 {
		t.Fatal("malformed engine bucket cohort must retain its exact original root")
	}
	return selected[0]
}

func nativeEngineBucketWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if nativeEngineReadSetup(t, cursor, "", predecessor) {
			return false
		}
		if _, ok := cursor.Node().(ast.Stmt); ok {
			text := formattedNativeReadNode(cursor.Node())
			if text == "fixture := open(t, testPipelineRunID)" || text == "ctx := fixture.Context" {
				cursor.Delete()
				return false
			}
		}
		if predecessor {
			if call, ok := cursor.Node().(*ast.CallExpr); ok && formattedNativeReadNode(call.Fun) == "testWorkflowStoreRunContext" {
				cursor.Replace(ast.NewIdent("ctx"))
				return false
			}
			if call, ok := cursor.Node().(*ast.CallExpr); ok && formattedNativeReadNode(call.Fun) == "store.upsert" {
				call.Fun = mutationSeedExpression(t, "fixture.Construct")
			}
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func TestNativeEngineBucketRecipePreservesMalformedCarrierAndExactRefusal(t *testing.T) {
	row := nativeEngineBucketRecipe(t)
	if nativeEngineBucketWorkload(t, row.Before, true) != nativeEngineBucketWorkload(t, row.After, false) {
		t.Fatal("engine bucket carrier, decoder or exact refusal changed")
	}
}

func TestNativeEngineBucketOracleRejectsChangedPayloadAndRefusal(t *testing.T) {
	row := nativeEngineBucketRecipe(t)
	for _, change := range []struct{ original, replacement string }{
		{`"evidence": "bad"`, `"evidence": map[string]any{}`},
		{`testEngineStateAddress("root", "root", "22222222-2222-2222-2222-222222222222")`, `testEngineStateAddress("root", "other", "22222222-2222-2222-2222-222222222222")`},
		{`err == nil || !strings.Contains(err.Error(), "invalid workflow state bucket")`, "false"},
	} {
		if !strings.Contains(row.After, change.original) {
			t.Fatalf("bucket proof obligation missing: %s", change.original)
		}
		changed := strings.Replace(row.After, change.original, change.replacement, 1)
		if nativeEngineBucketWorkload(t, row.Before, true) == nativeEngineBucketWorkload(t, changed, false) {
			t.Fatalf("weakened bucket proof admitted: %s", change.original)
		}
	}
}
