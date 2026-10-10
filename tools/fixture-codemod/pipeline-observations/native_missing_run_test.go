package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"
)

func missingRunRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-missing-run" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 1 {
		t.Fatal("missing-run cohort is not exact")
	}
	return selected[0]
}

func missingRunWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		body.List = body.List[2:]
	} else {
		body.List = body.List[3:]
	}
	if predecessor {
		ast.Inspect(body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if ok && formattedNativeReadNode(call.Fun) == "store.upsert" {
				call.Fun = mutationSeedExpression(t, "fixture.Construct")
				call.Args[0] = ast.NewIdent("ctx")
			}
			return true
		})
		condition := body.List[1].(*ast.IfStmt)
		original := mutationSeedExpression(t, `err == nil || !strings.Contains(err.Error(), "run_id is required")`)
		if formattedNativeReadNode(condition.Cond) != formattedNativeReadNode(original) {
			t.Fatal("original missing-run assertion changed")
		}
		condition.Cond = mutationSeedExpression(t, `err == nil || err.Error() != "flow instance activation readiness: dynamic flow runtime readiness requires run_id"`)
	}
	return formattedNativeReadNode(body)
}

func TestNativeMissingRunRecipePreservesRefusalAndSuppliedInstance(t *testing.T) {
	row := missingRunRecipe(t)
	if missingRunWorkload(t, row.Before, true) != missingRunWorkload(t, row.After, false) {
		t.Fatal("missing-run refusal or instance changed")
	}
	body := projectionShapeFunction(t, row.After).Body
	expected := projectionShapeStatement(t, `ctx:=fixture.Context`)
	if formattedNativeReadNode(body.List[1]) != formattedNativeReadNode(expected) {
		t.Fatal("negative control gained a run")
	}
	guard := projectionShapeStatement(t, `if runtimecorrelation.RunIDFromContext(ctx)!="" {t.Fatal("missing-run negative control inherited a run")}`)
	if formattedNativeReadNode(body.List[2]) != formattedNativeReadNode(guard) {
		t.Fatal("missing run is not explicitly verified")
	}
	for _, condition := range []string{"err == nil", `err.Error() != "flow instance activation readiness: dynamic flow runtime readiness requires run_id"`} {
		changed := strings.Replace(row.After, condition, "false", 1)
		if changed == row.After || missingRunWorkload(t, row.Before, true) == missingRunWorkload(t, changed, false) {
			t.Fatalf("weakened missing-run assertion accepted: %s", condition)
		}
	}
}
