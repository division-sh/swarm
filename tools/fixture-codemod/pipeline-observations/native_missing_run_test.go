package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func missingRunCurrentPredecessor(row recipe, source []byte) (recipe, error) {
	if row.Family != "native-missing-run" || row.File != "internal/runtime/pipeline/workflow_instance_store_run_scope_test.go" || row.Function != "TestWorkflowInstanceStore_RequiresRunContext" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != "3e93b7a5a6edeb70f529ed96119c2b0a4936666be6f65173bb21907d88ba527b" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "6134c248411bd26886d4df49cdc1a09d6103488e0d1e29aa6b091523930ef868" {
		return recipe{}, fmt.Errorf("missing-run successor changed its historical refusal or instance")
	}
	const prior = `"flow instance activation readiness: dynamic flow runtime readiness requires run_id"`
	const current = `"prepare native component construction: flow instance activation readiness: dynamic flow runtime readiness requires run_id"`
	if strings.Count(row.After, prior) != 1 {
		return recipe{}, fmt.Errorf("missing-run exact refusal multiplicity changed")
	}
	expected := recipe{Family: row.Family, File: row.File, Function: row.Function, Before: row.Before, After: row.After,
		Successor: strings.Replace(row.After, prior, current, 1)}
	if !reflect.DeepEqual(row, expected) {
		return recipe{}, fmt.Errorf("missing-run successor changed more than the exact construction refusal")
	}
	rows, err := checkpointRecipes(source, []recipe{row})
	if err != nil {
		return recipe{}, err
	}
	if _, changes, err := rewriteSource(row.File, source, rows); err != nil || len(changes) != 0 {
		return recipe{}, fmt.Errorf("missing-run successor is not the exact live endpoint: %v", err)
	}
	expected.Successor = ""
	return expected, nil
}

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
	source, err := os.ReadFile(filepath.Join("..", "..", "..", row.File))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missingRunCurrentPredecessor(row, source); err != nil {
		t.Fatal(err)
	}
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
