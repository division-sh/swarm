package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"
)

func initialPreparationRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-initial-preparation" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 1 {
		t.Fatal("initial preparation cohort is not exact")
	}
	return selected[0]
}

func initialPreparationWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	body.List = body.List[4:]
	if predecessor {
		ast.Inspect(body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if ok && formattedNativeReadNode(call.Fun) == "store.Load" {
				call.Fun = mutationSeedExpression(t, "fixture.Persistence.LoadWorkflowInstance")
			}
			return true
		})
	}
	return formattedNativeReadNode(body)
}

func TestNativeInitialPreparationRecipePreservesEmissionsRefusalAndNoPersistence(t *testing.T) {
	row := initialPreparationRecipe(t)
	if initialPreparationWorkload(t, row.Before, true) != initialPreparationWorkload(t, row.After, false) {
		t.Fatal("initial preparation workload or persistence assertions changed")
	}
	body := projectionShapeFunction(t, row.After).Body
	for index, expected := range map[int]string{
		1: "store := fixture.Persistence.store",
		2: "store.lifecycleOwner = &workflowInitialMaterializationTestOwner{emissions: 1}",
		3: "ctx := withLiveWorkflowInitialEntry(fixture.Context)",
	} {
		if formattedNativeReadNode(body.List[index]) != formattedNativeReadNode(projectionShapeStatement(t, expected)) {
			t.Fatal("native preparation regained another owner or changed its hostile emissions")
		}
	}
	for _, condition := range []string{"err == nil", "!strings.Contains(err.Error(), \"lifecycle emissions outside its atomic commit\")", "err != nil || found"} {
		changed := strings.Replace(row.After, condition, "false", 1)
		if changed == row.After || initialPreparationWorkload(t, row.Before, true) == initialPreparationWorkload(t, changed, false) {
			t.Fatalf("weakened initial-entry proof accepted: %s", condition)
		}
	}
}
