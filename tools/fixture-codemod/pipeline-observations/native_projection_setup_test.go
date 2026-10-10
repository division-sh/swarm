package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

var nativeProjectionRoundTripRoots = map[string]bool{
	"TestWorkflowInstanceStoreProjection_RoundTripPreservesCanonicalState":              true,
	"TestWorkflowInstanceStoreProjection_StaticRowsPersistCanonicalFlowPathOnRoundTrip": true,
}

func TestNativeProjectionRoundTripRecipesPreserveEveryWorkloadStatement(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Family != "native-projection-roundtrip" {
			continue
		}
		if row.File != "internal/runtime/pipeline/workflow_instance_store_projection_test.go" || !nativeProjectionRoundTripRoots[row.Function] || seen[row.Function] {
			t.Fatalf("unreviewed or duplicated projection root: %s/%s", row.File, row.Function)
		}
		seen[row.Function] = true
		if projectionRoundTripWorkload(t, row.Before, true) != projectionRoundTripWorkload(t, row.After, false) {
			t.Fatalf("projection workload/assertions changed: %s", row.Function)
		}
	}
	if len(seen) != len(nativeProjectionRoundTripRoots) {
		t.Fatalf("projection cohort=%d, want both round-trip roots", len(seen))
	}
}

func projectionRoundTripWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "projection.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	setup := 2
	if predecessor {
		setup = 3
	}
	fn.Body.List = fn.Body.List[setup:]
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || formattedNativeReadNode(selector.X) != "store" {
			return true
		}
		if len(call.Args) != 2 || formattedNativeReadNode(call.Args[0]) != "testWorkflowStoreRunContext(t, store)" {
			t.Fatal("projection context/workload drift")
		}
		call.Args[0] = ast.NewIdent("ctx")
		switch selector.Sel.Name {
		case "upsert":
			selector.X = ast.NewIdent("fixture")
			selector.Sel.Name = "Construct"
		case "Load":
			selector.X = &ast.SelectorExpr{X: ast.NewIdent("fixture"), Sel: ast.NewIdent("Persistence")}
			selector.Sel.Name = "LoadWorkflowInstance"
		default:
			t.Fatalf("unreviewed projection operation: %s", selector.Sel.Name)
		}
		return true
	})
	return formattedNativeReadNode(fn.Body)
}

func TestNativeProjectionProofControlRejectsWeakenedIdentityAndNumericAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Function != "TestWorkflowInstanceStoreProjection_RoundTripPreservesCanonicalState" {
			continue
		}
		for _, condition := range []string{"got != \"3\"", "identity.ParentRoute.EntityID != parentID"} {
			mutated := strings.Replace(row.After, condition, "false", 1)
			if mutated == row.After || projectionRoundTripWorkload(t, row.Before, true) == projectionRoundTripWorkload(t, mutated, false) {
				t.Fatalf("weakened exact projection assertion admitted: %s", condition)
			}
		}
		return
	}
	t.Fatal("missing projection proof recipe")
}
