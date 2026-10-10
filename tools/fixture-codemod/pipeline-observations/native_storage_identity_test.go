package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

var nativeStorageIdentityRoots = map[string]bool{
	"TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed":       true,
	"TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores":  true,
	"TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata": true,
}

func TestNativeStorageIdentityRecipesPreserveEveryWorkloadStatement(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Family != "native-storage-identity" {
			continue
		}
		if !nativeStorageIdentityRoots[row.Function] || seen[row.Function] {
			t.Fatalf("unreviewed or repeated storage identity root: %s", row.Function)
		}
		seen[row.Function] = true
		if nativeStorageIdentityWorkload(t, row.Before, row.Function, true) != nativeStorageIdentityWorkload(t, row.After, row.Function, false) {
			t.Fatalf("storage identity workload/assertion changed: %s", row.Function)
		}
	}
	if len(seen) != len(nativeStorageIdentityRoots) {
		t.Fatalf("storage identity cohort=%d, want all three roots", len(seen))
	}
}

func nativeStorageIdentityWorkload(t *testing.T, source, root string, predecessor bool) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "identity.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	body := file.Decls[0].(*ast.FuncDecl).Body
	if predecessor && root != "TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata" {
		loop := body.List[len(body.List)-1].(*ast.RangeStmt)
		call := loop.Body.List[len(loop.Body.List)-1].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = call.Args[1].(*ast.FuncLit).Body
	}
	setup := 1
	if root == "TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata" && predecessor {
		setup = 2
	}
	if root == "TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores" && !predecessor {
		setup = 3
	}
	if !storageIdentitySetupMatches(body, root, predecessor) {
		t.Fatal("storage identity setup or run-owner binding changed")
	}
	body.List = body.List[setup:]
	normalizeStorageIdentityRunSeeds(t, body)
	normalizeStorageIdentityCalls(body)
	return formattedNativeReadNode(body)
}

func storageIdentitySetupMatches(body *ast.BlockStmt, root string, predecessor bool) bool {
	expected := []string{"fixture := open(t)"}
	if predecessor {
		switch root {
		case "TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed":
			expected = []string{"store := backend.store(t)"}
		case "TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores":
			expected = []string{"store, ctx := setup.open(t)"}
		case "TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata":
			expected = []string{"db := newSQLiteWorkflowInstanceStoreTestDB(t)", "store := newSQLiteWorkflowInstanceStoreForTest(t, db)"}
		}
	} else if root == "TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores" {
		expected = append(expected, "ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)", "if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil { t.Fatal(err) }")
	}
	for i, source := range expected {
		file, err := parser.ParseFile(token.NewFileSet(), "setup.go", "package proof\nfunc setup(){"+source+"}", parser.AllErrors)
		if err != nil || i >= len(body.List) {
			return false
		}
		want := file.Decls[0].(*ast.FuncDecl).Body.List[0]
		if formattedNativeReadNode(body.List[i]) != formattedNativeReadNode(want) {
			return false
		}
	}
	return true
}

func TestNativeStorageIdentityOracleRejectsChangedRunBindingAndIgnoredSeedFailure(t *testing.T) {
	root := "TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores"
	for _, setup := range []string{
		`fixture := open(t); ctx := runtimecorrelation.WithRunID(fixture.Context, foreignRun); if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil { t.Fatal(err) }`,
		`fixture := open(t); ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID); if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil { _ = err }`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "setup.go", "package proof\nfunc setup(){"+setup+"}", parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		if storageIdentitySetupMatches(file.Decls[0].(*ast.FuncDecl).Body, root, false) {
			t.Fatal("changed native run binding or ignored refusal accepted")
		}
	}
}

func normalizeStorageIdentityRunSeeds(t *testing.T, body *ast.BlockStmt) {
	t.Helper()
	ast.Inspect(body, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, statement := range block.List {
			guard, ok := statement.(*ast.IfStmt)
			if !ok || guard.Init == nil {
				continue
			}
			init, ok := guard.Init.(*ast.AssignStmt)
			if !ok || len(init.Rhs) != 1 {
				continue
			}
			call, ok := init.Rhs[0].(*ast.CallExpr)
			if !ok || formattedNativeReadNode(call.Fun) != "fixture.RequireRun" {
				continue
			}
			if len(call.Args) != 2 || formattedNativeReadNode(guard.Cond) != "err != nil" || formattedNativeReadNode(guard.Body) != "{\n\tt.Fatal(err)\n}" {
				t.Fatal("native run materialization changed its fail-closed assertion")
			}
			block.List[i] = &ast.ExprStmt{X: &ast.CallExpr{Fun: ast.NewIdent("ensurePipelineTestRun"), Args: []ast.Expr{ast.NewIdent("t"), ast.NewIdent("store"), call.Args[1]}}}
		}
		return true
	})
}

func normalizeStorageIdentityCalls(body *ast.BlockStmt) {
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch formattedNativeReadNode(call.Fun) {
		case "store.create", "store.upsert":
			call.Fun, _ = parser.ParseExpr("fixture.Construct")
		case "store.Load":
			call.Fun, _ = parser.ParseExpr("fixture.Persistence.LoadWorkflowInstance")
		}
		for i, arg := range call.Args {
			if formattedNativeReadNode(arg) == "testAuthorActivityContext(t, context.Background())" {
				call.Args[i], _ = parser.ParseExpr("fixture.Context")
			}
		}
		return true
	})
}

func TestNativeStorageIdentityOracleRejectsWeakenedScopeRouteAndParentAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	conditions := map[string]string{
		"TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed":       "gotA.CurrentState != \"source_state\" || gotB.CurrentState != \"fork_state\"",
		"TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores":  "else if found",
		"TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata": "identity.ParentRoute.EntityID != \"parent-ent\"",
	}
	for _, row := range rows {
		condition, found := conditions[row.Function]
		if !found || row.Family != "native-storage-identity" {
			continue
		}
		replacement := "false"
		if condition == "else if found" {
			replacement = "else if false"
		}
		mutated := strings.Replace(row.After, condition, replacement, 1)
		if mutated == row.After || nativeStorageIdentityWorkload(t, row.Before, row.Function, true) == nativeStorageIdentityWorkload(t, mutated, row.Function, false) {
			t.Fatalf("weakened identity assertion admitted: %s", row.Function)
		}
	}
}
