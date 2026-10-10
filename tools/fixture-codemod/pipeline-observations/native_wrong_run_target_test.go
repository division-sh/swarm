package main

import (
	"go/ast"
	"strings"
	"testing"
)

func nativeWrongRunTargetBody(t *testing.T, source string, predecessor bool) (ast.Stmt, *ast.RangeStmt, *ast.BlockStmt) {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	loop := body.List[1].(*ast.RangeStmt)
	if predecessor {
		loop = loop.Body.List[0].(*ast.RangeStmt)
	}
	call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
	return body.List[0], loop, call.Args[1].(*ast.FuncLit).Body
}

func nativeWrongRunTargetWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	coordinate, cases, body := nativeWrongRunTargetBody(t, source, predecessor)
	statements := []ast.Stmt{coordinate}
	proof := false
	for index, statement := range body.List {
		text := formattedNativeReadNode(statement)
		proof = proof || strings.HasPrefix(text, "stateBefore, lifecycleBefore := ")
		if (proof && index != len(body.List)-1) || strings.HasPrefix(text, "entityID := ") || strings.HasPrefix(text, "now := ") {
			statements = append(statements, statement)
		}
	}
	if !proof {
		t.Fatal("wrong-root cohort lost original ordered execution/count proof")
	}
	return formattedNativeReadNode(cases.X) + formattedNativeReadNode(&ast.BlockStmt{List: statements})
}

func nativeWrongRunCompleteSeed(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	_, _, body := nativeWrongRunTargetBody(t, source, predecessor)
	for _, statement := range body.List {
		branch, ok := statement.(*ast.SwitchStmt)
		if !ok || formattedNativeReadNode(branch.Tag) != "testCase.seed" {
			continue
		}
		complete := branch.Body.List[0].(*ast.CaseClause)
		if formattedNativeReadNode(complete.List[0]) != `"complete"` || len(complete.Body) != 1 {
			t.Fatal("original complete-target seed changed")
		}
		conditional := complete.Body[0].(*ast.IfStmt)
		call := conditional.Init.(*ast.AssignStmt).Rhs[0].(*ast.CallExpr)
		if predecessor {
			normalizeNativeWrongRunCompleteSeed(t, call)
		}
		return formattedNativeReadNode(conditional)
	}
	t.Fatal("wrong-root cohort lost native/predecessor construction")
	return ""
}

func normalizeNativeWrongRunCompleteSeed(t *testing.T, call *ast.CallExpr) {
	t.Helper()
	if formattedNativeReadNode(call.Fun) != "store.upsert" {
		t.Fatal("complete-target predecessor owner changed")
	}
	call.Fun = mutationSeedExpression(t, "fixture.Construct")
	literal := call.Args[1].(*ast.CallExpr).Args[0].(*ast.CompositeLit)
	for _, field := range literal.Elts {
		pair := field.(*ast.KeyValueExpr)
		if formattedNativeReadNode(pair.Key) == "WorkflowVersion" {
			if formattedNativeReadNode(pair.Value) != `"1"` {
				t.Fatal("original component version changed")
			}
			pair.Value = mutationSeedExpression(t, "source.WorkflowVersion()")
		}
	}
}

func nativeWrongRunTargetReadback(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	_, _, body := nativeWrongRunTargetBody(t, source, predecessor)
	readback := body.List[len(body.List)-1].(*ast.IfStmt)
	if formattedNativeReadNode(readback.Cond) != `testCase.seed != ""` {
		t.Fatal("wrong-root seeded readback disappeared")
	}
	var assertion *ast.IfStmt
	if predecessor {
		assertion = readback.Body.List[len(readback.Body.List)-1].(*ast.IfStmt)
		if formattedNativeReadNode(assertion.Cond) != `err != nil || !strings.Contains(marker, "unchanged")` {
			t.Fatal("original marker/read-error assertion changed")
		}
		assertion.Init = nil
		assertion.Cond = mutationSeedExpression(t, `err != nil || !found || !strings.Contains(marker, "unchanged")`)
	} else {
		assertion = readback.Body.List[2].(*ast.IfStmt)
	}
	return formattedNativeReadNode(assertion)
}

func TestNativeWrongRunTargetRecipePreservesAllFourCasesAndOrderedRefusalCounts(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-wrong-run-target")
	if nativeWrongRunTargetWorkload(t, row.Before, true) != nativeWrongRunTargetWorkload(t, row.After, false) || nativeWrongRunCompleteSeed(t, row.Before, true) != nativeWrongRunCompleteSeed(t, row.After, false) || nativeWrongRunTargetReadback(t, row.Before, true) != nativeWrongRunTargetReadback(t, row.After, false) {
		t.Fatal("wrong-root case table, input, two attempts, count conservation or complete seed changed")
	}
	for _, required := range []string{`fixture.MissingHeader(ctx, testPipelineRunID, wrongRunID)`, `before.Presence != expected`, `WorkflowTargetPersistenceStateOnly`, `EnteredStageAt: now, CreatedAt: now, UpdatedAt: now`, `nativeWorkflowEnginePhysicalCountsForTest(t, fixture, ctx)[table]`, `testRunScopedWorkflowInstanceFromContext(ctx, wrongRunID), runtimeidentity.NormalizeEntityID(entityID)`, `err != nil || !found || !strings.Contains(marker, "unchanged")`, `!reflect.DeepEqual(before.State, persisted)`} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("wrong-root proof lost exact hostile prestate/count/readback: %s", required)
		}
	}
}

func TestNativeWrongRunTargetOracleRejectsLostMarkerAndReadErrors(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-wrong-run-target")
	for _, required := range []string{`!found`, `err != nil`, `!strings.Contains(marker, "unchanged")`} {
		changed := strings.ReplaceAll(row.After, required, "false")
		if nativeWrongRunTargetReadback(t, row.Before, true) == nativeWrongRunTargetReadback(t, changed, false) {
			t.Fatalf("weakened marker/readback assertion admitted: %s", required)
		}
	}
}

func TestNativeWrongRunTargetOracleRejectsLostCasesAttemptsAndConservation(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-wrong-run-target")
	for _, required := range []string{`"complete-existing"`, `"state-only-existing"`, `"materializing"`, `"entityless"`, `newCoordinator(), newCoordinator()`, `"disagrees with current root coordinate"`, `stateAfter != stateBefore`, `lifecycleAfter != lifecycleBefore`} {
		changed := strings.Replace(row.After, required, "false", 1)
		if nativeWrongRunTargetWorkload(t, row.Before, true) == nativeWrongRunTargetWorkload(t, changed, false) {
			t.Fatalf("weakened wrong-root case/assertion admitted: %s", required)
		}
	}
}
