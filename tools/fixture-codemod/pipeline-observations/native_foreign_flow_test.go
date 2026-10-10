package main

import (
	"encoding/json"
	"go/ast"
	"go/token"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeForeignFlowRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-foreign-flow-refusal" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 1 {
		t.Fatal("foreign-flow cohort must retain its complete original root")
	}
	return selected[0]
}

func nativeForeignFlowWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if nativeEngineReadSetup(t, cursor, "", predecessor) || nativeForeignFlowSetup(cursor) {
			return false
		}
		if predecessor {
			if call, ok := cursor.Node().(*ast.CallExpr); ok && formattedNativeReadNode(call.Fun) == "testWorkflowStoreRunContext" {
				cursor.Replace(ast.NewIdent("ctx"))
				return false
			}
			nativeForeignFlowBindings(t, cursor.Node())
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func nativeForeignFlowSetup(cursor *astutil.Cursor) bool {
	if _, ok := cursor.Node().(ast.Stmt); !ok {
		return false
	}
	text := formattedNativeReadNode(cursor.Node())
	for _, prefix := range []string{
		"_, db, _ := testutil.StartPostgres(t)", "repo := pipelineEngineStateRepo{",
		"source := loadWorkflowTempSource(", "pc := fixture.NewCoordinator(",
		"before, exists, err := fixture.Persistence.LoadWorkflowInstance(",
		"after, exists, err := fixture.Persistence.LoadWorkflowInstance(",
	} {
		if strings.HasPrefix(text, prefix) {
			cursor.Delete()
			return true
		}
	}
	conditional, ok := cursor.Node().(*ast.IfStmt)
	if ok && len(conditional.Body.List) == 1 &&
		(strings.HasPrefix(formattedNativeReadNode(conditional.Body.List[0]), `t.Fatalf("before foreign-flow refusal:`) ||
			strings.HasPrefix(formattedNativeReadNode(conditional.Body.List[0]), `t.Fatalf("foreign-flow refusal mutated its original target:`)) {
		cursor.Delete()
		return true
	}
	return false
}

func nativeForeignFlowBindings(t *testing.T, node ast.Node) {
	t.Helper()
	nativeEngineReadBindings(t, node)
	if call, ok := node.(*ast.CallExpr); ok && formattedNativeReadNode(call) == "loadEngineEvaluationForTest(t, ctx, repo, address)" {
		call.Args[1] = ast.NewIdent("executionCtx")
	}
	if pair, ok := node.(*ast.KeyValueExpr); ok && formattedNativeReadNode(pair.Key) == "WorkflowVersion" && formattedNativeReadNode(pair.Value) == `"1.6.0"` {
		pair.Value = mutationSeedExpression(t, "source.WorkflowVersion()")
	}
	if assignment, ok := node.(*ast.AssignStmt); ok {
		if formattedNativeReadNode(assignment) == `ctx := withPipelineFlowScope(testWorkflowStoreRunContext(t, store), "flow-b")` {
			assignment.Lhs[0] = ast.NewIdent("executionCtx")
			assignment.Rhs[0].(*ast.CallExpr).Args[0] = ast.NewIdent("ctx")
		}
		if len(assignment.Rhs) == 1 && strings.Contains(formattedNativeReadNode(assignment.Rhs[0]), ").CommitEngineMutation(") {
			assignment.Tok = token.ASSIGN
			assignment.Rhs[0].(*ast.CallExpr).Args[0] = ast.NewIdent("executionCtx")
		}
	}
}

func TestNativeForeignFlowRecipePreservesTargetCandidateAndAuthorizationRefusal(t *testing.T) {
	row := nativeForeignFlowRecipe(t)
	if nativeForeignFlowWorkload(t, row.Before, true) != nativeForeignFlowWorkload(t, row.After, false) {
		t.Fatal("foreign-flow target, candidate or refusal assertion changed")
	}
	for _, required := range []string{
		`"flow-a/schema.yaml"`, `"flow-b/schema.yaml"`, "repo := pipelineEngineStateRepo{coordinator: pc}",
		"fixture.RequireRun(ctx, testPipelineRunID)", "!reflect.DeepEqual(before, after)",
	} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("native foreign-flow ownership/conservation proof missing: %s", required)
		}
	}
}

func TestNativeForeignFlowOracleRejectsLostTargetScopeAndRefusal(t *testing.T) {
	row := nativeForeignFlowRecipe(t)
	for _, original := range []string{
		`withPipelineFlowScope(ctx, "flow-b")`, `testEngineStateAddress("flow-b", "flow-a", entityID)`,
		`"note": "bad write"`, `err == nil || !strings.Contains(err.Error(), "cross_flow_write_forbidden")`,
	} {
		if !strings.Contains(row.After, original) {
			t.Fatalf("foreign-flow obligation missing: %s", original)
		}
		changed := strings.Replace(row.After, original, "false", 1)
		if nativeForeignFlowWorkload(t, row.Before, true) == nativeForeignFlowWorkload(t, changed, false) {
			t.Fatalf("weakened foreign-flow obligation admitted: %s", original)
		}
	}
}
