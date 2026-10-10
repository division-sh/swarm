package main

import (
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeInvalidLifecycleBody(t *testing.T, source string, predecessor bool) (*ast.CompositeLit, *ast.BlockStmt) {
	t.Helper()
	loop := projectionShapeFunction(t, source).Body.List[0].(*ast.RangeStmt)
	if predecessor {
		loop = loop.Body.List[0].(*ast.RangeStmt)
	}
	call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
	return loop.X.(*ast.CompositeLit), call.Args[1].(*ast.FuncLit).Body
}

func nativeInvalidLifecycleCases(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	cases, _ := nativeInvalidLifecycleBody(t, source, predecessor)
	var facts []ast.Stmt
	for _, entry := range cases.Elts {
		var pairFacts []ast.Expr
		for _, field := range entry.(*ast.CompositeLit).Elts {
			pair := field.(*ast.KeyValueExpr)
			key := formattedNativeReadNode(pair.Key)
			if key == "name" || key == "wantError" {
				pairFacts = append(pairFacts, pair)
			}
		}
		facts = append(facts, &ast.ExprStmt{X: &ast.CompositeLit{Elts: pairFacts}})
	}
	return formattedNativeReadNode(&ast.BlockStmt{List: facts})
}

func nativeInvalidLifecycleWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	_, body := nativeInvalidLifecycleBody(t, source, predecessor)
	var statements []ast.Stmt
	proof := false
	for _, statement := range body.List {
		text := formattedNativeReadNode(statement)
		proof = proof || strings.HasPrefix(text, "node := pipelineNode(")
		if strings.HasPrefix(text, "instancePath := ") || strings.HasPrefix(text, "entityID := ") || proof {
			statements = append(statements, statement)
		}
		if strings.Contains(text, "invalid %s persistence error") {
			break
		}
	}
	if !proof {
		t.Fatal("invalid-lifecycle cohort lost its original ordered handler/refusal")
	}
	statements = append(statements, body.List[len(body.List)-2:]...)
	selected := &ast.BlockStmt{List: statements}
	if predecessor {
		astutil.Apply(selected, func(cursor *astutil.Cursor) bool {
			if call, ok := cursor.Node().(*ast.CallExpr); ok && formattedNativeReadNode(call.Fun) == "store.Load" {
				call.Fun = mutationSeedExpression(t, "fixture.Persistence.LoadWorkflowInstance")
			}
			return true
		}, nil)
	}
	return formattedNativeReadNode(selected)
}

func TestNativeInvalidLifecycleRecipePreservesEveryCaseHandlerAndOriginalReadback(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-invalid-lifecycle-target")
	if nativeInvalidLifecycleCases(t, row.Before, true) != nativeInvalidLifecycleCases(t, row.After, false) || nativeInvalidLifecycleWorkload(t, row.Before, true) != nativeInvalidLifecycleWorkload(t, row.After, false) {
		t.Fatal("invalid-lifecycle case/error/handler/input/refusal/marker workload changed")
	}
	for _, required := range []string{`WorkflowName: "."`, `WorkflowVersion: source.WorkflowVersion()`, `Mode: "static"`, `CurrentState: "active"`, `EntityType: "test_entity"`, `Fields: map[string]any{"marker": "unchanged"}`, `instance.Fields = map[string]any{}`, `instance.WorkflowName, instance.Mode = "other-flow", "template"`, `fixture.MissingFields(ctx, testPipelineRunID, entityID)`, `fixture.Terminated(ctx, testPipelineRunID, instancePath, time.Now().UTC())`, `fixture.Draining(ctx, testPipelineRunID, instancePath)`, `expectedPresence = WorkflowTargetPersistenceLifecycleOnly`, `err != nil || found`, `!reflect.DeepEqual(before, after)`} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("native lifecycle prestate or observation weakened: %s", required)
		}
	}
}

func TestNativeInvalidLifecycleOracleRejectsLostCasesErrorsAndFieldConservation(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-invalid-lifecycle-target")
	for _, required := range []string{`"lifecycle-only"`, `"wrong-descriptor"`, `"terminated-status"`, `"draining-status"`, `testCase.wantError`, `persisted.Revision != 1`, `persisted.Fields["marker"] != "unchanged"`} {
		changed := strings.ReplaceAll(row.After, required, "false")
		if nativeInvalidLifecycleCases(t, row.Before, true) == nativeInvalidLifecycleCases(t, changed, false) && nativeInvalidLifecycleWorkload(t, row.Before, true) == nativeInvalidLifecycleWorkload(t, changed, false) {
			t.Fatalf("lost original lifecycle case/refusal/assertion admitted: %s", required)
		}
	}
}
