package main

import (
	"go/ast"
	"strings"
	"testing"
)

func nativeNonactiveTargetBody(t *testing.T, source string, predecessor bool) *ast.BlockStmt {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		loop := body.List[0].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		return call.Args[1].(*ast.FuncLit).Body
	}
	return body
}

func nativeNonactiveTargetWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	var statements []ast.Stmt
	for _, statement := range nativeNonactiveTargetBody(t, source, predecessor).List {
		text := formattedNativeReadNode(statement)
		switch {
		case strings.HasPrefix(text, "instancePath := "), strings.HasPrefix(text, "entityID := "), strings.HasPrefix(text, "handler := "):
			statements = append(statements, statement)
		case strings.HasPrefix(text, "evt := "):
			if predecessor {
				call := statement.(*ast.AssignStmt).Rhs[0].(*ast.CallExpr)
				if len(call.Args) != 10 || formattedNativeReadNode(call.Args[7]) != `""` {
					t.Fatal("original delayed-event input changed")
				}
				call.Fun = mutationSeedExpression(t, "eventtest.ExistingRunRootIngressWithRoutingSource")
				call.Args = append(append([]ast.Expr{}, call.Args[:7]...), call.Args[8], mutationSeedExpression(t, `testWorkflowRoutingSource(".", instancePath, entityID)`), call.Args[9])
			}
			statements = append(statements, statement)
		case strings.HasPrefix(text, "execute := "):
			closure := statement.(*ast.AssignStmt).Rhs[0].(*ast.FuncLit)
			statements = append(statements, &ast.BlockStmt{List: closure.Body.List[:4]})
		case strings.HasPrefix(text, "execute("):
			if predecessor {
				call := statement.(*ast.ExprStmt).X.(*ast.CallExpr)
				call.Args = call.Args[:2]
				if nested, ok := call.Args[1].(*ast.CallExpr); ok && formattedNativeReadNode(nested.Fun) == "newCoordinator" {
					nested.Args = nil
				}
			}
			statements = append(statements, statement)
		case strings.Contains(text, `t.Fatalf("non-active replay changed target:`):
			statements = append(statements, statement)
		}
	}
	if len(statements) != 8 {
		t.Fatalf("delayed-event complete workload has %d components, want 8", len(statements))
	}
	return formattedNativeReadNode(&ast.BlockStmt{List: statements})
}

func TestNativeNonactiveTargetRecipePreservesEventHandlerBothAttemptsAndReadback(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-nonactive-delivery-target")
	if nativeNonactiveTargetWorkload(t, row.Before, true) != nativeNonactiveTargetWorkload(t, row.After, false) {
		t.Fatal("delayed/replayed event, handler, attempts or original field/status refusal changed")
	}
	for _, required := range []string{`fixture.Draining(ctx, testPipelineRunID, instancePath)`, `fixture.Publish(ctx, evt, route)`, `pending.Status != deliverylifecycle.StatusPending`, `!bytes.Equal(before, readStorage())`, `after["entity_mutations"] != countsBefore["entity_mutations"]`, `delivery.Status != deliverylifecycle.StatusPending`, `!reflect.DeepEqual(pending, delivery)`} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("delayed target lost native pending/effect conservation: %s", required)
		}
	}
	if strings.Index(row.After, "fixture.Draining(") > strings.Index(row.After, "fixture.Publish(") {
		t.Fatal("original hostile temporal cut reordered")
	}
}

func TestNativeNonactiveTargetOracleRejectsLostAttemptErrorPayloadAndMarker(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-nonactive-delivery-target")
	for _, required := range []string{`"delayed"`, `"replayed after coordinator reconstruction"`, `"lifecycle is not active"`, `{"item_id":"a"}`, `persisted.Revision != 1`, `persisted.Fields["marker"] != "unchanged"`, `persisted.Status != "draining"`} {
		changed := strings.Replace(row.After, required, "false", 1)
		if nativeNonactiveTargetWorkload(t, row.Before, true) == nativeNonactiveTargetWorkload(t, changed, false) {
			t.Fatalf("weakened delayed/replayed workload admitted: %s", required)
		}
	}
}
