package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeDeliveryTargetRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-delivery-target-prestate" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 2 {
		t.Fatal("delivery target cohort must include refusal and constructed pre-state")
	}
	return selected
}

func TestNativeDeliveryTargetRecipesPreserveAllStateAndTargetAssertions(t *testing.T) {
	for _, row := range nativeDeliveryTargetRecipes(t) {
		if nativeDeliveryTargetWorkload(t, row, row.Before, true) != nativeDeliveryTargetWorkload(t, row, row.After, false) {
			t.Fatalf("delivery target state/payload/assertion changed: %s", row.Function)
		}
	}
}

func nativeDeliveryTargetWorkload(t *testing.T, row recipe, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		loop := body.List[0].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = call.Args[1].(*ast.FuncLit).Body
		body.List = nativeDeliveryTargetClaimStatements(t, body.List)
	}
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if handlerNativeSetupStatement(t, cursor, predecessor) || nativeEngineReadSetup(t, cursor, row.Function, predecessor) {
			return false
		}
		if predecessor {
			nativeEngineReadBindings(t, cursor.Node())
			if call, ok := cursor.Node().(*ast.CallExpr); ok {
				handlerNativeRootCall(t, call, true)
				nativeDeliveryTargetEventCall(t, row.Function, call)
			}
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func nativeDeliveryTargetClaimStatements(t *testing.T, statements []ast.Stmt) []ast.Stmt {
	t.Helper()
	var output []ast.Stmt
	for _, stmt := range statements {
		source := formattedNativeReadNode(stmt)
		if source == "seedExactOnceEvent(t, store, ctx, evt)" {
			continue
		}
		if !strings.HasPrefix(source, "deliveryCtx := withClaimedWorkflowNodePublicationForTest(") {
			output = append(output, stmt)
			continue
		}
		call := stmt.(*ast.AssignStmt).Rhs[0].(*ast.CallExpr)
		route := projectionShapeStatement(t, "route := routeValue").(*ast.AssignStmt)
		route.Rhs[0] = call.Args[4]
		output = append(output, route,
			projectionShapeStatement(t, "fixture.Publish(ctx, evt, route)"),
			projectionShapeStatement(t, "evt = eventtest.TargetRouted(evt, target)"),
			projectionShapeStatement(t, "deliveryCtx, stopHeartbeat := claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, evt, route)"),
			projectionShapeStatement(t, `defer func(){if err:=stopHeartbeat();err!=nil{t.Errorf("native scenario heartbeat cleanup: %v",err)}}()`))
	}
	return output
}

func nativeDeliveryTargetEventCall(t *testing.T, name string, call *ast.CallExpr) {
	t.Helper()
	if name != "TestDeliveryTargetApplicationCarriesConstructedScenarioPreStateThroughMutationOnSQLiteAndPostgres" || formattedNativeReadNode(call.Fun) != "handlerTestRootIngress" {
		return
	}
	if len(call.Args) != 10 || formattedNativeReadNode(call.Args[7]) != `""` {
		t.Fatal("original delivery scenario event identity changed")
	}
	call.Fun = mutationSeedExpression(t, "eventtest.ExistingRunRootIngressWithRoutingSource")
	call.Args = append(append([]ast.Expr{}, call.Args[:7]...), call.Args[8], mutationSeedExpression(t, `testWorkflowRoutingSource(".", instancePath, entityID)`), call.Args[9])
}

func TestNativeDeliveryTargetOracleRejectsLostRefusalAndPreStateAssertions(t *testing.T) {
	for _, row := range nativeDeliveryTargetRecipes(t) {
		for _, condition := range []string{"len(instances) != 0", "!result.Handled", `instance.Fields["marker"] != "preserved"`, `!instance.Gates["./approved"]`, "instance.Revision != 2", "!instance.CreatedAt.Equal(occurredAt)", "!instance.EnteredStageAt.Equal(occurredAt)"} {
			if !strings.Contains(row.After, condition) {
				continue
			}
			changed := strings.Replace(row.After, condition, "false", 1)
			if nativeDeliveryTargetWorkload(t, row, row.Before, true) == nativeDeliveryTargetWorkload(t, row, changed, false) {
				t.Fatalf("weakened delivery target assertion accepted: %s", condition)
			}
		}
	}
}
