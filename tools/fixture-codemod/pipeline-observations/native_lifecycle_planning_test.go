package main

import (
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeLifecyclePlanningWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		loop := body.List[0].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body = call.Args[1].(*ast.FuncLit).Body
	}
	var kept []ast.Stmt
	for index, statement := range body.List {
		text := formattedNativeReadNode(statement)
		if nativeLifecyclePlanningAddedCheck(t, body.List, index) {
			continue
		}
		if nativeLifecyclePlanningSetup(text) {
			continue
		}
		kept = append(kept, statement)
	}
	body.List = kept
	if predecessor {
		astutil.Apply(body, func(cursor *astutil.Cursor) bool {
			nativeTransitionMutationBindings(t, cursor.Node())
			nativeLifecyclePlanningEvent(t, cursor.Node())
			return true
		}, nil)
	}
	return formattedNativeReadNode(body)
}

func nativeLifecyclePlanningAddedCheck(t *testing.T, statements []ast.Stmt, index int) bool {
	t.Helper()
	if index == 0 {
		return false
	}
	previous := formattedNativeReadNode(statements[index-1])
	var expected string
	switch {
	case strings.HasPrefix(previous, "node, _, found := "):
		expected = `if !found {t.Fatal("selected lifecycle cause has no declared handler")}`
	case strings.HasPrefix(previous, "id, err := "):
		expected = `if err != nil {t.Fatal(err)}`
	default:
		return false
	}
	if formattedNativeReadNode(statements[index]) != formattedNativeReadNode(projectionShapeStatement(t, expected)) {
		t.Fatal("native lifecycle planning identity stopped failing closed")
	}
	return true
}

func nativeLifecyclePlanningSetup(text string) bool {
	for _, prefix := range []string{
		"store, ctx := ", "pc := ", "source := ", "fixture := ", "ctx := ",
		"node, _, found := ", "deliveryRoute := ", "fixture.Publish(",
		"id, err := ", "pending, err := ", "if err != nil || pending.EventID ",
	} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func nativeLifecyclePlanningEvent(t *testing.T, node ast.Node) {
	t.Helper()
	call, ok := node.(*ast.CallExpr)
	if !ok {
		return
	}
	switch formattedNativeReadNode(call.Fun) {
	case "workflowLifecycleEventForTest":
		if len(call.Args) != 8 || formattedNativeReadNode(call.Args[3]) != `"orders"` || formattedNativeReadNode(call.Args[6]) != `"orders/lifecycle.transitioned"` {
			t.Fatal("original accepted lifecycle event changed")
		}
		accepted := mutationSeedExpression(t, `eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "orders/lifecycle.transitioned", "operator", "", []byte(`+"`{}`"+`), 0, runtimecorrelation.RunIDFromContext(ctx), handlerTestWorkflowEnvelope("orders", path, entityID), testWorkflowRoutingSource("orders", path, entityID), now, executionmode.Live)`).(*ast.CallExpr)
		*call = *accepted
	case "admitTestLifecycleDeliveryOccurrence":
		call.Fun = mutationSeedExpression(t, "effect.WithExecutionOccurrence")
		call.Args = []ast.Expr{mutationSeedExpression(t, `"delivery"`), mutationSeedExpression(t, "pending.DeliveryID")}
	}
}

func TestNativeLifecyclePlanningRecipePreservesCausesErrorsAndPartialPlanConservation(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-lifecycle-cause-planning")
	if nativeLifecyclePlanningWorkload(t, row.Before, true) != nativeLifecyclePlanningWorkload(t, row.After, false) {
		t.Fatal("lifecycle source, entity, accepted/gate events, five causes or original error/partial-plan conservation changed")
	}
	for _, required := range []string{"fixture.Construct(ctx, instance)", "fixture.Publish(ctx, accepted, deliveryRoute)", "pending.Status != deliverylifecycle.StatusPending", "pending.EventID != accepted.ID()", "pending.Route.Target != deliveryRoute.Target", "pending.Route.Recipient != deliveryRoute.Recipient", "effect.WithExecutionOccurrence(\"delivery\", pending.DeliveryID)"} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("native cause preparation lost exact publication/owner: %s", required)
		}
	}
}

func TestNativeLifecyclePlanningOracleRejectsLostCasesGateDiagnosticAndConservation(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-lifecycle-cause-planning")
	for _, required := range []string{`"selected cause"`, `"wrong flow"`, `"wrong prepared target"`, `"unowned same-flow carrier"`, `"standalone gate lacks card proof"`, "(err != nil) != tc.wantError", "!reflect.DeepEqual(plan, PreparedWorkflowLifecycleMutation{})", "!reflect.DeepEqual(candidate, instance)", "gate transition has no authoritative activation/card"} {
		changed := strings.ReplaceAll(row.After, required, "false")
		if nativeLifecyclePlanningWorkload(t, row.Before, true) == nativeLifecyclePlanningWorkload(t, changed, false) {
			t.Fatalf("weakened lifecycle cause witness admitted: %s", required)
		}
	}
}
