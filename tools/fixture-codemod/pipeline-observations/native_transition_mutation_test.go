package main

import (
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeTransitionMutationBody(t *testing.T, source string, predecessor bool) *ast.BlockStmt {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		last := len(body.List) - 1
		loop := body.List[last].(*ast.RangeStmt)
		call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
		body.List = append(body.List[:last], call.Args[1].(*ast.FuncLit).Body.List...)
	}
	return body
}

func nativeTransitionMutationCounts(t *testing.T, closure *ast.FuncLit) string {
	t.Helper()
	for _, statement := range closure.Body.List {
		loop, ok := statement.(*ast.RangeStmt)
		if ok && formattedNativeReadNode(loop.Key) == "_" && formattedNativeReadNode(loop.Value) == "table" {
			return formattedNativeReadNode(loop.X)
		}
	}
	t.Fatal("transition refusal lost complete fourteen-table conservation")
	return ""
}

func nativeTransitionMutationWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := nativeTransitionMutationBody(t, source, predecessor)
	var kept []ast.Stmt
	for index, statement := range body.List {
		text := formattedNativeReadNode(statement)
		if index > 0 && strings.HasPrefix(formattedNativeReadNode(body.List[index-1]), "application, err := ") {
			if text != formattedNativeReadNode(projectionShapeStatement(t, "if err != nil { t.Fatal(err) }")) {
				t.Fatal("native target preparation stopped failing closed")
			}
			continue
		}
		if nativeTransitionMutationSetup(text) {
			continue
		}
		if strings.HasPrefix(text, "rowCounts := ") {
			closure := statement.(*ast.AssignStmt).Rhs[0].(*ast.FuncLit)
			kept = append(kept, projectionShapeStatement(t, "rowCounts := conservedApplicationRows("+nativeTransitionMutationCounts(t, closure)+")"))
			continue
		}
		kept = append(kept, statement)
	}
	body.List = kept
	if predecessor {
		astutil.Apply(body, func(cursor *astutil.Cursor) bool {
			nativeTransitionMutationBindings(t, cursor.Node())
			return true
		}, nil)
	}
	return formattedNativeReadNode(body)
}

func nativeTransitionMutationSetup(text string) bool {
	for _, prefix := range []string{
		"db, store := ", "pc := ", "var ctx ", `if backend == "sqlite"`,
		"fixture := ", "ctx := correlation.WithRunID(", "if err := fixture.RequireRun(",
		"seedExactOnceEvent(", "route := ", "fixture.Publish(", "ctx, stop := ", "defer func()",
		"publicationEffect, err = admitTestLifecycleDeliveryOccurrence(",
		"claim, claimed := ", "if !claimed ", "publicationEffect, err = publicationEffect.WithExecutionOccurrence(",
		"application, err := ", "ctx = withDeliveryTargetApplication(",
	} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func nativeTransitionMutationBindings(t *testing.T, node ast.Node) {
	t.Helper()
	if pair, ok := node.(*ast.KeyValueExpr); ok {
		switch formattedNativeReadNode(pair.Key) {
		case "WorkflowVersion":
			if formattedNativeReadNode(pair.Value) != `"1"` {
				t.Fatal("original transition component version changed")
			}
			pair.Value = mutationSeedExpression(t, "source.WorkflowVersion()")
		case "store":
			if formattedNativeReadNode(pair.Value) == "store" {
				pair.Value = mutationSeedExpression(t, "pc.workflowStore")
			}
		}
	}
	call, ok := node.(*ast.CallExpr)
	if !ok {
		return
	}
	switch formattedNativeReadNode(call.Fun) {
	case "correlation.WithInboundEvent":
		if formattedNativeReadNode(call.Args[1]) == "accepted" {
			call.Args[1] = mutationSeedExpression(t, "application.Event()")
		}
	case "store.upsert":
		call.Fun = mutationSeedExpression(t, "fixture.Construct")
	case "store.Load":
		call.Fun = mutationSeedExpression(t, "fixture.Persistence.LoadWorkflowInstance")
	case "handlerTestRootIngress":
		if len(call.Args) != 10 || formattedNativeReadNode(call.Args[7]) != `""` {
			t.Fatal("original accepted transition event changed")
		}
		call.Fun = mutationSeedExpression(t, "eventtest.ExistingRunRootIngressWithRoutingSource")
		call.Args = append(append([]ast.Expr{}, call.Args[:7]...), call.Args[8], mutationSeedExpression(t, `testWorkflowRoutingSource(".", testPipelineRunID, entityID)`), call.Args[9])
	}
}

func TestNativeTransitionMutationRecipePreservesAllEvidenceAndConservation(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-compiled-transition-mutation")
	if nativeTransitionMutationWorkload(t, row.Before, true) != nativeTransitionMutationWorkload(t, row.After, false) {
		t.Fatal("compiled declaration/evidence table, exact event, ordered rejection, count/state conservation or accepted history changed")
	}
	for _, required := range []string{
		"fixture.Construct(ctx, instance)", "fixture.Publish(ctx, accepted, route)",
		"claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, accepted, route)",
		"publicationEffect.WithExecutionOccurrence(\"delivery\", claim.DeliveryID())",
		"fixture.ApplicationStorage(ctx)", "out[table] = len(value.Rows)",
		"pc.prepareDeliveryTargetApplication(ctx, node.Key(), MustDeliveryTargetHandler(node).ForEvent(accepted.Type()), handlers[\"advance\"], accepted, route.Target)",
		"ctx = withDeliveryTargetApplication(ctx, application)",
		"native application snapshot omitted %s", "if !claimed {", "if err := stop(); err != nil {",
	} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("native transition proof lost exact owner/occurrence/count evidence: %s", required)
		}
	}
}

func TestNativeTransitionMutationOracleRejectsLostCasesAndSuccessAssertions(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-compiled-transition-mutation")
	for _, required := range []string{
		`"foreign_rule"`, `"forged_gate"`, `"unknown_timer"`, `"noop_cannot_smuggle_evidence"`,
		`"duplicate_effect"`, `"event_time"`, `"missing_state_cause"`,
		"!reflect.DeepEqual(counts, rowCounts())", "!reflect.DeepEqual(before, after)",
		"after.Revision != before.Revision+1", "!record.Evidence.RuleSelection().Equal(selection)",
		"!reflect.DeepEqual(record, hydrated)",
	} {
		changed := strings.ReplaceAll(row.After, required, "false")
		if nativeTransitionMutationWorkload(t, row.Before, true) == nativeTransitionMutationWorkload(t, changed, false) {
			t.Fatalf("weakened compiled-transition evidence admitted: %s", required)
		}
	}
}
