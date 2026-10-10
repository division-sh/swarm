package main

import (
	"go/ast"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeEventAuthorityBody(t *testing.T, source string, predecessor bool) *ast.BlockStmt {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	if predecessor {
		last := len(body.List) - 1
		backend := body.List[last].(*ast.RangeStmt)
		body.List = append(body.List[:last], backend.Body.List...)
	}
	return body
}

func nativeEventAuthoritySetup(text string) bool {
	for _, prefix := range []string{
		"db, store := ", "pc := ", "var ctx ", `if backend == "sqlite"`,
		"dialect := ", "seedPipelineEventRecordForDialect(",
		"fixture := ", "ctx := runtimecorrelation.WithRunID(", "if err := fixture.RequireRun(",
		"route := ", "fixture.Publish(", "pending, err := ", "if err != nil || pending.EventID ",
		"publicationEffect, err = admitTestLifecycleDeliveryOccurrence(",
		"publicationEffect, err = publicationEffect.WithExecutionOccurrence(",
	} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func nativeEventAuthorityWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := nativeEventAuthorityBody(t, source, predecessor)
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if cursor.Node() == nil {
			return false
		}
		if block, ok := cursor.Node().(*ast.BlockStmt); ok {
			block.List = nativeEventAuthorityIdentityErrorCut(t, block.List)
		}
		var text string
		if _, ok := cursor.Node().(ast.Stmt); ok {
			text = formattedNativeReadNode(cursor.Node())
		}
		if nativeEventAuthoritySetup(text) {
			cursor.Delete()
			return false
		}
		if strings.HasPrefix(text, "snapshotRows := ") {
			closure := cursor.Node().(*ast.AssignStmt).Rhs[0].(*ast.FuncLit)
			cursor.Replace(projectionShapeStatement(t, "snapshotRows := preservedTypedApplicationRows("+nativeTransitionMutationCounts(t, closure)+")"))
			return false
		}
		if predecessor {
			nativeEventAuthorityBindings(t, cursor.Node())
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func nativeEventAuthorityBindings(t *testing.T, node ast.Node) {
	t.Helper()
	nativeTransitionMutationBindings(t, node)
	call, ok := node.(*ast.CallExpr)
	if ok && formattedNativeReadNode(call.Fun) == "t.Run" && formattedNativeReadNode(call.Args[0]) == `backend + "/" + variant` {
		call.Args[0] = ast.NewIdent("variant")
	}
}

func nativeEventAuthorityIdentityErrorCut(t *testing.T, statements []ast.Stmt) []ast.Stmt {
	t.Helper()
	var kept []ast.Stmt
	for index, statement := range statements {
		if strings.HasPrefix(formattedNativeReadNode(statement), "id, err := deliverylifecycle.DeliveryID(") {
			continue
		}
		if index > 0 && strings.HasPrefix(formattedNativeReadNode(statements[index-1]), "id, err := deliverylifecycle.DeliveryID(") {
			if formattedNativeReadNode(statement) != formattedNativeReadNode(projectionShapeStatement(t, "if err != nil {t.Fatal(err)}")) {
				t.Fatal("exact publication identity stopped failing closed")
			}
			continue
		}
		kept = append(kept, statement)
	}
	return kept
}

func TestNativeEventAuthorityRecipePreservesAllElevenVariantsAndTypedConservation(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-transition-event-authority")
	if nativeEventAuthorityWorkload(t, row.Before, true) != nativeEventAuthorityWorkload(t, row.After, false) {
		t.Fatal("accepted events, variant table, coherent projections, two success controls or exact refusals/conservation changed")
	}
	for _, required := range []string{
		"fixture.Construct(ctx, instance)", "fixture.Publish(ctx, accepted, route)", "fixture.Publish(ctx, other, route)",
		"pc.deliveryStore.Snapshot(ctx, id)", "pending.Status != deliverylifecycle.StatusPending",
		"publicationEffect.WithExecutionOccurrence(\"delivery\", pending.DeliveryID)",
		"fixture.ApplicationStorage(ctx)", "native typed snapshot omitted %s", "out[table] = value.Rows",
	} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("native event authority lost original selected evidence: %s", required)
		}
	}
	if strings.Contains(row.After, "claimNativeWorkflowHandlerPublicationForTest") {
		t.Fatal("direct component control must not be replaced by a claimed/stamped delivery")
	}
}

func TestNativeEventAuthorityOracleRejectsLostContradictionsAndControlAssertions(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-transition-event-authority")
	for _, required := range []string{
		`"direct_execution_control"`, `"application_without_execution_event"`, `"contradictory_inbound_event"`,
		`"foreign_accepted_handler"`, "state.TriggerEventID = otherID", "state.TriggeredAt = at.Add(time.Second)",
		"after.Revision != before.Revision+1", "!reflect.DeepEqual(before, after)", "!slices.Equal(rows, afterRows[table])",
		"!strings.Contains(commitErr.Error(), want)", "mutation.ValidateTransitionEvidence()",
	} {
		changed := strings.ReplaceAll(row.After, required, "false")
		if nativeEventAuthorityWorkload(t, row.Before, true) == nativeEventAuthorityWorkload(t, changed, false) {
			t.Fatalf("weakened event authority admitted: %s", required)
		}
	}
}
