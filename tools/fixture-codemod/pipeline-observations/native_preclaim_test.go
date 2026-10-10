package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func nativePreclaimBody(t *testing.T, source string) *ast.BlockStmt {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	loop := body.List[1].(*ast.RangeStmt)
	call := loop.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
	return call.Args[1].(*ast.FuncLit).Body
}

func nativePreclaimWorkload(t *testing.T, source string) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	loop := body.List[1].(*ast.RangeStmt)
	var statements []ast.Stmt
	started := false
	for _, statement := range nativePreclaimBody(t, source).List {
		text := formattedNativeReadNode(statement)
		started = started || strings.HasPrefix(text, "delivery, err := ")
		if strings.HasPrefix(text, "assertDeliveryAuthorityOutcomeCount(") || strings.Contains(text, `t.Fatalf("delivery authority node outcomes = `) {
			break
		}
		if started && !strings.HasPrefix(text, "pending, err := ") && !strings.HasPrefix(text, "if err != nil || pending.EventID ") && !strings.HasPrefix(text, "readAttempts := ") && !strings.HasPrefix(text, "if attempts := ") {
			statements = append(statements, statement)
		}
	}
	if !started {
		t.Fatal("preclaim proof lost exact delivery and carrier setup")
	}
	return formattedNativeReadNode(body.List[0]) + formattedNativeReadNode(loop.X) + formattedNativeReadNode(&ast.BlockStmt{List: statements})
}

func nativePreclaimStatusAssertion(t *testing.T, source string) string {
	t.Helper()
	for _, statement := range nativePreclaimBody(t, source).List {
		conditional, ok := statement.(*ast.IfStmt)
		if ok && strings.Contains(formattedNativeReadNode(conditional.Body), `t.Fatalf("preclaim delivery status=`) {
			conditional.Init = nil
			return formattedNativeReadNode(conditional)
		}
	}
	t.Fatal("preclaim proof lost pending-status/read-error assertion")
	return ""
}

func TestNativePreclaimRecipePreservesCancellationCarrierAndUnsettledDisposition(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-preclaim-failure")
	if nativePreclaimWorkload(t, row.Before) != nativePreclaimWorkload(t, row.After) || nativePreclaimStatusAssertion(t, row.Before) != nativePreclaimStatusAssertion(t, row.After) {
		t.Fatal("three failure cases, exact errors, cancellation cut, direct carrier or pending readback changed")
	}
	for _, required := range []string{
		"fixture.Construct(runCtx, materializedWorkflowInstanceForTest(", "fixture.Publish(runCtx, evt, route)",
		"pending.EventID != evt.ID()", "pending.Route.Target != route.Target", "pending.Route.Recipient != route.Recipient", "pending.Status != runtimedelivery.StatusPending",
		"fixture.ApplicationStorage(runCtx)", `snapshot["event_delivery_attempts"]`, "if attempts := readAttempts(); len(attempts) != 0 {",
		"!reflect.DeepEqual(pending, after)", `eventtest.ExistingRunRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`,
	} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("native preclaim lost original selected evidence: %s", required)
		}
	}
}

func TestNativePreclaimOracleRejectsLostJoinedErrorCancellationReturnAndNoSettlement(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-preclaim-failure")
	for _, required := range []string{`name: "joined_failure"`, "store.cancel = cancel", "!store.called", "errors.Is(err, context.Canceled) != tc.wantCanceled", "errors.Is(err, independent) != tc.wantStore", "settled || !outcome.ContinueDispatch()", "continuation.returns.Load() != 1", "continuation.consumes.Load() != 0"} {
		changed := strings.ReplaceAll(row.After, required, "false")
		if nativePreclaimWorkload(t, row.Before) == nativePreclaimWorkload(t, changed) {
			t.Fatalf("weakened exact preclaim contract admitted: %s", required)
		}
	}
}

func TestNativePreclaimSourceProducerRetainsOriginalAuthoredBytesAfterConstructorRetirement(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-preclaim-source-producer")
	before := projectionShapeFunction(t, row.Before).Body
	load := before.List[2].(*ast.AssignStmt).Rhs[0]
	path := filepath.Join("..", "..", "..", row.File)
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	helper, err := uniqueFunction(file, "deliveryAuthoritySourceForTest")
	if err != nil || len(helper.Body.List) != 2 {
		t.Fatalf("finite source helper: err=%v", err)
	}
	if formattedNativeReadNode(helper.Body.List[1].(*ast.ReturnStmt).Results[0]) != formattedNativeReadNode(load) {
		t.Fatal("authored source bytes changed during producer extraction")
	}
	actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, "deliveryAuthoritySourceForTest"))
	want, afterErr := canonicalFunction(row.After)
	if err != nil || afterErr != nil || actual != want {
		t.Fatalf("native source producer differs from its finite snapshot: %v/%v", err, afterErr)
	}
	if strings.Contains(row.After, "newPipelineTestDeliveryOwner") || strings.Contains(row.After, "newPostgresPipelineCoordinatorForTest") {
		t.Fatal("source producer restored raw-store construction")
	}
}
