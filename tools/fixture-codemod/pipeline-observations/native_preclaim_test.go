package main

import (
	"go/ast"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
	helper := projectionShapeFunction(t, row.After)
	if formattedNativeReadNode(helper.Body.List[1].(*ast.ReturnStmt).Results[0]) != formattedNativeReadNode(load) {
		t.Fatal("authored source bytes changed during producer extraction")
	}
	root := canonicalrouting.CopyPipelineDeliveryAuthority(t)
	files := load.(*ast.CallExpr).Args[1].(*ast.CompositeLit)
	for _, element := range files.Elts {
		pair := element.(*ast.KeyValueExpr)
		name, err := strconv.Unquote(pair.Key.(*ast.BasicLit).Value)
		if err != nil {
			t.Fatal(err)
		}
		want, err := strconv.Unquote(pair.Value.(*ast.BasicLit).Value)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || string(actual) != want {
			t.Fatalf("closed source %s differs from immutable authored bytes: %v", name, err)
		}
	}
	actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, "deliveryAuthoritySourceForTest"))
	want, afterErr := canonicalFunction(row.Successor)
	if err != nil || afterErr != nil || actual != want {
		t.Fatalf("native source producer differs from its finite snapshot: %v/%v", err, afterErr)
	}
	if strings.Contains(row.Successor, "newPipelineTestDeliveryOwner") || strings.Contains(row.Successor, "newPostgresPipelineCoordinatorForTest") || !strings.Contains(row.Successor, "canonicalrouting.CopyPipelineDeliveryAuthority(t)") {
		t.Fatal("source producer restored raw-store construction")
	}
}
