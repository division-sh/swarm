package main

import (
	"go/ast"
	"strings"
	"testing"
)

func nativeUnstampedDeliveryWorkload(t *testing.T, source string, predecessor bool) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	var statements []ast.Stmt
	started := false
	for _, statement := range body.List {
		text := formattedNativeReadNode(statement)
		started = started || strings.HasPrefix(text, "postCommit := ") || strings.HasPrefix(text, "ictx := ")
		if !started {
			continue
		}
		if strings.HasPrefix(text, "postCommit := ") {
			continue
		}
		if predecessor && strings.HasPrefix(text, "ictx := WithPipelinePostCommitActions(") {
			statement.(*ast.AssignStmt).Rhs[0] = mutationSeedExpression(t, "runCtx")
		}
		if strings.HasPrefix(text, "node := ") || strings.HasPrefix(text, "assertDeliveryAuthority") || strings.HasPrefix(text, "after := ") || strings.Contains(text, "delivery authority node outcomes/deliveries") {
			continue
		}
		if predecessor && strings.HasPrefix(text, "if got := bus.publishedCount();") {
			statement.(*ast.IfStmt).Init.(*ast.AssignStmt).Rhs[0] = mutationSeedExpression(t, `after["events"] - before["events"]`)
		}
		statements = append(statements, statement)
	}
	if len(statements) != 5 {
		t.Fatalf("unstamped original execution/assertions has %d components, want 5", len(statements))
	}
	return formattedNativeReadNode(&ast.BlockStmt{List: statements})
}

func TestNativeUnstampedDeliveryRecipePreservesActualInterceptionAndNoEmission(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-unstamped-node-delivery")
	if nativeUnstampedDeliveryWorkload(t, row.Before, true) != nativeUnstampedDeliveryWorkload(t, row.After, false) {
		t.Fatal("unstamped event-wide interception, exact context, passthrough or no-emission assertion changed")
	}
	for _, required := range []string{
		"fixture.Construct(runCtx, materializedWorkflowInstanceForTest(", "fixture.PublishDirect(runCtx, evt)",
		"fixture.ApplicationStorage(runCtx)", `[]string{"events", "event_deliveries", "event_delivery_attempts"}`,
		`before["events"] != 1`, `before["event_deliveries"] != 0`, `before["event_delivery_attempts"] != 0`,
		`after["event_delivery_attempts"] != 0 || after["event_deliveries"] != 0`,
		`eventtest.ExistingRunRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`,
	} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("unstamped proof lost native event/prestate/conservation: %s", required)
		}
	}
}

func TestNativeUnstampedDeliveryOracleRejectsLostPassthroughErrorsAndEmissions(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-unstamped-node-delivery")
	for _, required := range []string{"if err != nil {", "if !passthrough {", "got != 0", "pc.Intercept(ictx, evt)"} {
		changed := strings.ReplaceAll(row.After, required, "false")
		if strings.HasPrefix(required, "if ") {
			changed = strings.ReplaceAll(row.After, required, "if false {")
		}
		if nativeUnstampedDeliveryWorkload(t, row.Before, true) == nativeUnstampedDeliveryWorkload(t, changed, false) {
			t.Fatalf("weakened unstamped node contract admitted: %s", required)
		}
	}
}
