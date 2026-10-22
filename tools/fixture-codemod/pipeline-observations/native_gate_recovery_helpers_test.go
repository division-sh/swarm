package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"
)

func gateRecoveryNativeHelper(name string) bool {
	switch name {
	case "insertGateRecoveryRun", "gateRecoveryPipelineReceiptCount", "assertGateRecoveryProcessedReceipt",
		"assertGateRecoveryErrorReceipt", "assertGateRecoveryObligationStatus", "assertProposedEffectProofCounts", "loadProposedEffectProofRequest":
		return true
	}
	return false
}

func gateRecoveryCallerHandoff(t *testing.T, source string) string {
	t.Helper()
	function := projectionShapeFunction(t, source)
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if ok && gateRecoveryNativeHelper(name.Name) {
			call.Args[1] = &ast.SelectorExpr{X: call.Args[1], Sel: ast.NewIdent("events")}
		}
		return true
	})
	return formattedNativeReadNode(function)
}

func TestGateRecoveryHelpersRetainOriginalLifecycleAndReceiptAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-gate-recovery-owner-helpers" && !(row.Family == "activity" && row.Function == "assertProposedEffectProofCounts") {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		want, err := canonicalFunction(row.After)
		got, actualErr := canonicalFunction(actual)
		if err != nil || actualErr != nil || want != got {
			t.Fatalf("native helper differs from reviewed output: %s", row.Function)
		}
		for _, raw := range []string{"gateRecoveryStoreCase", ".db", ".postgres", ".events", "QueryRow", "RequirePostgres", "RequireSQLite"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("native helper retains raw carrier or dialect interpreter: %s/%s", row.Function, raw)
			}
		}
		for _, preserved := range []string{
			`"load final pipeline receipt: %v"`, `"load quarantined pipeline receipt: %v"`,
			`outcome != "success" || reason != "decision_route_processed"`,
			`outcome != "dead_letter" || reason != wantReason`,
			`err != nil || got != want`, `gotRequests != requests || gotAttempts != attempts`,
			`"durable activity counts = requests:%d attempts:%d, want %d/%d"`, `return count`,
		} {
			if strings.Contains(row.Before, preserved) && !strings.Contains(actual, preserved) {
				t.Fatalf("original receipt/error/cardinality assertion changed: %s/%s", row.Function, preserved)
			}
		}
	}
	if matched != 7 {
		t.Fatalf("native lifecycle/receipt helpers=%d want7", matched)
	}
	setup := selectedCausalObservationBody(t, "internal/runtime/pipeline/workflow_gate_recovery_external_test.go", "insertGateRecoveryRun")
	if !strings.Contains(setup, "storetest.RequireRun(t, testAuthorActivityContext(t, context.Background()), tc.(storetest.RunFixtureStore), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID})") {
		t.Fatal("native setup lost exact original owner/context/scenario origin")
	}
}

func TestGateRecoveryCallerOracleRejectsChangedWorkloadAndWrongOwner(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var row recipe
	for _, candidate := range rows {
		if candidate.Family == "native-gate-recovery-owner-caller" && candidate.Function == "newA2CanonicalAccumulatorProof" {
			row = candidate
		}
	}
	if row.Function == "" {
		t.Fatal("missing exact accumulator setup handoff recipe")
	}
	want := gateRecoveryCallerHandoff(t, row.Before)
	for _, cut := range []string{"insertGateRecoveryRun", ".events", "t.Helper()"} {
		changed := strings.Replace(row.After, cut, "unreviewed", 1)
		if changed == row.After || formattedNativeReadNode(projectionShapeFunction(t, changed)) == want {
			t.Fatalf("changed owner/setup cut accepted: %s", cut)
		}
	}
}
