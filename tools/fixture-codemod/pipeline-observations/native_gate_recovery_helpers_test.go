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
