package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

var nativeLoopSetupRoots = map[string]string{
	"TestLoopActivityClaimOrdersAgainstRepeatAndCloseOnBothStores":           "activity_journal_test.go",
	"seedLoopActivityInstance":                                               "activity_journal_test.go",
	"TestLoopActivityClaimCommitAcknowledgmentLossReconcilesWithoutDispatch": "activity_engine_test.go",
}

func TestNativeLoopRecipesPreserveGenerationOrderingAndNoDispatch(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Family != "native-loop-claim" {
			continue
		}
		if nativeLoopSetupRoots[row.Function] == "" || row.File != "internal/runtime/pipeline/"+nativeLoopSetupRoots[row.Function] || seen[row.Function] {
			t.Fatalf("unknown or duplicated loop consumer: %s/%s", row.File, row.Function)
		}
		seen[row.Function] = true
		if row.Function == "seedLoopActivityInstance" {
			before, after := loopConstructionFacts(t, row.Before), loopConstructionFacts(t, row.After)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("loop activation/carrier construction changed: before=%v after=%v", before, after)
			}
			continue
		}
		before, after := loopWorkloadAndAssertions(t, row.Before), loopWorkloadAndAssertions(t, row.After)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("loop ordering or no-dispatch proof changed: %s\nbefore=%v\nafter=%v", row.Function, before, after)
		}
		if !strings.Contains(row.After, "fixture.RequireRun(ctx, runID)") {
			t.Fatal("loop consumer lacks canonical run construction")
		}
	}
	if len(seen) != 3 {
		t.Fatalf("loop cohort=%d, want three exact functions", len(seen))
	}
}

func loopWorkloadAndAssertions(t *testing.T, source string) []string {
	t.Helper()
	facts := mockWorkloadAndAssertions(t, source)
	var retained []string
	for index := 0; index < len(facts); index++ {
		if index+1 < len(facts) && (strings.Contains(facts[index+1], "commit acknowledgment loss was not reached") || strings.Contains(facts[index+1], "scan journal:")) {
			index++
			continue
		}
		retained = append(retained, facts[index])
	}
	file, err := parser.ParseFile(token.NewFileSet(), "loop.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if loop, ok := node.(*ast.RangeStmt); ok && loop.Value != nil && formattedNativeReadNode(loop.Value) == "operation" {
			retained = append(retained, formattedNativeReadNode(loop.X))
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := formattedNativeReadNode(call.Fun)
		if selected, ok := call.Fun.(*ast.SelectorExpr); ok {
			name = selected.Sel.Name
		}
		switch name {
		case "ClaimActivityAttemptForLoopGeneration", "advanceLoopActivityInstance", "loopActivityStartRecord":
			retained = append(retained, formattedNativeReadNode(call))
		case "seedLoopActivityInstance", "seedNativeLoopActivityInstance":
			if len(call.Args) != 4 {
				t.Fatal("changed loop seed arguments")
			}
			retained = append(retained, "seedLoopActivityInstance("+formattedNativeReadNode(call.Args[0])+", native-owner, "+formattedNativeReadNode(call.Args[2])+", "+formattedNativeReadNode(call.Args[3])+")")
		}
		return true
	})
	return retained
}

func loopConstructionFacts(t *testing.T, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "loop.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var facts []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch formattedNativeReadNode(call.Fun) {
		case "loopruntime.New", "loopruntime.Store", "runtimeengine.NewStateCarrier", "materializedWorkflowInstanceForTest":
			facts = append(facts, formattedNativeReadNode(call))
		}
		return true
	})
	return facts
}

func TestNativeLoopControlsRejectChangedClaimGenerationAndConstructedCarrier(t *testing.T) {
	before := `func proof(){ intent.Generation=activation.Generation(); if !record.Generation.Equal(intent.Generation) { t.Fatal("claim changed") } }`
	after := strings.Replace(before, "!record.Generation.Equal(intent.Generation)", "false", 1)
	if reflect.DeepEqual(loopWorkloadAndAssertions(t, before), loopWorkloadAndAssertions(t, after)) {
		t.Fatal("loop proof weakened its generation fence")
	}
	before = `func seed(){ loopruntime.New(run,entity,"validation","revision","revision_id",id,"review",3,at) }`
	after = strings.Replace(before, `"review"`, `"other"`, 1)
	if reflect.DeepEqual(loopConstructionFacts(t, before), loopConstructionFacts(t, after)) {
		t.Fatal("loop proof changed its admitted activation carrier")
	}
}
