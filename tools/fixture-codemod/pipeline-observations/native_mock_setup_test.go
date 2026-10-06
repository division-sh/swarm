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

var nativeMockSetupRoots = map[string]bool{
	"TestPipelineActivityRequestMockFlowLocalProviderConnectorUsesGeneratedResponseAndJournal": true,
	"TestPipelineActivityRequestMockTerminalReplayDoesNotRequireCurrentResponsePlan":           true,
	"TestPipelineActivityRequestMockAdmissionFailsBeforeJournalCredentialsAndHTTP":             true,
	"TestMockOnlyPostureRejectsLiveActivityBeforeJournalCredentialsAndHTTP":                    true,
}

func TestNativeMockRecipesPreserveAdmissionWorkloadsAndRefusalAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Family != "native-mock-activity" {
			continue
		}
		if row.File != "internal/runtime/pipeline/activity_engine_test.go" || !nativeMockSetupRoots[row.Function] || seen[row.Function] {
			t.Fatalf("unknown or duplicated mock consumer: %s/%s", row.File, row.Function)
		}
		seen[row.Function] = true
		before, after := mockWorkloadAndAssertions(t, row.Before), mockWorkloadAndAssertions(t, row.After)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("mock workload or refusal assertion changed: %s\nbefore=%v\nafter=%v", row.Function, before, after)
		}
		if !strings.Contains(row.After, "fixture.RequireRun(ctx, runID)") || strings.Contains(row.After, "newDurablePipelineCoordinatorForTest") {
			t.Fatalf("mock consumer retained reconstructed setup: %s", row.Function)
		}
	}
	if len(seen) != len(nativeMockSetupRoots) {
		t.Fatalf("mock cohort=%d, want four", len(seen))
	}
}

func mockWorkloadAndAssertions(t *testing.T, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "mock.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	facts := journalMutationAndAssertionCalls(t, source)
	ast.Inspect(file, func(node ast.Node) bool {
		if assignment, ok := node.(*ast.AssignStmt); ok && mockIntentAssignment(assignment) {
			facts = append(facts, formattedNativeReadNode(assignment))
		}
		if loop, ok := node.(*ast.RangeStmt); ok {
			if _, literal := loop.X.(*ast.CompositeLit); literal && formattedNativeReadNode(loop.Value) == "tc" {
				facts = append(facts, formattedNativeReadNode(loop.X))
			}
		}
		if loop, ok := node.(*ast.ForStmt); ok {
			facts = append(facts, formattedNativeReadNode(loop.Init), formattedNativeReadNode(loop.Cond), formattedNativeReadNode(loop.Post))
		}
		if call, ok := node.(*ast.CallExpr); ok && mockProofWorkloadCall(call) {
			facts = append(facts, formattedNativeReadNode(call))
		}
		return true
	})
	for index, fact := range facts {
		facts[index] = strings.NewReplacer("observed.Attempts", "attempts", "observed.MockAttemptStories", "storyModes").Replace(fact)
	}
	return facts
}

func mockIntentAssignment(assignment *ast.AssignStmt) bool {
	for _, left := range assignment.Lhs {
		selector, ok := left.(*ast.SelectorExpr)
		if ok && formattedNativeReadNode(selector.X) == "intent" {
			return true
		}
	}
	return false
}

func mockProofWorkloadCall(call *ast.CallExpr) bool {
	name := formattedNativeReadNode(call.Fun)
	if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
		name = selector.Sel.Name
	}
	switch name {
	case "NewServer", "testTelegramConnectorTool", "WithSchemas", "WithStaticCredentials",
		"CompileMockResponsePlan", "NewMockResponsePlan", "testNonIdempotentActivityIntent",
		"mustActivityInput", "executeActivityIntent":
		return true
	}
	return false
}

func TestNativeMockControlsRejectWeakenedRefusalAndChangedExecutionMode(t *testing.T) {
	before := `func proof(){ intent.ExecutionMode=executionmode.Mock; if attempts != 0 || credentials.reads.Load()!=0 || httpCalls.Load()!=0 { t.Fatal("side effect") } }`
	for _, after := range []string{
		strings.Replace(before, "attempts != 0 || credentials.reads.Load()!=0 || httpCalls.Load()!=0", "false", 1),
		strings.Replace(before, "executionmode.Mock", "executionmode.Live", 1),
	} {
		if reflect.DeepEqual(mockWorkloadAndAssertions(t, before), mockWorkloadAndAssertions(t, after)) {
			t.Fatal("mock proof allowed a changed authority or weakened refusal")
		}
	}
}
