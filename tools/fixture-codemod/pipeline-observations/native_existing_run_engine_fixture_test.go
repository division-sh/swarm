package main

import (
	"encoding/json"
	"go/ast"
	"reflect"
	"strings"
	"testing"
)

func TestNativeRetainedEngineFixturePreservesFactsAndExplicitOriginCorrection(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-existing-run-engine-fixture" {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, "VerifyRetainedNodeContractHandlerUsesRuntimeEnginePathForTest")
		want, err := canonicalFunction(row.After)
		got, actualErr := canonicalFunction(actual)
		if err != nil || actualErr != nil || want != got {
			t.Fatal("retained engine fixture changed outside reviewed migration")
		}
		arguments := func(source, constructor string) []string {
			var args []string
			ast.Inspect(projectionShapeFunction(t, source), func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if ok && formattedNativeReadNode(call.Fun) == constructor {
					if args != nil {
						t.Fatal("ambiguous event constructor")
					}
					for _, arg := range call.Args {
						args = append(args, formattedNativeReadNode(arg))
					}
				}
				return true
			})
			return args
		}
		before := arguments(row.Before, "eventtest.RunCreatingRootIngressWithRoutingSource")
		after := arguments(row.After, "eventtest.ExistingRunRootIngressWithRoutingSource")
		if len(before) != 11 || len(after) != 10 || before[7] != `""` || !reflect.DeepEqual(append(before[:7:7], before[8:]...), after) {
			t.Fatal("explicit create-to-existing correction changed event ID/type/source/payload/envelope/mode/clock")
		}
		beforeTail := row.Before[strings.Index(row.Before, "\toutcome, err :="):]
		afterTail := row.After[strings.Index(row.After, "\toutcome, err :="):]
		if beforeTail != afterTail {
			t.Fatal("original handler/handoff/handled/one-emission assertions changed")
		}
		for _, cut := range []string{"nativeHandlerEngineExistingEntityForTest(t, open, testPipelineRunID)", "fixture.Publish(ctx, evt, route)", "claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, evt, route)", "if err := stop(); err != nil", "mustCurrentWorkflowState(t, pc, ctx, flowidentity.StoredRoute(\".\", testPipelineRunID, testPipelineRunID)", "if !outcome.Handled", "got != 1", "got != \"custom.emitted\""} {
			if !strings.Contains(actual, cut) {
				t.Fatalf("engine fixture lost exact live path: %s", cut)
			}
			mutant := strings.Replace(row.After, cut, "unreviewedEngineCut", 1)
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("engine mutation accepted: %s", cut)
			}
		}
		for _, raw := range []string{"newConstructorHandlerUnitCoordinator", "prepareConstructorUnitDelivery", "recordingPipelineBus", "RunCreatingRootIngressWithRoutingSource"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("obsolete scenario engine fixture survives: %s", raw)
			}
		}
	}
	proof := selectedCausalObservationBody(t, "internal/runtime/pipeline/workflow_handler_native_external_test.go", "TestRetainedNodeContractHandlerUsesRuntimeEnginePath")
	for _, cut := range []string{"workflowHandlerNativeFixture(t, backend, source, 1, 1)", "RunCreatingRootIngressWithRoutingSource", "events.AdmittedRunCreateAuthorized", "storetest.EnsureRunForAdmittedEvent(ctx, fixture.Runs, admitted, wrong.CreatedAt())", "run lifecycle origin conflict", "!reflect.DeepEqual(after, before)", "afterCounts != counts", "missing committed handler emission", "opened.Runs.LoadRunOrigin(opened.Context, runID)", "runtimerunlifecycle.ScenarioSetupRunOrigin()"} {
		if !strings.Contains(proof, cut) {
			t.Fatalf("native origin/negative proof lost %s", cut)
		}
	}
	if matched != 1 {
		t.Fatalf("retained engine recipes=%d,want1", matched)
	}
}
