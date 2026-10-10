package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"strings"
	"testing"
)

func nativePositiveChildUsesExactSource(fn *ast.FuncDecl, source string) bool {
	matched, invalid := 0, false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		switch name.Name {
		case "materializedWorkflowInstanceForTest":
			invalid = true
		case "materializedWorkflowInstanceForSource":
			if len(call.Args) != 4 || formattedNativeReadNode(call.Args[0]) != "t" || formattedNativeReadNode(call.Args[1]) != source || formattedNativeReadNode(call.Args[2]) != "ctx" {
				invalid = true
			} else {
				matched++
			}
		}
		return true
	})
	return matched == 1 && !invalid
}

func TestNativePipelinePositiveChildFixturesRequireExactSourcePreparation(t *testing.T) {
	for _, row := range []struct{ file, name, source string }{
		{"workflow_join_lifecycle_test.go", "VerifyNativeWorkflowJoinUsesSelectedStoreScheduleOwnerOnBothStoresForTest", "pc.SemanticSource()"},
		{"workflow_join_lifecycle_test.go", "VerifyNativeWorkflowJoinSchedulePreservesMockExecutionModeOnBothStoresForTest", "pc.SemanticSource()"},
		{"workflow_join_lifecycle_test.go", "VerifyNativeWorkflowJoinCountWaitsDespiteEmptyStateMembersOnBothStoresForTest", "pc.SemanticSource()"},
		{"workflow_join_lifecycle_test.go", "VerifyNativeWorkflowJoinDurableIdentityIncludesStageOnBothStoresForTest", "pc.SemanticSource()"},
		{"workflow_join_lifecycle_test.go", "VerifyNativeWorkflowJoinFailurePersistsCanonicalDeliveryOutcomeAndRuntimeLogForTest", "pc.SemanticSource()"},
		{"constructor_handler_native_fixture_test.go", "constructNativeQueryEntitiesGuardInstanceForTest", "pc.SemanticSource()"},
		{"activity_boring_native_fixture_test.go", "seedNativeActivityBoringSourceFlowForTest", "fixture.pc.SemanticSource()"},
	} {
		t.Run(row.name, func(t *testing.T) {
			body := selectedCausalObservationBody(t, "internal/runtime/pipeline/"+row.file, row.name)
			if !nativePositiveChildUsesExactSource(projectionShapeFunction(t, body), row.source) {
				t.Fatal("positive native child does not consume its exact source/run constructor")
			}
			for _, mutant := range []string{
				strings.Replace(body, "materializedWorkflowInstanceForSource(t, "+row.source+", ctx,", "materializedWorkflowInstanceForTest(", 1),
				strings.Replace(body, row.source+", ctx,", "foreignSource, ctx,", 1),
				strings.Replace(body, row.source+", ctx,", row.source+", foreignContext,", 1),
			} {
				if nativePositiveChildUsesExactSource(projectionShapeFunction(t, mutant), row.source) {
					t.Fatal("lost or crossed child-construction authority accepted")
				}
			}
		})
	}
}

func TestNativePipelineColdJoinConstructorAndFanOutRecipesStaySourcePinned(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-pipeline-delivery-cold-join-constructor-semantic" && row.Family != "native-pipeline-delivery-fanout-semantic" {
			continue
		}
		count++
		source := []byte("package probe\n" + row.Before)
		after, changed, err := rewriteFunction(row.File, source, row)
		if err != nil || !changed {
			t.Fatalf("finite native repair failed: %s/%v", row.Function, err)
		}
		if _, changed, err := rewriteFunction(row.File, after, row); err != nil || changed {
			t.Fatalf("native repair is not idempotent: %s/%v", row.Function, err)
		}
		if _, _, err := rewriteFunction(row.File, bytes.Replace(source, []byte("{"), []byte("{ unreviewedEscape();"), 1), row); err == nil {
			t.Fatalf("changed native source admitted: %s", row.Function)
		}
		actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, nativePipelineRecipeReplacementName(t, row)))
		want, afterErr := canonicalFunction(row.After)
		if err != nil || afterErr != nil || actual != want {
			t.Fatalf("native repair differs from its reviewed snapshot: %s/%v/%v", row.Function, err, afterErr)
		}
	}
	if count != 25 {
		t.Fatalf("native join/constructor/fan-out recipes=%d, want 25", count)
	}
}
