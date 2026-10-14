package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"strings"
	"testing"
)

func nativePositiveChildUsesExactSource(fn *ast.FuncDecl, source string, preparations int) bool {
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
	return matched == preparations && !invalid && (preparations != 2 || nativePositiveParentChildCoordinates(fn))
}

func nativePositiveParentChildCoordinates(fn *ast.FuncDecl) bool {
	root, child, invalid := 0, 0, false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || formattedNativeReadNode(call.Fun) != "materializedWorkflowInstanceForSource" || len(call.Args) != 4 {
			return true
		}
		instance, ok := call.Args[3].(*ast.CompositeLit)
		if !ok || formattedNativeReadNode(instance.Type) != "WorkflowInstance" {
			invalid = true
			return true
		}
		fields := make(map[string]string)
		for _, element := range instance.Elts {
			field, ok := element.(*ast.KeyValueExpr)
			if !ok {
				invalid = true
				continue
			}
			fields[formattedNativeReadNode(field.Key)] = formattedNativeReadNode(field.Value)
		}
		if fields["WorkflowName"] == `"."` {
			root++
			invalid = invalid || fields["InstanceID"] != "root" || fields["StorageRef"] != "root" || fields["EntityID"] != "root"
		} else {
			child++
			invalid = invalid || fields["WorkflowName"] != `"orders"` || fields["StorageRef"] != "path" || fields["InstanceID"] != `strings.TrimPrefix(path, "orders/")`
		}
		return true
	})
	return root == 1 && child == 1 && !invalid
}

func nativePositivePreparationMutant(fn *ast.FuncDecl, position int, field string) *ast.FuncDecl {
	index := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || formattedNativeReadNode(call.Fun) != "materializedWorkflowInstanceForSource" || len(call.Args) != 4 {
			return true
		}
		index++
		if index != position {
			return true
		}
		switch field {
		case "source":
			call.Args[1] = ast.NewIdent("foreignSource")
		case "context":
			call.Args[2] = ast.NewIdent("foreignContext")
		case "owner":
			call.Fun = ast.NewIdent("materializedWorkflowInstanceForTest")
		case "coordinate":
			instance := call.Args[3].(*ast.CompositeLit)
			for _, element := range instance.Elts {
				key := element.(*ast.KeyValueExpr)
				if formattedNativeReadNode(key.Key) == "StorageRef" {
					key.Value = ast.NewIdent("foreignCoordinate")
				}
			}
		}
		return false
	})
	return fn
}

func TestNativePipelinePositiveChildFixturesRequireExactSourcePreparation(t *testing.T) {
	for _, row := range []struct {
		file, name, source string
		preparations       int
	}{
		{"workflow_join_lifecycle_test.go", "VerifyNativeWorkflowJoinUsesSelectedStoreScheduleOwnerOnBothStoresForTest", "pc.SemanticSource()", 2},
		{"workflow_join_lifecycle_test.go", "VerifyNativeWorkflowJoinSchedulePreservesMockExecutionModeOnBothStoresForTest", "pc.SemanticSource()", 2},
		{"workflow_join_lifecycle_test.go", "VerifyNativeWorkflowJoinCountWaitsDespiteEmptyStateMembersOnBothStoresForTest", "pc.SemanticSource()", 2},
		{"workflow_join_lifecycle_test.go", "VerifyNativeWorkflowJoinDurableIdentityIncludesStageOnBothStoresForTest", "pc.SemanticSource()", 2},
		{"workflow_join_lifecycle_test.go", "VerifyNativeWorkflowJoinFailurePersistsCanonicalDeliveryOutcomeAndRuntimeLogForTest", "pc.SemanticSource()", 2},
		{"constructor_handler_native_fixture_test.go", "constructNativeQueryEntitiesGuardInstanceForTest", "pc.SemanticSource()", 1},
		{"activity_boring_native_fixture_test.go", "seedNativeActivityBoringSourceFlowForTest", "fixture.pc.SemanticSource()", 1},
	} {
		t.Run(row.name, func(t *testing.T) {
			body := selectedCausalObservationBody(t, "internal/runtime/pipeline/"+row.file, row.name)
			if !nativePositiveChildUsesExactSource(projectionShapeFunction(t, body), row.source, row.preparations) {
				t.Fatal("positive native child does not consume its exact source/run constructor")
			}
			for _, mutant := range []string{
				strings.ReplaceAll(body, "materializedWorkflowInstanceForSource(t, "+row.source+", ctx,", "materializedWorkflowInstanceForTest("),
				strings.ReplaceAll(body, row.source+", ctx,", "foreignSource, ctx,"),
				strings.ReplaceAll(body, row.source+", ctx,", row.source+", foreignContext,"),
			} {
				if nativePositiveChildUsesExactSource(projectionShapeFunction(t, mutant), row.source, row.preparations) {
					t.Fatal("lost or crossed child-construction authority accepted")
				}
			}
			for position := 1; position <= row.preparations; position++ {
				for _, field := range []string{"source", "context", "owner", "coordinate"} {
					if field == "coordinate" && row.preparations == 1 {
						continue
					}
					mutant := nativePositivePreparationMutant(projectionShapeFunction(t, body), position, field)
					if nativePositiveChildUsesExactSource(mutant, row.source, row.preparations) {
						t.Fatalf("preparation %d lost its own %s authority", position, field)
					}
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
		current := row.After
		if row.Successor != "" {
			current = row.Successor
		}
		want, afterErr := canonicalFunction(current)
		if err != nil || afterErr != nil || actual != want {
			t.Fatalf("native repair differs from its reviewed snapshot: %s/%v/%v", row.Function, err, afterErr)
		}
	}
	if count != 25 {
		t.Fatalf("native join/constructor/fan-out recipes=%d, want 25", count)
	}
}
