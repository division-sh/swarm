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

func nativeHandlerObserverSetupWorkload(t *testing.T, source string) string {
	t.Helper()
	old := "return fixture, pc, ctx, &nativeHandlerDispatchObservationForTest{Bus: pc.bus}"
	replacement := "planner, ok := pc.bus.(EngineMutationPublicationPlanner)\nif !ok {t.Fatal(\"native handler observation requires the original mutation publication planner\")}\nobserver := &nativeHandlerDispatchObservationForTest{Bus: pc.bus, EngineMutationPublicationPlanner: planner}\npc.bus = observer\nreturn fixture, pc, ctx, observer"
	return formattedNativeReadNode(projectionShapeFunction(t, strings.Replace(source, old, replacement, 1)))
}

func TestNativeHandlerObserverSetupPreservesOriginalOwnerAndWholeExecutionWindow(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-handler-observation-window")
	if nativeHandlerObserverSetupWorkload(t, row.Before) != nativeHandlerObserverSetupWorkload(t, row.After) {
		t.Fatal("source, native setup or continuous observation/owner changed")
	}
	row = nativeMissingHeaderRecipe(t, "native-handler-continuous-handoff-observation")
	want := `func dispatchNativeHandlerFollowUpForTest(pc *PipelineCoordinator,ctx context.Context,observer *nativeHandlerDispatchObservationForTest,followUp handlerCommittedFollowUp)error{if pc.bus!=observer{return fmt.Errorf("handler dispatch observation must cover the entire execution window")};return pc.transferCommittedHandlerFollowUp(ctx,followUp,nil)}`
	if formattedNativeReadNode(projectionShapeFunction(t, row.After)) != formattedNativeReadNode(projectionShapeFunction(t, want)) {
		t.Fatal("handoff-only observation or different dispatcher returned")
	}
	path := filepath.Join("..", "..", "..", "internal", "runtime", "pipeline", "workflow_handler_native_fixture_test.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	roles := map[string]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		decl, ok := node.(*ast.TypeSpec)
		if !ok || decl.Name.Name != "nativeHandlerDispatchObservationForTest" {
			return true
		}
		for _, field := range decl.Type.(*ast.StructType).Fields.List {
			if len(field.Names) == 0 {
				roles[formattedNativeReadNode(field.Type)] = true
			}
		}
		return false
	})
	if len(roles) != 2 || !roles["Bus"] || !roles["EngineMutationPublicationPlanner"] {
		t.Fatal("observer hid original required planner or dispatcher capabilities")
	}
}

func TestNativeHandlerObserverSetupOracleRejectsLateWindowAndDifferentOwner(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-handler-observation-window")
	for _, change := range [][2]string{
		{"\tpc.bus = observer\n", ""},
		{"Bus: pc.bus, EngineMutationPublicationPlanner: planner", "Bus: pc.bus"},
		{"Bus: pc.bus, EngineMutationPublicationPlanner: planner", "Bus: anotherBus, EngineMutationPublicationPlanner: planner"},
		{"Fields: cloneMap(fields)", "Fields: map[string]any{}"},
	} {
		changed := strings.Replace(row.After, change[0], change[1], 1)
		if changed == row.After || nativeHandlerObserverSetupWorkload(t, row.Before) == nativeHandlerObserverSetupWorkload(t, changed) {
			t.Fatalf("late observation or changed owner/workload admitted: %s", change[0])
		}
	}
}
