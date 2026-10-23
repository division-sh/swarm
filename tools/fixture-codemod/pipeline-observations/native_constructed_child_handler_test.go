package main

import (
	"strings"
	"testing"
)

func constructedChildHandlerWorkload(source string) string {
	start := strings.Index(source, "\tsource := loadWorkflowTempSource")
	setup := strings.Index(source, "\tpc, bus, ctx :=")
	if setup < 0 {
		setup = strings.Index(source, "\tmodule := canonicalPreviewWorkflowModuleForTest")
	}
	tail := strings.Index(source, "\tresult, err :=")
	end := strings.Index(source, "\tstored, found, err :=")
	if end < 0 {
		end = strings.LastIndex(source, "}")
	}
	if start < 0 || setup < start || tail < setup || end < tail {
		return "invalid child handler workload"
	}
	work := source[start:setup] + source[tail:end]
	return strings.Replace(work, "pipelineOnlySourceNode(t, pc.SemanticSource(), \"node-a\")", "node", 1)
}

func TestNativeConstructedChildHandlerRejectsChangedContractAndAssertions(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-constructed-child-handler")
	for _, cut := range []string{"scoring/schema.yaml", "name: text", "Updated Entity", "has(entity.name)", "!result.Handled", "got != 1", "FlowInstanceEntityID(\"scoring\")", `payload["label"]`} {
		mutant := strings.Replace(row.After, cut, "unreviewed", 1)
		if mutant == row.After || constructedChildHandlerWorkload(row.Before) == constructedChildHandlerWorkload(mutant) {
			t.Fatalf("changed child contract/assertion was admitted: %s", cut)
		}
	}
}

func TestConstructedScenarioValuesRetainOriginalConstructorFacts(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-constructed-scenario-values")
	before := strings.Replace(row.Before, "\tsource := pc.SemanticSource()\n", "", 1)
	before = strings.Replace(before, "\tinstance := WorkflowInstance{", "\treturn WorkflowInstance{", 1)
	end := strings.Index(before, "\tif err := pc.workflowStore.create")
	if end < 0 {
		t.Fatal("missing original unit persistence cut")
	}
	before = before[:end] + "}"
	before = strings.Replace(before, "seedConstructorUnitInstance(t *testing.T, pc *PipelineCoordinator, ctx context.Context, flowID string)", "constructedScenarioInstanceForTest(t *testing.T, source semanticview.Source, ctx context.Context, flowID string)", 1)
	before = strings.Replace(before, "unit constructor requires", "scenario constructor requires", 1)
	actual := selectedCausalObservationBody(t, row.File, "constructedScenarioInstanceForTest")
	want, err := canonicalFunction(before)
	got, actualErr := canonicalFunction(actual)
	if err != nil || actualErr != nil || want != got {
		t.Fatal("pure scenario values changed compiled fields/stage, parent or entity identity")
	}
	if !row.Removed {
		t.Fatal("raw constructor seed was not retired")
	}
	if files, changes, err := prepareFiles("../../..", []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
		t.Fatalf("raw constructor seed survived retirement: %v", err)
	}
}
