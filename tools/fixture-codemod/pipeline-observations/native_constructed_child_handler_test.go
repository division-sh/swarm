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

func TestNativeConstructedChildHandlerRetainsAuthoredIdentityAndWrites(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-constructed-child-handler")
	if constructedChildHandlerWorkload(row.Before) != constructedChildHandlerWorkload(row.After) {
		t.Fatal("original authored child, write expression, emit expression or assertion changed")
	}
	actual := selectedCausalObservationBody(t, row.File, "VerifyExecuteNodeContractHandlerUsesConstructedEntityIdentityForWritesAndEmitForTest")
	want, err := canonicalFunction(row.After)
	got, actualErr := canonicalFunction(actual)
	if err != nil || actualErr != nil || want != got {
		t.Fatal("constructed child handler differs from reviewed native migration")
	}
	for _, cut := range []string{
		"fixture.RequireRun(ctx, testPipelineRunID)",
		"constructedScenarioInstanceForTest(t, module.SemanticSource(), ctx, \"scoring\")",
		"fixture.Construct(ctx, instance)",
		"pc.bus = bus",
		"eventtest.ExistingRunRootIngressWithRoutingSource",
		"FlowID: \"scoring\", FlowInstance: instance.StorageRef, EntityID: instance.EntityID",
		"fixture.Publish(ctx, event, route)",
		"claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, event, route)",
		"if err := stop(); err != nil",
		"fixture.Persistence.LoadWorkflowInstance",
		"!reflect.DeepEqual(persisted, bus.publishedEvent(0))",
	} {
		if !strings.Contains(actual, cut) {
			t.Fatalf("native child handler lost exact owner/cut: %s", cut)
		}
	}
	if strings.Index(actual, "pc.bus = bus") > strings.Index(actual, "fixture.Publish(ctx, event, route)") {
		t.Fatal("child dispatch observer attaches after the execution window")
	}
	for _, obsolete := range []string{"newConstructorHandlerUnitCoordinator", "prepareConstructorUnitDelivery", "handlerTestRootIngress", "recordingPipelineBus"} {
		if strings.Contains(actual, obsolete) {
			t.Fatalf("native child handler retains fake persistence/authority: %s", obsolete)
		}
	}
	proof := selectedCausalObservationBody(t, "internal/runtime/pipeline/workflow_handler_native_external_test.go", "TestExecuteNodeContractHandlerUsesConstructedEntityIdentityForWritesAndEmit")
	if !strings.Contains(proof, `[]string{"sqlite", "postgres"}`) || !strings.Contains(proof, "workflowHandlerNativeFixture(t, backend, source, 1, 1)") {
		t.Fatal("child handler lost both-store original-coordinator mutation/claim proof")
	}
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
