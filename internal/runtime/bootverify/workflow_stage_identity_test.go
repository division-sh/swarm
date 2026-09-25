package bootverify

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestStateMachineCoherenceRejectsStaleInitialProjection(t *testing.T) {
	graph := runtimecontracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{
			Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{{ID: "ready", Initial: true}, {ID: "Ready", Terminal: true}},
		}},
		Semantics: runtimecontracts.WorkflowSemanticView{
			InitialStage: "ready", StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{".": graph},
		},
	}
	source := semanticview.Wrap(bundle)
	if findings := (&checkerContext{source: source}).stateMachineCoherence(); len(findings) != 0 {
		t.Fatalf("unmodified selected stage source failed verification: %#v", findings)
	}
	bundle.Semantics.InitialStage = "Ready"
	if got := compiledInitialStageForFlow(source, "."); got != "ready" || !bootverifyFlowStateful(source, ".") {
		t.Fatalf("raw initial projection replaced compiled initial: %q stateful=%v", got, bootverifyFlowStateful(source, "."))
	}
	if findings := (&checkerContext{source: source}).stateMachineCoherence(); !reportContains(findings, "state_machine_coherence", "disagrees with selected compiled initial stage") {
		t.Fatalf("stale authored initial was accepted: %#v", findings)
	}
	bundle.Semantics.InitialStage = ""
	bundle.RootSchema.StageDeclarations.Entries = nil
	if got := compiledInitialStageForFlow(source, "."); got != "ready" {
		t.Fatalf("cleared raw declaration changed compiled initial: %q", got)
	}
	if flowIsStateless(source, ".") {
		t.Fatal("raw absent stages overrode the compiled stateful topology")
	}
}
