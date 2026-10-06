package bootverify

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestStateMachineCoherenceIgnoresMutableInitialProjection(t *testing.T) {
	graph := runtimecontracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{
			Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{{ID: "ready"}, {ID: "Ready", Final: true}},
		}},
		Semantics: runtimecontracts.WorkflowSemanticView{
			StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{".": graph},
		},
	}
	source := semanticview.Wrap(bundle)
	if findings := (&checkerContext{source: source}).stateMachineCoherence(); len(findings) != 0 {
		t.Fatalf("unmodified selected stage source failed verification: %#v", findings)
	}
	projection := bundle.Semantics.StageTopologies["."]
	projection.InitialStage = "Ready"
	bundle.Semantics.StageTopologies["."] = projection
	if got := compiledInitialStageForFlow(source, "."); got != "ready" || !bootverifyFlowStateful(source, ".") {
		t.Fatalf("raw initial projection replaced compiled initial: %q stateful=%v", got, bootverifyFlowStateful(source, "."))
	}
	if findings := (&checkerContext{source: source}).stateMachineCoherence(); len(findings) != 0 {
		t.Fatalf("non-authoritative projection changed verification: %#v", findings)
	}
	bundle.RootSchema.StageDeclarations.Entries = nil
	if got := compiledInitialStageForFlow(source, "."); got != "ready" {
		t.Fatalf("cleared raw declaration changed compiled initial: %q", got)
	}
	if flowIsStateless(source, ".") {
		t.Fatal("raw absent stages overrode the compiled stateful topology")
	}
}
