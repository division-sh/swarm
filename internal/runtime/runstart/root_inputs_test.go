package runstart

import (
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/flowmodel"
	"swarm/internal/runtime/semanticview"
)

func TestDeriveRootInputSetRequiresDeclaredAndRoutableRootInput(t *testing.T) {
	bundle := rootInputTestBundle("scan.corpus_file_requested")
	set, err := DeriveRootInputSet(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRootInputSet: %v", err)
	}
	if got, want := set.Declared, []string{"scan.corpus_file_requested"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("declared = %#v, want %#v", got, want)
	}
	if got, want := set.Routable, []string{"scan.corpus_file_requested"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("routable = %#v, want %#v", got, want)
	}
	if _, err := ValidateInputEvents(semanticview.Wrap(bundle), []string{"scan.requested"}); err == nil {
		t.Fatal("expected retired undeclared root input to fail")
	}
}

func TestValidateInputEventsRejectsDeclaredUnroutableRootInput(t *testing.T) {
	const eventName = "scan.unroutable_requested"
	bundle := rootInputTestBundle(eventName)
	bundle.FlowTree.Root.Children[0].Nodes["scan-orchestrator"] = runtimecontracts.SystemNodeContract{
		ID:           "scan-orchestrator",
		SubscribesTo: []string{"scan.other_requested"},
	}
	bundle.Nodes["scan-orchestrator"] = bundle.FlowTree.Root.Children[0].Nodes["scan-orchestrator"]

	if _, err := ValidateInputEvents(semanticview.Wrap(bundle), []string{eventName}); err == nil {
		t.Fatal("expected declared but unroutable root input to fail")
	}
}

func rootInputTestBundle(eventName string) *runtimecontracts.WorkflowContractBundle {
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Path:  "discovery",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{eventName},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": flow.Nodes["scan-orchestrator"],
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"discovery": &root.Children[0],
			},
		},
	}
}
