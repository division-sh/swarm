package pipeline

import (
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/flowmodel"
	"swarm/internal/runtime/semanticview"
)

func TestWorkflowFlowInputProducerAliases_IncludeProducerScopedAlias(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	discovery := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "discovery",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{"scan.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, discovery}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer":  &root.Children[0],
				"discovery": &root.Children[1],
			},
		},
	}

	aliases := workflowFlowInputProducerAliases(semanticview.Wrap(bundle), "discovery", "scan.requested")
	for _, candidate := range aliases {
		if candidate == "producer/scan.requested" {
			return
		}
	}
	t.Fatalf("aliases = %#v, want producer/scan.requested", aliases)
}

func TestWorkflowFlowInputProducerAliases_AutoWireCrossFlowInputPinsToProducerScopedEvent(t *testing.T) {
	scoring := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "scoring", Flow: "scoring"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"vertical.shortlisted"}},
			},
		},
		Path: "scoring",
	}
	validation := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"vertical.shortlisted"}},
			},
		},
		Path: "validation",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"validation-orchestrator": {
				ID:           "validation-orchestrator",
				SubscribesTo: []string{"vertical.shortlisted"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{scoring, validation}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"scoring":    &root.Children[0],
				"validation": &root.Children[1],
			},
		},
	}

	aliases := workflowFlowInputProducerAliases(semanticview.Wrap(bundle), "validation", "vertical.shortlisted")
	for _, alias := range aliases {
		if alias == "scoring/vertical.shortlisted" {
			return
		}
	}
	t.Fatalf("aliases = %#v, want scoring/vertical.shortlisted", aliases)
}
