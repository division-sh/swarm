package bus_test

import (
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
)

func TestNewEventBusWithOptions_DoesNotUseAmbientWorkflowSemanticSource(t *testing.T) {
	scoring := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "scoring", Flow: "scoring"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"score.dimension_complete"}},
			},
		},
		Path: "scoring",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"score.dimension_complete": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scoring-node": {
				ID:           "scoring-node",
				SubscribesTo: []string{"score.dimension_complete"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{scoring}}
	_ = &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"scoring": &root.Children[0],
			},
		},
	}

	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if got := eb.RouteTable().Resolve("scoring/score.dimension_complete"); len(got) != 0 {
		t.Fatalf("Resolve(scoring/score.dimension_complete) = %#v, want no ambient-derived routes", got)
	}
}
