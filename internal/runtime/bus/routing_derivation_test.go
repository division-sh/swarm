package bus_test

import (
	"context"
	"testing"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/flowmodel"
	"swarm/internal/runtime/semanticview"
)

func TestEventBusRemoveFlowInstanceDropsDerivedRoutes(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.AddFlowInstance(runtimecontracts.SystemNodeContract{
		ID:           "reviewer-{instance_id}",
		Produces:     []string{"task.started"},
		SubscribesTo: []string{"task.started"},
	}, "review/inst-1"); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	if got := eb.RouteTable().Resolve("review/inst-1/task.started"); len(got) != 1 || got[0].ID != "reviewer-inst-1" {
		t.Fatalf("resolved subscribers after add = %#v", got)
	}
	if err := eb.RemoveFlowInstance("review", "inst-1"); err != nil {
		t.Fatalf("RemoveFlowInstance: %v", err)
	}
	if got := eb.RouteTable().Resolve("review/inst-1/task.started"); len(got) != 0 {
		t.Fatalf("resolved subscribers after remove = %#v, want none", got)
	}
}

type routePersistenceTestStore struct {
	routes map[string]runtimebus.FlowInstanceRouteRecord
}

func (*routePersistenceTestStore) AppendEvent(context.Context, events.Event) error { return nil }
func (*routePersistenceTestStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (s *routePersistenceTestStore) UpsertFlowInstanceRoute(_ context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s.routes == nil {
		s.routes = map[string]runtimebus.FlowInstanceRouteRecord{}
	}
	s.routes[route.TemplateID+"/"+route.InstanceID] = route
	return nil
}

func (s *routePersistenceTestStore) DeleteFlowInstanceRoute(_ context.Context, templateID, instanceID string) error {
	delete(s.routes, templateID+"/"+instanceID)
	return nil
}

func (s *routePersistenceTestStore) ListFlowInstanceRoutes(context.Context) ([]runtimebus.FlowInstanceRouteRecord, error) {
	out := make([]runtimebus.FlowInstanceRouteRecord, 0, len(s.routes))
	for _, route := range s.routes {
		out = append(out, route)
	}
	return out, nil
}

func TestEventBusFlowInstanceRoutesPersistAcrossAddAndRemove(t *testing.T) {
	store := &routePersistenceTestStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.AddFlowInstance(runtimecontracts.SystemNodeContract{
		ID:           "reviewer-{instance_id}",
		Produces:     []string{"task.started"},
		SubscribesTo: []string{"task.started"},
	}, "review/inst-1"); err != nil {
		t.Fatalf("AddFlowInstance: %v", err)
	}
	if _, ok := store.routes["review/inst-1"]; !ok {
		t.Fatalf("persisted routes = %#v, want review/inst-1", store.routes)
	}
	if err := eb.RemoveFlowInstance("review", "inst-1"); err != nil {
		t.Fatalf("RemoveFlowInstance: %v", err)
	}
	if len(store.routes) != 0 {
		t.Fatalf("persisted routes after remove = %#v, want none", store.routes)
	}
}

func TestDeriveRouteTable_InputPinsAutoWireFromProducerOutput(t *testing.T) {
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
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := rt.Resolve("producer/scan.requested")
	if len(got) != 1 || got[0].ID != "scan-orchestrator" {
		t.Fatalf("Resolve(producer/scan.requested) = %#v, want scan-orchestrator", got)
	}
	if got := rt.Resolve("scan.requested"); len(got) != 0 {
		t.Fatalf("Resolve(scan.requested) = %#v, want none", got)
	}
	if got := rt.Resolve("discovery/scan.requested"); len(got) != 0 {
		t.Fatalf("Resolve(discovery/scan.requested) = %#v, want none", got)
	}
}

func TestDeriveRouteTable_InputPinsStayLocalWithoutExternalProducer(t *testing.T) {
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
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"scoring": &root.Children[0],
			},
		},
	}
	rt, err := runtimebus.DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := rt.Resolve("scoring/score.dimension_complete")
	if len(got) != 1 || got[0].ID != "scoring-node" {
		t.Fatalf("Resolve(scoring/score.dimension_complete) = %#v, want scoring-node", got)
	}
	if got := rt.Resolve("score.dimension_complete"); len(got) != 0 {
		t.Fatalf("Resolve(score.dimension_complete) = %#v, want none", got)
	}
}
