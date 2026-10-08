package pinrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func constructedParentSourceFixture(t *testing.T) semanticview.Source {
	t.Helper()
	leaf := contracts.FlowContractView{
		Path: "orders/child/leaf", Paths: contracts.FlowContractPaths{FlowPath: "orders/child/leaf"},
		Schema:    contracts.FlowSchemaDocument{Name: "leaf"},
		Agents:    map[string]contracts.AgentRegistryEntry{"reader": {ID: "reader"}},
		AgentURIs: map[string]string{"reader": "test://orders/child/leaf/reader"},
	}
	child := contracts.FlowContractView{
		Path: "orders/child", Paths: contracts.FlowContractPaths{FlowPath: "orders/child"},
		Schema: contracts.FlowSchemaDocument{Name: "child"}, Children: []contracts.FlowContractView{leaf},
		Agents:    map[string]contracts.AgentRegistryEntry{"reader": {ID: "reader"}},
		AgentURIs: map[string]string{"reader": "test://orders/child/reader"},
	}
	root := contracts.FlowContractView{Path: ".", Paths: contracts.FlowContractPaths{FlowPath: "."}, Schema: contracts.FlowSchemaDocument{Name: "root"},
		Children: []contracts.FlowContractView{{Path: "orders", Paths: contracts.FlowContractPaths{FlowPath: "orders"},
			Schema: contracts.FlowSchemaDocument{Name: "orders", Instance: mustTemplateInstanceField(t, "order_id")}, Children: []contracts.FlowContractView{child}}},
	}
	bundle := &contracts.WorkflowContractBundle{RootSchema: &root.Schema,
		FlowSchemas: map[string]contracts.FlowSchemaDocument{},
		FlowTree:    contracts.FlowTree{Root: &root, ByID: map[string]*contracts.FlowContractView{}, ByPath: map[string]*contracts.FlowContractView{}},
		URIRegistry: contracts.ContractURIRegistry{ByURI: map[string]contracts.ContractURIRef{}},
	}
	var index func(*contracts.FlowContractView, *contracts.FlowContractView)
	index = func(view, parent *contracts.FlowContractView) {
		view.Parent = parent
		bundle.FlowTree.ByID[view.Paths.FlowPath], bundle.FlowTree.ByPath[view.Paths.FlowPath] = view, view
		bundle.FlowSchemas[view.Paths.FlowPath] = view.Schema
		for local, owner := range view.AgentURIs {
			bundle.URIRegistry.ByURI[owner] = contracts.ContractURIRef{Kind: "agent", FlowID: view.Paths.FlowPath, LocalID: local, Full: owner}
		}
		for i := range view.Children {
			index(&view.Children[i], view)
		}
	}
	index(&root, nil)
	return semanticview.Wrap(bundle)
}

func TestConstructedParentSourceSiblingProbe(t *testing.T) {
	source := constructedParentSourceFixture(t)
	for _, key := range []string{"one", "two"} {
		parent := flowidentity.Derive(source, "orders", key)
		for _, flow := range []string{"orders/child", "orders/child/leaf"} {
			child, err := flowidentity.KeylessChild(source, parent, flow)
			if err != nil {
				t.Fatal(err)
			}
			parent = child
			t.Run(child.InstancePath, func(t *testing.T) {
				owner := "test://" + flow + "/reader"
				actor := actors.AgentConfig{ID: "reader", FlowID: flow, FlowPath: child.InstancePath,
					Identity: agentidentitytest.Declared(t, "reader", owner, child.ScopeKey, child.InstanceID, child.InstancePath)}
				route := events.RouteIdentity{FlowID: flow, FlowInstance: child.InstancePath, EntityID: child.EntityID}
				got, err := AdmitFlowExecutionRoutingSource(source, actor.Identity.RunID, child, route)
				if err != nil || got.Route() != route || got.Kind() != events.RoutingSourceStaticFlow {
					t.Fatalf("constructed flow source = %+v: %v", got.Route(), err)
				}
				for _, entity := range []string{"", child.EntityID} {
					actor.EntityID = entity
					got, err := AdmitAgentExecutionRoutingSource(source, actor, entity, child)
					want := events.RouteIdentity{FlowID: flow, FlowInstance: child.InstancePath, EntityID: entity}
					if err != nil || got.Route() != want || got.Kind() != events.RoutingSourceStaticFlow {
						t.Fatalf("constructed agent source = %+v, want %+v: %v", got.Route(), want, err)
					}
				}
			})
		}
	}
}

func TestDerivedStaticSourceValidatesBeforeNormalization(t *testing.T) {
	source := constructedParentSourceFixture(t)
	parent := flowidentity.Derive(source, "orders", "one")
	child, err := flowidentity.KeylessChild(source, parent, "orders/child")
	if err != nil {
		t.Fatal(err)
	}
	actor := actors.AgentConfig{ID: "reader", FlowID: child.TemplateID, FlowPath: child.InstancePath,
		Identity: agentidentitytest.Declared(t, "reader", "test://orders/child/reader", child.ScopeKey, child.InstanceID, child.InstancePath)}
	for _, test := range []struct {
		name   string
		mutate func(*flowidentity.Instance)
	}{
		{"missing parent", func(i *flowidentity.Instance) { i.ParentRoute = flowidentity.ParentRoute{} }},
		{"foreign parent", func(i *flowidentity.Instance) {
			i.ParentRoute.FlowInstance = "orders/two"
			i.ParentRoute.EntityID = flowidentity.EntityID("orders/two")
			i.ParentEntityID = i.ParentRoute.EntityID
		}},
		{"contradictory parent entity", func(i *flowidentity.Instance) { i.ParentRoute.EntityID = "foreign" }},
		{"foreign declaration", func(i *flowidentity.Instance) { i.TemplateID = "orders" }},
		{"foreign entity", func(i *flowidentity.Instance) { i.EntityID = "foreign" }},
		{"unstored", func(i *flowidentity.Instance) { i.HasStoredPath = false }},
		{"malformed parent", func(i *flowidentity.Instance) { i.ParentRoute.FlowInstance = " " + i.ParentRoute.FlowInstance }},
		{"authored path substitution", func(i *flowidentity.Instance) { i.InstancePath = i.ScopeKey }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := child
			test.mutate(&bad)
			route := events.RouteIdentity{FlowID: bad.TemplateID, FlowInstance: bad.InstancePath, EntityID: bad.EntityID}
			if _, err := AdmitFlowExecutionRoutingSource(source, actor.Identity.RunID, bad, route); err == nil {
				t.Fatal("invalid construction acquired flow source authority")
			}
			if _, err := AdmitAgentExecutionRoutingSource(source, actor, "", bad); err == nil {
				t.Fatal("invalid construction acquired agent source authority")
			}
		})
	}
	if _, err := AdmitAgentExecutionRoutingSource(source, actor, "", nil); err == nil {
		t.Fatal("route without construction acquired agent source authority")
	}
}
