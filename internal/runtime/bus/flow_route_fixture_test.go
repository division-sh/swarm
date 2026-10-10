package bus

import (
	"context"
	"errors"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (rt *RouteTable) ConstructedRouteRequestFixture(req FlowInstanceRouteMaterializationRequest) FlowInstanceRouteMaterializationRequest {
	if req.Instance != (runtimeflowidentity.Instance{}) {
		return req
	}
	declaration, found := rt.templates[req.Identity.Route.ScopeKey]
	if found {
		req.Instance = ConstructedFlowInstanceIdentityFixture(rt.source, declaration.FlowID, req.Identity.Route.InstanceID, req.Identity.RunID)
	}
	return req
}

func ConstructedFlowInstanceIdentityFixture(source semanticview.Source, flowID, instanceID, runID string) runtimeflowidentity.Instance {
	if source == nil {
		return runtimeflowidentity.Instance{}
	}
	schema, found := source.FlowSchemaByID(flowID)
	if !found {
		return runtimeflowidentity.Instance{}
	}
	if flowID == semanticview.RootExecutionFlowID(source) {
		return runtimeflowidentity.Stored(source, flowID, runID, runID, runtimeflowidentity.EntityID(runID), "")
	}
	bundle, found := semanticview.Bundle(source)
	if !found {
		return runtimeflowidentity.Instance{}
	}
	view, found := bundle.FlowViewByID(flowID)
	if !found || view.Parent == nil {
		return runtimeflowidentity.Instance{}
	}
	parent := ConstructedFlowInstanceIdentityFixture(source, view.Parent.Paths.FlowPath, "", runID)
	var child runtimeflowidentity.Instance
	var err error
	if schema.Instance.Empty() {
		child, err = runtimeflowidentity.KeylessChild(source, parent, flowID)
	} else {
		child, err = runtimeflowidentity.KeyedChild(source, parent, flowID, instanceID)
	}
	if err != nil {
		return runtimeflowidentity.Instance{}
	}
	return child
}

func StoredFlowInstanceIdentityFixture(source semanticview.Source, flowID, instanceID, runID, entityID string) runtimeflowidentity.Instance {
	instance := ConstructedFlowInstanceIdentityFixture(source, flowID, instanceID, runID)
	instance.EntityID = entityID
	return instance
}

func (rt *RouteTable) AddConstructedFlowInstanceRouteFixture(req FlowInstanceRouteMaterializationRequest) error {
	return rt.AddFlowInstanceRoute(rt.ConstructedRouteRequestFixture(req))
}

// These isolated table inputs neither construct native instances nor stage a
// durable topology. They retire with the remaining mutable-table tests.
func (eb *EventBus) AddFlowInstanceRouteFixture(req FlowInstanceRouteMaterializationRequest) error {
	return eb.AddFlowInstanceRouteContextFixture(context.Background(), req)
}

func (eb *EventBus) PublishPersistedFlowInstanceRouteFixture(req FlowInstanceRouteMaterializationRequest) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	if _, err := eb.admitSourceArtifactFact(context.Background()); err != nil {
		return err
	}
	table := eb.RouteTable()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	return table.AddConstructedFlowInstanceRouteFixture(req.Normalized())
}

func (eb *EventBus) RetirePublishedFlowInstanceRouteFixture(identity runtimeflowidentity.RunScopedFlowInstance) error {
	if eb == nil || eb.RouteTable() == nil {
		return errors.New("route table is not initialized")
	}
	return eb.RouteTable().RemoveFlowInstanceRoute(identity)
}

func (eb *EventBus) AddFlowInstanceRouteContextFixture(ctx context.Context, req FlowInstanceRouteMaterializationRequest) error {
	if eb != nil && eb.RouteTable() != nil {
		req = eb.RouteTable().ConstructedRouteRequestFixture(req)
	}
	if eb == nil {
		return errors.New("event bus is required")
	}
	if _, err := eb.admitSourceArtifactFact(ctx); err != nil {
		return err
	}
	if reader, ok := eb.durable.ConstructionPublications.(interface {
		installConstructionReceipt(runtimeflowidentity.RunScopedFlowInstance, runtimepipeline.FlowConstructionPublicationEvidence)
	}); ok {
		reader.installConstructionReceipt(req.Identity, runtimepipeline.FlowConstructionPublicationEvidence{Identity: req.Instance})
	}
	return eb.PublishPersistedFlowInstanceRouteFixture(req)
}

func (eb *EventBus) RemoveFlowInstanceRouteFixture(identity runtimeflowidentity.RunScopedFlowInstance) error {
	return eb.RemoveFlowInstanceRouteContextFixture(context.Background(), identity)
}

func (eb *EventBus) RemoveFlowInstanceRouteContextFixture(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	if eb == nil {
		return errors.New("event bus is required")
	}
	var err error
	ctx, err = eb.admitSourceArtifactFact(ctx)
	if err != nil {
		return err
	}
	table := eb.RouteTable()
	if table == nil {
		return errors.New("route table is not initialized")
	}
	return table.RemoveFlowInstanceRoute(identity)
}
