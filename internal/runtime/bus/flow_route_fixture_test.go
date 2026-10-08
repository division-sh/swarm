package bus

import (
	"context"
	"errors"
	"fmt"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (rt *RouteTable) ConstructedRouteRequestFixture(req FlowInstanceRouteMaterializationRequest) FlowInstanceRouteMaterializationRequest {
	if req.Instance != (runtimeflowidentity.Instance{}) {
		return req
	}
	flowID, found := rt.FlowInstanceTemplateID(req.Identity.Route)
	if found {
		req.Instance = ConstructedFlowInstanceIdentityFixture(rt.source, flowID, req.Identity.Route.InstanceID, req.Identity.RunID)
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

// Fixture methods deliberately bypass activation authority. They construct
// route-state inputs for routing tests and are absent from production builds.
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
	committed, commitErr := eb.StageFlowInstanceRouteContext(ctx, req)
	if !committed.Acknowledged {
		return errors.Join(commitErr, errors.New("flow-instance route topology commit was not acknowledged"))
	}
	return errors.Join(commitErr, eb.PublishPersistedFlowInstanceRouteFixture(req))
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
	owner, exists, err := table.flowInstanceRouteRemovalOwner(identity)
	if err != nil {
		return err
	}
	if !exists {
		owner = identity.Normalize()
		if owner.Validate() != nil {
			return fmt.Errorf("flow-instance route removal requires exact identity")
		}
	}
	persister := eb.durable.FlowRouteTopology
	if persister == nil {
		if eb.ephemeral {
			return table.removeFlowInstanceRouteForContext(ctx, owner)
		}
		return errors.New("selected store requires exact flow-instance route-set persistence")
	}
	descriptorLister := eb.durable.ActiveFlows
	if descriptorLister == nil {
		return errors.New("flow-instance route removal requires active flow-instance descriptors")
	}
	staged, identities, err := eb.deriveFlowInstanceRouteRecordTopology(ctx, table, descriptorLister, owner.RunID, nil, owner)
	if err != nil {
		return err
	}
	committed, commitErr := persister.ReplaceFlowInstanceRouteTopology(ctx, flowInstanceRouteTopologyRecordSets(staged, identities))
	if !committed.Acknowledged {
		return errors.Join(commitErr, errors.New("flow-instance route topology commit was not acknowledged"))
	}
	return errors.Join(commitErr, table.removeFlowInstanceRouteForContext(context.WithoutCancel(ctx), owner))
}
