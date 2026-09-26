package bus

import (
	"context"
	"errors"
	"fmt"
	"sort"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

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
	return table.AddFlowInstanceRoute(req.Normalized())
}

func (eb *EventBus) RetirePublishedFlowInstanceRouteFixture(identity runtimeflowidentity.RunScopedFlowInstance) error {
	if eb == nil || eb.RouteTable() == nil {
		return errors.New("route table is not initialized")
	}
	return eb.RouteTable().RemoveFlowInstanceRoute(identity)
}

func (eb *EventBus) AddFlowInstanceRouteContextFixture(ctx context.Context, req FlowInstanceRouteMaterializationRequest) error {
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
	staged, identities, err := eb.deriveFlowInstanceRouteTopology(ctx, table, descriptorLister, owner.RunID, nil, owner)
	if err != nil {
		return err
	}
	identities = append(identities, owner)
	sort.Slice(identities, func(i, j int) bool { return identities[i].Key() < identities[j].Key() })
	committed, commitErr := persister.ReplaceFlowInstanceRouteTopology(ctx, flowInstanceRouteTopologyRecordSets(staged, identities))
	if !committed.Acknowledged {
		return errors.Join(commitErr, errors.New("flow-instance route topology commit was not acknowledged"))
	}
	return errors.Join(commitErr, table.removeFlowInstanceRouteForContext(context.WithoutCancel(ctx), owner))
}
