package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Connection permission remains compiled-graph-owned. This lookup consumes
// actual constructor ownership, never a causal sender as a structural parent.
func (o templateInstanceLifecycleOwner) constructionParent(ctx context.Context, event events.Event, plan pinrouting.ConnectRoutePlan) (flowidentity.Instance, error) {
	bundle, found := semanticview.Bundle(o.source)
	if !found {
		return flowidentity.Instance{}, fmt.Errorf("connection construction requires its admitted tree")
	}
	view, found := bundle.FlowViewByID(plan.ReceiverEndpoint().Readback().FlowID)
	if !found || view.Parent == nil {
		return flowidentity.Instance{}, fmt.Errorf("connection receiver has no structural parent")
	}
	parentFlow := view.Parent.Paths.FlowPath
	instances, err := o.constructionOwners(ctx, event.RunID())
	if err != nil {
		return flowidentity.Instance{}, err
	}
	preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	var selected map[string]flowidentity.Instance
	if preview != nil {
		selected = preview.selected
	}
	owner, err := o.connectionLexicalOwner(ctx, event, plan.Readback().FlowPath, instances, selected)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	return o.constructedDescendant(ctx, event.RunID(), owner, parentFlow, instances, selected)
}

func (o templateInstanceLifecycleOwner) connectionLexicalOwner(ctx context.Context, event events.Event, ownerFlow string, instances map[string]flowidentity.Instance, selected map[string]flowidentity.Instance) (flowidentity.Instance, error) {
	route := event.RoutingSource().Route()
	if route.FlowInstance == "" {
		root, err := o.constructionInstance(ctx, event.RunID(), semanticview.RootExecutionFlowID(o.source), event.RunID(), flowidentity.EntityID(event.RunID()), instances)
		if err != nil {
			return flowidentity.Instance{}, fmt.Errorf("declaration connection has no exact constructed run root: %w", err)
		}
		return o.constructedDescendant(ctx, event.RunID(), root, ownerFlow, instances, selected)
	}
	owner, err := o.constructionInstance(ctx, event.RunID(), route.FlowID, route.FlowInstance, route.EntityID, instances)
	if err != nil {
		return flowidentity.Instance{}, fmt.Errorf("connection source contradicts its constructed owner: %w", err)
	}
	for steps := 0; steps <= len(o.source.FlowScopes()); steps++ {
		if owner.TemplateID == ownerFlow {
			return owner, nil
		}
		parent, err := o.constructionInstance(ctx, event.RunID(), owner.ParentRoute.FlowID, owner.ParentRoute.FlowInstance, owner.ParentEntityID, instances)
		if err != nil {
			return flowidentity.Instance{}, fmt.Errorf("connection has no exact lexical owner for %s: %w", ownerFlow, err)
		}
		owner = parent
	}
	return flowidentity.Instance{}, fmt.Errorf("connection construction ancestry is cyclic")
}

func (o templateInstanceLifecycleOwner) constructedDescendant(ctx context.Context, runID string, owner flowidentity.Instance, targetFlow string, instances map[string]flowidentity.Instance, selected map[string]flowidentity.Instance) (flowidentity.Instance, error) {
	bundle, found := semanticview.Bundle(o.source)
	if !found {
		return flowidentity.Instance{}, fmt.Errorf("construction selection requires its admitted tree")
	}
	view, found := bundle.FlowViewByID(targetFlow)
	if !found {
		return flowidentity.Instance{}, fmt.Errorf("construction selection has an unknown target %s", targetFlow)
	}
	var descendants []string
	for cursor := view; cursor.Paths.FlowPath != owner.TemplateID; cursor = cursor.Parent {
		if cursor.Parent == nil {
			return flowidentity.Instance{}, fmt.Errorf("target %s is outside lexical owner %s", targetFlow, owner.TemplateID)
		}
		descendants = append(descendants, cursor.Paths.FlowPath)
	}
	for index := len(descendants) - 1; index >= 0; index-- {
		var err error
		owner, err = o.selectedConstructionChild(ctx, runID, owner, descendants[index], instances, selected)
		if err != nil {
			return flowidentity.Instance{}, err
		}
	}
	return owner, nil
}

func (o templateInstanceLifecycleOwner) selectedConstructionChild(ctx context.Context, runID string, parent flowidentity.Instance, flowID string, instances map[string]flowidentity.Instance, selected map[string]flowidentity.Instance) (flowidentity.Instance, error) {
	schema, found := o.source.FlowSchemaByID(flowID)
	if !found {
		return flowidentity.Instance{}, fmt.Errorf("construction selection has an unknown flow %s", flowID)
	}
	if schema.Instance.Empty() {
		child, err := flowidentity.KeylessChild(o.source, parent, flowID)
		if err != nil {
			return flowidentity.Instance{}, err
		}
		stored, err := o.constructionInstance(ctx, runID, flowID, child.InstancePath, child.EntityID, instances)
		if err != nil {
			return flowidentity.Instance{}, fmt.Errorf("connection keyless ancestor %s is not constructed: %w", flowID, err)
		}
		if stored != child {
			return flowidentity.Instance{}, fmt.Errorf("connection keyless ancestor %s contradicts its structural parent", flowID)
		}
		return stored, nil
	}
	child, found := selected[flowID]
	if !found || child.ParentRoute != (flowidentity.ParentRoute{FlowID: parent.TemplateID, FlowInstance: parent.InstancePath, EntityID: parent.EntityID}) {
		return flowidentity.Instance{}, fmt.Errorf("connection keyed ancestor %s requires its exact selected constructor edge", flowID)
	}
	return child, nil
}

func (o templateInstanceLifecycleOwner) constructionInstance(ctx context.Context, runID, flowID, path, entityID string, instances map[string]flowidentity.Instance) (flowidentity.Instance, error) {
	if instance, found := instances[path]; found {
		if instance.TemplateID != flowID || entityID != "" && instance.EntityID != entityID {
			return flowidentity.Instance{}, fmt.Errorf("construction selection contradicts its exact receiver")
		}
		return instance, nil
	}
	reader, ok := o.plan.(pipeline.FlowConstructionPublicationReader)
	if !ok || entityID == "" {
		return flowidentity.Instance{}, fmt.Errorf("construction selection requires its exact durable receipt")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(flowidentity.ScopeKey(o.source, flowID), "", path))
	if err != nil {
		return flowidentity.Instance{}, err
	}
	evidence, err := reader.LoadFlowConstructionPublication(ctx, owner, entityID)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	instance := evidence.Identity
	if instance.Route() != owner.Route || instance.EntityID != entityID || instance.TemplateID != flowID {
		return flowidentity.Instance{}, fmt.Errorf("durable construction contradicts its selected receiver")
	}
	if err := instance.ValidateConstruction(o.source, runID); err != nil {
		return flowidentity.Instance{}, err
	}
	instances[path] = instance
	return instance, nil
}

func (o templateInstanceLifecycleOwner) constructionOwners(ctx context.Context, runID string) (map[string]flowidentity.Instance, error) {
	owners := make(map[string]flowidentity.Instance)
	var tables []*RouteTable
	if _, durable := o.plan.(pipeline.FlowConstructionPublicationReader); !durable {
		tables = append(tables, o.routeTable)
	}
	if preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes); preview != nil && preview.table != nil {
		tables = append(tables, preview.table)
	}
	for _, table := range tables {
		if table == nil {
			continue
		}
		table.mu.RLock()
		for coordinate, instance := range table.instanceOwners {
			if coordinate.RunID != runID {
				continue
			}
			if previous, found := owners[instance.InstancePath]; found && previous != instance {
				table.mu.RUnlock()
				return nil, fmt.Errorf("construction lookup has conflicting owner at %s", instance.InstancePath)
			}
			owners[instance.InstancePath] = instance
		}
		table.mu.RUnlock()
	}
	return owners, nil
}

func selectConnectionConstruction(ctx context.Context, instance flowidentity.Instance) error {
	preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	if preview == nil {
		return fmt.Errorf("connection construction selection requires publication planning")
	}
	if preview.selected == nil {
		preview.selected = make(map[string]flowidentity.Instance)
	}
	if previous, found := preview.selected[instance.TemplateID]; found && previous != instance {
		return fmt.Errorf("connection construction selected competing ancestors of %s", instance.TemplateID)
	}
	preview.selected[instance.TemplateID] = instance
	return nil
}
