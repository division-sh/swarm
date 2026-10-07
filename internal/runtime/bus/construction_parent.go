package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
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
	owner, err := o.connectionLexicalOwner(event, plan.Readback().FlowPath, instances, selected)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	return o.constructedDescendant(owner, parentFlow, instances, selected)
}

func (o templateInstanceLifecycleOwner) connectionLexicalOwner(event events.Event, ownerFlow string, instances map[string]flowidentity.Instance, selected map[string]flowidentity.Instance) (flowidentity.Instance, error) {
	route := event.RoutingSource().Route()
	if route.FlowInstance == "" {
		root, found := instances[event.RunID()]
		if !found || root.TemplateID != semanticview.RootExecutionFlowID(o.source) || root.EntityID != flowidentity.EntityID(event.RunID()) {
			return flowidentity.Instance{}, fmt.Errorf("declaration connection has no exact constructed run root")
		}
		return o.constructedDescendant(root, ownerFlow, instances, selected)
	}
	owner, found := instances[route.FlowInstance]
	if !found || owner.TemplateID != route.FlowID || route.EntityID != "" && owner.EntityID != route.EntityID {
		return flowidentity.Instance{}, fmt.Errorf("connection source contradicts its constructed owner")
	}
	for steps := 0; steps <= len(instances); steps++ {
		if owner.TemplateID == ownerFlow {
			return owner, nil
		}
		parent, found := instances[owner.ParentRoute.FlowInstance]
		if !found || parent.TemplateID != owner.ParentRoute.FlowID || parent.EntityID != owner.ParentEntityID {
			return flowidentity.Instance{}, fmt.Errorf("connection has no exact lexical owner for %s", ownerFlow)
		}
		owner = parent
	}
	return flowidentity.Instance{}, fmt.Errorf("connection construction ancestry is cyclic")
}

func (o templateInstanceLifecycleOwner) constructedDescendant(owner flowidentity.Instance, targetFlow string, instances map[string]flowidentity.Instance, selected map[string]flowidentity.Instance) (flowidentity.Instance, error) {
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
		owner, err = o.selectedConstructionChild(owner, descendants[index], instances, selected)
		if err != nil {
			return flowidentity.Instance{}, err
		}
	}
	return owner, nil
}

func (o templateInstanceLifecycleOwner) selectedConstructionChild(parent flowidentity.Instance, flowID string, instances map[string]flowidentity.Instance, selected map[string]flowidentity.Instance) (flowidentity.Instance, error) {
	schema, found := o.source.FlowSchemaByID(flowID)
	if !found {
		return flowidentity.Instance{}, fmt.Errorf("construction selection has an unknown flow %s", flowID)
	}
	if schema.Instance.Empty() {
		child, err := flowidentity.KeylessChild(o.source, parent, flowID)
		if err != nil {
			return flowidentity.Instance{}, err
		}
		stored, found := instances[child.InstancePath]
		if !found || stored != child {
			return flowidentity.Instance{}, fmt.Errorf("connection keyless ancestor %s is not constructed", flowID)
		}
		return stored, nil
	}
	child, found := selected[flowID]
	if !found || child.ParentRoute != (flowidentity.ParentRoute{FlowID: parent.TemplateID, FlowInstance: parent.InstancePath, EntityID: parent.EntityID}) || instances[child.InstancePath] != child {
		return flowidentity.Instance{}, fmt.Errorf("connection keyed ancestor %s requires its exact selected constructor edge", flowID)
	}
	return child, nil
}

func (o templateInstanceLifecycleOwner) constructionOwners(ctx context.Context, runID string) (map[string]flowidentity.Instance, error) {
	owners := make(map[string]flowidentity.Instance)
	tables := []*RouteTable{o.routeTable}
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
