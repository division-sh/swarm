package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Connection permission remains compiled-graph-owned. This lookup consumes
// actual constructor ownership, never a causal sender as a structural parent.
func (o connectInstanceSelector) constructionParent(ctx context.Context, event events.Event, plan pinrouting.ConnectRoutePlan) (flowidentity.Instance, error) {
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
	var selected map[string][]flowidentity.Instance
	if preview != nil {
		selected = preview.selected
	}
	owner, err := o.connectionLexicalOwner(ctx, event, plan.Readback().FlowPath, instances, selected)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	return o.constructedDescendant(ctx, event.RunID(), owner, parentFlow, instances, selected)
}

func (o connectInstanceSelector) connectionLexicalOwner(ctx context.Context, event events.Event, ownerFlow string, instances map[string]flowidentity.Instance, selected map[string][]flowidentity.Instance) (flowidentity.Instance, error) {
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

func (o connectInstanceSelector) constructedDescendant(ctx context.Context, runID string, owner flowidentity.Instance, targetFlow string, instances map[string]flowidentity.Instance, selected map[string][]flowidentity.Instance) (flowidentity.Instance, error) {
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

func (o connectInstanceSelector) selectedConstructionChild(ctx context.Context, runID string, parent flowidentity.Instance, flowID string, instances map[string]flowidentity.Instance, selected map[string][]flowidentity.Instance) (flowidentity.Instance, error) {
	schema, found := o.source.FlowSchemaByID(flowID)
	if !found {
		return flowidentity.Instance{}, fmt.Errorf("construction selection has an unknown flow %s", flowID)
	}
	if schema.Instance.Empty() {
		fact, present := correlation.SourceArtifactFactFromContext(ctx)
		if !present {
			return flowidentity.Instance{}, fmt.Errorf("connection ancestry requires its admitted source fact")
		}
		lookup, err := pipeline.NewDeclaredFlowInstanceLookup(o.source, fact, runID, flowID, parent, nil)
		if err != nil {
			return flowidentity.Instance{}, err
		}
		var prepared []pipeline.FlowInstanceActivationPlan
		if preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes); preview != nil {
			prepared = preview.plans
		}
		selected, err := pipeline.PrepareFlowInstanceSelection(ctx, o.index, o.plan, pipeline.FlowInstanceSelectionRequest{Lookup: lookup, Mode: contracts.FlowInputResolutionModeSelect, Prepared: prepared})
		if err != nil {
			return flowidentity.Instance{}, fmt.Errorf("connection keyless ancestor %s is not constructed: %w", flowID, err)
		}
		return selected.Identity(), nil
	}
	return selectedKeyedConstructionChild(parent, flowID, selected[flowID])
}

func selectedKeyedConstructionChild(parent flowidentity.Instance, flowID string, candidates []flowidentity.Instance) (flowidentity.Instance, error) {
	// Only a traversed compiled dependency needs a unique structural ancestor.
	parentRoute := flowidentity.ParentRoute{FlowID: parent.TemplateID, FlowInstance: parent.InstancePath, EntityID: parent.EntityID}
	var child flowidentity.Instance
	for _, candidate := range candidates {
		if candidate.ParentRoute != parentRoute {
			continue
		}
		if child != (flowidentity.Instance{}) && child != candidate {
			return flowidentity.Instance{}, fmt.Errorf("connection construction selected competing ancestors of %s", flowID)
		}
		child = candidate
	}
	if child == (flowidentity.Instance{}) {
		return flowidentity.Instance{}, fmt.Errorf("connection keyed ancestor %s requires its exact selected constructor edge", flowID)
	}
	return child, nil
}

func (o connectInstanceSelector) constructionInstance(ctx context.Context, runID, flowID, path, entityID string, instances map[string]flowidentity.Instance) (flowidentity.Instance, error) {
	if instance, found := instances[path]; found {
		if instance.TemplateID != flowID || entityID != "" && instance.EntityID != entityID {
			return flowidentity.Instance{}, fmt.Errorf("construction selection contradicts its exact receiver")
		}
		return instance, nil
	}
	if o.index == nil {
		return flowidentity.Instance{}, fmt.Errorf("construction selection requires its native index owner")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(flowidentity.ScopeKey(o.source, flowID), "", path))
	if err != nil {
		return flowidentity.Instance{}, err
	}
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present {
		return flowidentity.Instance{}, fmt.Errorf("construction selection requires its admitted source fact")
	}
	request, err := pipeline.NewExactFlowInstanceLookup(o.source, fact, owner)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	observed, found, err := o.index.LookupFlowInstance(ctx, request)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	if !found {
		return flowidentity.Instance{}, &pipeline.WorkflowInstanceLookupMiss{RequestedKey: path}
	}
	if err := observed.ValidateSelection(request); err != nil {
		return flowidentity.Instance{}, err
	}
	instance := observed.Identity()
	if instance.Route() != owner.Route || entityID != "" && instance.EntityID != entityID || instance.TemplateID != flowID {
		return flowidentity.Instance{}, fmt.Errorf("durable construction contradicts its selected receiver")
	}
	if err := instance.ValidateConstruction(o.source, runID); err != nil {
		return flowidentity.Instance{}, err
	}
	return instance, nil
}

func (o connectInstanceSelector) constructionOwners(ctx context.Context, runID string) (map[string]flowidentity.Instance, error) {
	owners := make(map[string]flowidentity.Instance)
	preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	if preview == nil {
		return owners, nil
	}
	for _, tree := range preview.plans {
		for _, plan := range tree.ConstructionPlans() {
			if plan.Readiness.RunID != runID || plan.Validate() != nil {
				return nil, fmt.Errorf("prepared construction has a crossed run or invalid tree")
			}
			instance := plan.Identity
			if previous, found := owners[instance.InstancePath]; found && previous != instance {
				return nil, fmt.Errorf("prepared construction has conflicting owner at %s", instance.InstancePath)
			}
			owners[instance.InstancePath] = instance
		}
	}
	return owners, nil
}

func selectConnectionConstruction(ctx context.Context, instance flowidentity.Instance) error {
	preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	if preview == nil {
		return fmt.Errorf("connection construction selection requires publication planning")
	}
	if preview.selected == nil {
		preview.selected = make(map[string][]flowidentity.Instance)
	}
	for _, previous := range preview.selected[instance.TemplateID] {
		if previous == instance {
			return nil
		}
	}
	preview.selected[instance.TemplateID] = append(preview.selected[instance.TemplateID], instance)
	return nil
}
