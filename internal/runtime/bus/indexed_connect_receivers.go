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
)

func (r connectRoutePlanResolver) addIndexedLookupPaths(ctx context.Context, runID string, flows map[string]struct{}, paths map[string]struct{}) error {
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present || r.lifecycle.index == nil {
		return fmt.Errorf("connect lookup scope requires its admitted source and index owner")
	}
	flowIDs := make([]string, 0, len(flows))
	for flowID := range flows {
		flowIDs = append(flowIDs, flowID)
	}
	scope, err := pipeline.NewFlowInstanceLookupScope(r.source, fact, runID, flowIDs, nil)
	if err != nil {
		return err
	}
	if r.lifecycle.runProposal.Present() {
		if err := r.lifecycle.runProposal.Validate(runID, fact); err != nil {
			return err
		}
	} else {
		instances, err := r.indexedInstances(ctx, scope)
		if err != nil {
			return err
		}
		for _, instance := range instances {
			paths[instance.InstancePath] = struct{}{}
		}
	}
	proposals, err := r.prospectiveConnectInstances(ctx, runID, fact)
	if err != nil {
		return err
	}
	for _, instance := range proposals {
		if _, included := flows[instance.TemplateID]; included {
			paths[instance.InstancePath] = struct{}{}
		}
	}
	return nil
}

func preparedConnectPlans(ctx context.Context) []pipeline.FlowInstanceActivationPlan {
	preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	if preview == nil {
		return nil
	}
	return preview.plans
}

func (r connectRoutePlanResolver) prospectiveConnectInstances(ctx context.Context, runID string, fact correlation.SourceArtifactFact) ([]flowidentity.Instance, error) {
	var instances []flowidentity.Instance
	for _, tree := range preparedConnectPlans(ctx) {
		for _, plan := range tree.ConstructionPlans() {
			if err := plan.Validate(); err != nil {
				return nil, err
			}
			if plan.Readiness.RunID != runID || plan.Readiness.BundleHash != fact.BundleHash() || plan.Readiness.WorkflowVersion != r.source.WorkflowVersion() {
				return nil, fmt.Errorf("prospective receiver crosses its selected source or run")
			}
			instances = append(instances, plan.Identity)
		}
	}
	return instances, nil
}

func (r connectRoutePlanResolver) indexedConnectIdentity(ctx context.Context, runID string, target events.RouteIdentity) (flowidentity.Instance, error) {
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present || r.lifecycle.index == nil {
		return flowidentity.Instance{}, fmt.Errorf("connect receiver requires its admitted source and index owner")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(flowidentity.ScopeKey(r.source, target.FlowID), "", target.FlowInstance))
	if err != nil {
		return flowidentity.Instance{}, err
	}
	request, err := pipeline.NewExactFlowInstanceLookup(r.source, fact, owner)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	proposals, err := r.prospectiveConnectInstances(ctx, runID, fact)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	for _, instance := range proposals {
		if instance.Route() == owner.Route {
			if target.EntityID != "" && instance.EntityID != target.EntityID {
				return flowidentity.Instance{}, fmt.Errorf("prospective receiver crosses its target entity")
			}
			return instance, nil
		}
	}
	observed, found, err := r.lifecycle.index.LookupFlowInstance(ctx, request)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	if !found {
		return flowidentity.Instance{}, &pipeline.WorkflowInstanceLookupMiss{RequestedKey: target.FlowInstance}
	}
	if err := observed.ValidateSelection(request); err != nil {
		return flowidentity.Instance{}, err
	}
	instance := observed.Identity()
	if target.EntityID != "" && instance.EntityID != target.EntityID {
		return flowidentity.Instance{}, fmt.Errorf("native receiver crosses its target entity")
	}
	return instance, nil
}

func (r connectRoutePlanResolver) materializeKeylessConnect(ctx context.Context, event events.Event, plan pinrouting.ConnectRoutePlan) (pinrouting.ConnectRoutePlanMaterialization, connectInstanceSelection, error) {
	flowID := plan.ReceiverEndpoint().Readback().FlowID
	if plan.ReceiverEndpoint().IsRoot() {
		instance, err := r.indexedConnectIdentity(ctx, event.RunID(), events.RouteIdentity{FlowID: flowID, FlowInstance: event.RunID()})
		if err != nil {
			return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, err
		}
		return pinrouting.ConnectRoutePlanMaterialization{Target: plan.ReceiverRoute(instance.InstancePath, instance.EntityID)}, connectInstanceSelection{}, nil
	}
	parent, err := r.lifecycle.constructionParent(ctx, event, plan)
	if err != nil {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, err
	}
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, fmt.Errorf("keyless receiver requires its admitted source")
	}
	request, err := pipeline.NewDeclaredFlowInstanceLookup(r.source, fact, event.RunID(), flowID, parent, nil)
	if err != nil {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, err
	}
	selection, err := pipeline.PrepareFlowInstanceSelection(ctx, r.lifecycle.index, nil, pipeline.FlowInstanceSelectionRequest{Lookup: request, Mode: contracts.FlowInputResolutionModeSelect, Prepared: preparedConnectPlans(ctx), RunProposal: r.lifecycle.runProposal})
	if err != nil {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, err
	}
	instance := selection.Identity()
	return pinrouting.ConnectRoutePlanMaterialization{Target: plan.ReceiverRoute(instance.InstancePath, instance.EntityID)}, connectInstanceSelection{FlowInstanceSelection: selection, receiver: plan.ReceiverEndpoint(), identity: instance}, nil
}
