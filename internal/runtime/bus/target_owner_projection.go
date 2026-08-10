package bus

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type selectedRunTargetOwnerProjection struct {
	agents           map[agentidentity.Identity]ActiveAgentDescriptor
	agentsAvailable  bool
	descriptors      []ActiveTargetDescriptor
	targetsAvailable bool
	structural       events.RouteIdentity
	required         bool
}

func (p selectedRunTargetOwnerProjection) resolveRoutePlan(plan RoutePlan) (RoutePlan, error) {
	plan = plan.Normalized()
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		if intent.Recipient.IsAgent() && intent.Target.Empty() {
			if err := intent.AgentIdentity.Validate(); err != nil {
				return RoutePlan{}, fmt.Errorf("validate agent delivery owner for %s: %w", intent.Recipient.ID(), err)
			}
			continue
		}
		owner, err := p.resolve(intent.Target, intent.AllowStructuralOwner)
		if err != nil {
			return RoutePlan{}, fmt.Errorf("resolve delivery target for %s: %w", intent.Recipient.ID(), err)
		}
		intent.Target = owner
	}
	ledger, err := p.resolveConnectEvaluation(plan.ConnectEvaluation)
	if err != nil {
		return RoutePlan{}, err
	}
	plan.ConnectEvaluation = ledger
	return plan.Normalized(), nil
}

func (p selectedRunTargetOwnerProjection) resolveConnectEvaluation(ledger events.ConnectEvaluationLedger) (events.ConnectEvaluationLedger, error) {
	if !ledger.Present() {
		return ledger, nil
	}
	plans := ledger.Plans()
	resolved := make([]events.ConnectPlanEvaluation, 0, len(plans))
	for _, plan := range plans {
		targets := plan.Targets()
		for index, target := range targets {
			if target.Empty() {
				continue
			}
			owner, err := p.resolve(target, true)
			if err != nil {
				return events.ConnectEvaluationLedger{}, fmt.Errorf("resolve connect evaluation target: %w", err)
			}
			targets[index] = owner
		}
		projected, err := events.NewConnectPlanEvaluation(
			plan.PlanIdentity(), plan.Resolution(), targets, plan.Candidates(),
		)
		if err != nil {
			return events.ConnectEvaluationLedger{}, fmt.Errorf("project connect evaluation target owner: %w", err)
		}
		resolved = append(resolved, projected)
	}
	projected, err := events.NewConnectEvaluationLedger(resolved)
	if err != nil {
		return events.ConnectEvaluationLedger{}, fmt.Errorf("build projected connect evaluation: %w", err)
	}
	return projected, nil
}

func (p selectedRunTargetOwnerProjection) withActivationPlans(plans []runtimepipeline.FlowInstanceActivationPlan) (selectedRunTargetOwnerProjection, error) {
	for _, plan := range plans {
		normalized, err := plan.Normalized()
		if err != nil {
			return selectedRunTargetOwnerProjection{}, fmt.Errorf("normalize selected-run activation owner: %w", err)
		}
		if err := normalized.Validate(); err != nil {
			return selectedRunTargetOwnerProjection{}, fmt.Errorf("validate selected-run activation owner: %w", err)
		}
		p.descriptors = appendActiveTargetDescriptor(p.descriptors, ActiveTargetDescriptor{
			ID:           normalized.Identity.InstanceID,
			FlowInstance: normalized.Identity.InstancePath,
			EntityID:     normalized.Identity.EntityID,
		})
		p.targetsAvailable = true
	}
	return p, nil
}

type selectedRunTargetOwnerProjectionKey struct{}

func withSelectedRunTargetOwnerProjection(ctx context.Context, projection selectedRunTargetOwnerProjection) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, selectedRunTargetOwnerProjectionKey{}, projection)
}

func selectedRunTargetOwnerProjectionFromContext(ctx context.Context) (selectedRunTargetOwnerProjection, bool) {
	if ctx == nil {
		return selectedRunTargetOwnerProjection{}, false
	}
	projection, ok := ctx.Value(selectedRunTargetOwnerProjectionKey{}).(selectedRunTargetOwnerProjection)
	return projection, ok
}

func (p deliveryRecipientPolicy) loadSelectedRunTargetOwnerProjection(ctx context.Context) (selectedRunTargetOwnerProjection, error) {
	agents, agentsAvailable, err := p.loadActiveAgentDescriptors(ctx)
	if err != nil {
		return selectedRunTargetOwnerProjection{}, err
	}
	descriptors := activeTargetDescriptorsFromAgents(agents)
	targetsAvailable := agentsAvailable || len(descriptors) > 0
	if p.loadActiveTargetDescriptors != nil {
		loaded, available, err := p.loadActiveTargetDescriptors(ctx)
		if err != nil {
			return selectedRunTargetOwnerProjection{}, err
		}
		targetsAvailable = targetsAvailable || available
		for _, descriptor := range loaded {
			descriptors = appendActiveTargetDescriptor(descriptors, descriptor)
		}
	}
	projection := selectedRunTargetOwnerProjection{
		agents: agents, agentsAvailable: agentsAvailable,
		descriptors: descriptors, targetsAvailable: targetsAvailable, required: p.requireTargetOwners,
	}
	if route, ok := runtimedelivery.RouteFromContext(ctx); ok {
		projection.structural = route.Target.Normalized()
	}
	if projection.required {
		if err := projection.validate(); err != nil {
			return selectedRunTargetOwnerProjection{}, err
		}
	}
	return projection, nil
}

func (p selectedRunTargetOwnerProjection) validate() error {
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		if descriptor.EntityID == "" {
			return fmt.Errorf("selected-run target owner %q for flow instance %q is missing exact entity identity", descriptor.ID, descriptor.FlowInstance)
		}
	}
	return nil
}

func (p selectedRunTargetOwnerProjection) pinRoutingDescriptors(plans []runtimepinrouting.ConnectRoutePlan) []runtimepinrouting.Descriptor {
	out := make([]runtimepinrouting.Descriptor, 0, len(p.descriptors)+len(plans))
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		out = append(out, runtimepinrouting.Descriptor{
			ID: descriptor.ID, EntityID: descriptor.EntityID, FlowInstance: descriptor.FlowInstance,
			AddressFields: normalizeDescriptorAddressFields(descriptor.AddressFields),
		})
	}
	for _, plan := range plans {
		if !plan.StructuralTargetOwnerEligible() {
			continue
		}
		for _, target := range plan.Readback().Targets {
			if descriptor, ok := p.structuralDescriptor(target); ok {
				out = append(out, runtimepinrouting.Descriptor{
					ID: descriptor.ID, EntityID: descriptor.EntityID, FlowInstance: descriptor.FlowInstance,
				})
			}
		}
	}
	return out
}

func (p selectedRunTargetOwnerProjection) resolve(blueprint events.RouteIdentity, allowStructural bool) (events.RouteIdentity, error) {
	blueprint = blueprint.Normalized()
	if blueprint.Empty() {
		if !p.required {
			return blueprint, nil
		}
		return events.RouteIdentity{}, fmt.Errorf("receiver target blueprint is required")
	}
	owners := make(map[events.RouteIdentity]struct{})
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		if blueprint.FlowInstance != "" && descriptor.FlowInstance != blueprint.FlowInstance {
			continue
		}
		if blueprint.EntityID != "" && descriptor.EntityID != blueprint.EntityID {
			continue
		}
		owner := blueprint
		owner.EntityID = descriptor.EntityID
		owners[owner.Normalized()] = struct{}{}
	}
	if len(owners) == 0 && allowStructural {
		if blueprint.EntityID == "" {
			if descriptor, ok := p.structuralDescriptor(blueprint); ok {
				owner := blueprint
				owner.EntityID = descriptor.EntityID
				owners[owner.Normalized()] = struct{}{}
			}
		}
	}
	if len(owners) != 1 {
		if len(owners) == 0 && !p.required {
			return blueprint, nil
		}
		candidates := make([]string, 0, len(owners))
		for owner := range owners {
			candidates = append(candidates, fmt.Sprintf("%s/%s", owner.FlowInstance, owner.EntityID))
		}
		sort.Strings(candidates)
		if len(candidates) == 0 {
			return events.RouteIdentity{}, fmt.Errorf("receiver target owner is missing for flow instance %q", blueprint.FlowInstance)
		}
		return events.RouteIdentity{}, fmt.Errorf("receiver target owner is ambiguous for flow instance %q; candidates: %s", blueprint.FlowInstance, strings.Join(candidates, ", "))
	}
	for owner := range owners {
		return owner, nil
	}
	return events.RouteIdentity{}, fmt.Errorf("receiver target owner resolution failed")
}

func (p selectedRunTargetOwnerProjection) structuralDescriptor(blueprint events.RouteIdentity) (ActiveTargetDescriptor, bool) {
	blueprint = blueprint.Normalized()
	structural := p.structural.Normalized()
	if blueprint.FlowInstance == "" || structural.EntityID == "" {
		return ActiveTargetDescriptor{}, false
	}
	return ActiveTargetDescriptor{
		ID:       "structural:" + blueprint.FlowInstance,
		EntityID: structural.EntityID, FlowInstance: blueprint.FlowInstance,
	}.Normalized(), true
}
