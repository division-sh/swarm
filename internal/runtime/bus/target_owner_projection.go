package bus

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type selectedRunTargetOwnerProjection struct {
	context           context.Context
	agents            map[agentidentity.Identity]ActiveAgentDescriptor
	agentsAvailable   bool
	descriptors       []ActiveTargetDescriptor
	targetsAvailable  bool
	workflowInstances runtimepipeline.WorkflowInstancePersistenceReader
	source            semanticview.Source
	required          bool
}

func (p selectedRunTargetOwnerProjection) resolveRoutePlan(plan RoutePlan) (RoutePlan, error) {
	plan = plan.Normalized()
	var err error
	if err = p.resolveNodeTargetOwners(&plan); err != nil {
		return RoutePlan{}, err
	}
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		if !intent.TargetOwnership.Empty() {
			if err := intent.TargetOwnership.Validate(); err != nil {
				return RoutePlan{}, fmt.Errorf("validate admitted delivery target for %s: %w", intent.Recipient.ID(), err)
			}
			if intent.TargetBlueprint.Normalized() != intent.TargetOwnership.Route() {
				return RoutePlan{}, fmt.Errorf("validate admitted delivery target for %s: blueprint and typed owner disagree: blueprint=%#v owner=%s %#v", intent.Recipient.ID(), intent.TargetBlueprint.Normalized(), intent.TargetOwnership.Code(), intent.TargetOwnership.Route())
			}
			continue
		}
		if intent.Recipient.IsAgent() {
			if err := intent.AgentIdentity.Validate(); err != nil {
				return RoutePlan{}, fmt.Errorf("validate agent delivery owner for %s: %w", intent.Recipient.ID(), err)
			}
			if intent.TargetBlueprint.Empty() {
				blueprint, targeted, err := p.sameFlowAgentTargetBlueprint(plan.Event, *intent)
				if err != nil {
					return RoutePlan{}, fmt.Errorf("resolve same-flow agent delivery target for %s: %w", intent.Recipient.ID(), err)
				}
				if !targeted {
					continue
				}
				intent.TargetBlueprint = blueprint
			}
			if p.agentsAvailable {
				descriptor, ok := p.agents[intent.AgentIdentity.Normalize()]
				if !ok {
					if intent.PendingAgentLifecycle {
						owner, err := p.resolveSelectedRoute(intent.TargetBlueprint)
						if err != nil {
							return RoutePlan{}, fmt.Errorf("resolve pending delivery target for %s: %w", intent.Recipient.ID(), err)
						}
						if !owner.MaterializingEntity() {
							return RoutePlan{}, fmt.Errorf("resolve pending delivery target for %s: lifecycle creation requires materializing_entity ownership", intent.Recipient.ID())
						}
						intent.TargetOwnership = owner
						intent.TargetBlueprint = owner.Route()
						continue
					}
					return RoutePlan{}, fmt.Errorf("resolve delivery target for %s: exact active agent identity is missing", intent.Recipient.ID())
				}
				descriptor = descriptor.Normalized()
				ownerRoute := intent.TargetBlueprint.Normalized()
				if instance := descriptor.Identity.FlowInstance(); instance != "" && ownerRoute.FlowInstance != instance {
					return RoutePlan{}, fmt.Errorf("resolve delivery target for %s: target flow instance %q disagrees with exact agent identity %q", intent.Recipient.ID(), ownerRoute.FlowInstance, instance)
				}
				var (
					owner events.DeliveryTargetOwnership
					err   error
				)
				if descriptor.EntityID == "" {
					owner, err = p.resolveSelectedRoute(ownerRoute)
				} else {
					if ownerRoute.EntityID != "" && ownerRoute.EntityID != descriptor.EntityID {
						return RoutePlan{}, fmt.Errorf("resolve delivery target for %s: target entity %q disagrees with exact active agent entity %q", intent.Recipient.ID(), ownerRoute.EntityID, descriptor.EntityID)
					}
					ownerRoute.EntityID = descriptor.EntityID
					if p.targetsAvailable {
						owner, err = p.resolveSelectedRoute(ownerRoute)
					} else {
						owner, err = events.NewExistingEntityTarget(ownerRoute)
					}
				}
				if err != nil {
					return RoutePlan{}, fmt.Errorf("resolve delivery target for %s from exact agent identity: %w", intent.Recipient.ID(), err)
				}
				if !owner.ExistingEntity() && !owner.MaterializingEntity() {
					return RoutePlan{}, fmt.Errorf("resolve delivery target for %s from exact agent identity: targeted agent requires entity ownership", intent.Recipient.ID())
				}
				intent.TargetOwnership = owner
				intent.TargetBlueprint = owner.Route()
				continue
			}
			owner, err := p.resolveSelectedRoute(intent.TargetBlueprint)
			if err != nil {
				return RoutePlan{}, fmt.Errorf("resolve delivery target for %s: %w", intent.Recipient.ID(), err)
			}
			intent.TargetOwnership = owner
			if !owner.Empty() {
				intent.TargetBlueprint = owner.Route()
			}
			continue
		}
		handler := intent.Handler
		if handler.Empty() {
			return RoutePlan{}, fmt.Errorf("resolve delivery target handler for %s: route intent has no exact admitted handler", intent.Recipient.ID())
		}
		return RoutePlan{}, fmt.Errorf("resolve delivery target for %s: node intent escaped canonical preclassification", intent.Recipient.ID())
	}
	ledger, err := p.resolveConnectEvaluation(plan.ConnectEvaluation, plan.DeliveryIntents)
	if err != nil {
		return RoutePlan{}, err
	}
	plan.ConnectEvaluation = ledger
	return plan.Normalized(), nil
}

func (p selectedRunTargetOwnerProjection) resolveNodeTargetOwners(plan *RoutePlan) error {
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		if intent.Recipient.IsAgent() {
			continue
		}
		if intent.TargetOwnership.Empty() {
			handler := intent.Handler
			if handler.Empty() {
				return fmt.Errorf("resolve delivery target handler for %s: route intent has no exact admitted handler", intent.Recipient.ID())
			}
			owner, err := runtimepipeline.ClassifyDeliveryTargetOwnership(runtimepipeline.DeliveryTargetOwnershipRequest{
				Context: p.context, Source: p.source, Event: plan.Event, Recipient: intent.Recipient, Blueprint: intent.TargetBlueprint,
				Handler: handler, Candidates: p.targetOwnerCandidates(), WorkflowInstances: p.workflowInstances,
			})
			if err != nil {
				return fmt.Errorf("resolve delivery target for %s: %w", intent.Recipient.ID(), err)
			}
			intent.TargetOwnership = owner
			intent.TargetBlueprint = owner.Route()
		} else {
			if err := intent.TargetOwnership.Validate(); err != nil {
				return fmt.Errorf("validate admitted delivery target for %s: %w", intent.Recipient.ID(), err)
			}
			if intent.TargetBlueprint.Normalized() != intent.TargetOwnership.Route() {
				return fmt.Errorf("validate admitted delivery target for %s: blueprint and typed owner disagree: blueprint=%#v owner=%s %#v", intent.Recipient.ID(), intent.TargetBlueprint.Normalized(), intent.TargetOwnership.Code(), intent.TargetOwnership.Route())
			}
		}
	}
	return nil
}

func (p selectedRunTargetOwnerProjection) sameFlowAgentTargetBlueprint(evt events.Event, intent RoutePlanDeliveryIntent) (events.RouteIdentity, bool, error) {
	if intent.Producer != routeIntentProducerAgentPolicy {
		return events.RouteIdentity{}, false, nil
	}
	identity := intent.AgentIdentity.Normalize()
	if identity.Name.Source != agentidentity.NameSourceDeclared {
		return events.RouteIdentity{}, false, nil
	}
	descriptor, ok := p.agents[identity]
	if !ok {
		return events.RouteIdentity{}, false, nil
	}
	activeEntityID := descriptor.Normalized().EntityID
	if activeEntityID == "" {
		return events.RouteIdentity{}, false, nil
	}
	instance := identity.FlowInstance()
	flowID := runtimeflowidentity.SemanticScopeFromFlowInstanceRef(instance)
	if instance == "" {
		if p.source == nil {
			return events.RouteIdentity{}, false, nil
		}
		coordinate, err := semanticview.AdmitRootExecutionCoordinate(p.source, evt.RunID())
		if err != nil {
			return events.RouteIdentity{}, false, err
		}
		flowID, instance = coordinate.FlowID(), coordinate.RunID()
	}
	if flowID == "" {
		return events.RouteIdentity{}, false, nil
	}
	blueprint := events.RouteIdentity{FlowID: flowID, FlowInstance: instance, EntityID: activeEntityID}.Normalized()
	foundFlowOwner := false
	for _, selected := range p.descriptors {
		selected = selected.Normalized()
		if selected.FlowInstance != blueprint.FlowInstance {
			continue
		}
		foundFlowOwner = true
		if selected.EntityID == activeEntityID {
			return blueprint, true, nil
		}
	}
	if foundFlowOwner {
		return events.RouteIdentity{}, false, fmt.Errorf("active agent entity %q disagrees with every selected owner for flow instance %q", activeEntityID, blueprint.FlowInstance)
	}
	return events.RouteIdentity{}, false, fmt.Errorf("active agent entity %q has no exact selected owner for flow instance %q", activeEntityID, blueprint.FlowInstance)
}

func (p selectedRunTargetOwnerProjection) resolveConnectEvaluation(ledger events.ConnectEvaluationLedger, intents []RoutePlanDeliveryIntent) (events.ConnectEvaluationLedger, error) {
	for _, intent := range intents {
		if intent.ConnectPlan.Empty() {
			continue
		}
		if intent.TargetOwnership.Empty() {
			return events.ConnectEvaluationLedger{}, fmt.Errorf("connect recipient has no admitted target ownership")
		}
		if err := intent.TargetOwnership.Validate(); err != nil {
			return events.ConnectEvaluationLedger{}, err
		}
		route := intent.TargetOwnership.Route()
		matched := false
		for _, plan := range ledger.Plans() {
			if plan.PlanIdentity() != intent.ConnectPlan {
				continue
			}
			for _, target := range plan.Targets() {
				if target.FlowID == route.FlowID && target.FlowInstance == route.FlowInstance && (target.EntityID == "" || target.EntityID == route.EntityID) {
					matched = true
				}
			}
		}
		if !matched {
			return events.ConnectEvaluationLedger{}, fmt.Errorf("connect recipient owner has no agreeing exact plan target")
		}
	}
	if !ledger.Present() {
		return ledger, nil
	}
	plans := ledger.Plans()
	resolved := make([]events.ConnectPlanEvaluation, 0, len(plans))
	for _, plan := range plans {
		targets := make([]events.RouteIdentity, 0, len(plan.Targets()))
		for _, target := range plan.Targets() {
			matched := false
			for _, intent := range intents {
				if intent.ConnectPlan != plan.PlanIdentity() {
					continue
				}
				owner := intent.TargetOwnership
				route := owner.Route()
				if owner.Empty() || route.FlowID != target.FlowID || route.FlowInstance != target.FlowInstance {
					continue
				}
				if target.EntityID != "" && route.EntityID != target.EntityID {
					return events.ConnectEvaluationLedger{}, fmt.Errorf("connect recipient owner contradicts exact compiled target")
				}
				// A single connect can admit independent entityless and entity-bearing
				// recipients. Retain every decision, never force one sibling's owner.
				targets = append(targets, route)
				matched = true
			}
			if !matched {
				targets = append(targets, target)
			}
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
			ID:            normalized.Identity.InstanceID,
			FlowInstance:  normalized.Identity.InstancePath,
			EntityID:      normalized.Identity.EntityID,
			Materializing: true,
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
	var descriptors []ActiveTargetDescriptor
	targetsAvailable := false
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
		context: ctx,
		agents:  agents, agentsAvailable: agentsAvailable,
		descriptors: descriptors, targetsAvailable: targetsAvailable, workflowInstances: p.workflowInstances,
		source: p.semanticSource, required: p.requireTargetOwners,
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
		if descriptor.FlowInstance == "" || descriptor.EntityID == "" {
			return fmt.Errorf("selected-run target owner %q is missing exact entity identity or flow-instance identity: flow_instance=%q entity_id=%q", descriptor.ID, descriptor.FlowInstance, descriptor.EntityID)
		}
	}
	return nil
}

func (p selectedRunTargetOwnerProjection) pinRoutingDescriptors() ([]runtimepinrouting.Descriptor, error) {
	out := make([]runtimepinrouting.Descriptor, 0, len(p.descriptors))
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		out = append(out, runtimepinrouting.Descriptor{
			ID: descriptor.ID, EntityID: descriptor.EntityID, FlowInstance: descriptor.FlowInstance,
			AddressFields: normalizeDescriptorAddressFields(descriptor.AddressFields),
		})
	}
	return out, nil
}

func (p selectedRunTargetOwnerProjection) resolveSelectedRoute(blueprint events.RouteIdentity) (events.DeliveryTargetOwnership, error) {
	blueprint = blueprint.Normalized()
	if blueprint.Empty() {
		if !p.required {
			return events.DeliveryTargetOwnership{}, nil
		}
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target blueprint is required")
	}
	owners := make(map[events.DeliveryTargetOwnership]struct{})
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
		ownership, err := deliveryTargetOwnershipFromDescriptor(owner, descriptor)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		owners[ownership] = struct{}{}
	}
	if len(owners) != 1 {
		if len(owners) == 0 && !p.required {
			if blueprint.EntityID != "" {
				return events.NewExistingEntityTarget(blueprint)
			}
			return events.NewEntitylessReceiverTarget(blueprint)
		}
		candidates := make([]string, 0, len(owners))
		for owner := range owners {
			route := owner.Route()
			candidates = append(candidates, fmt.Sprintf("%s:%s/%s", owner.Code(), route.FlowInstance, route.EntityID))
		}
		sort.Strings(candidates)
		if len(candidates) == 0 {
			return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner is missing for flow instance %q", blueprint.FlowInstance)
		}
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner is ambiguous for flow instance %q; candidates: %s", blueprint.FlowInstance, strings.Join(candidates, ", "))
	}
	for owner := range owners {
		return owner, nil
	}
	return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner resolution failed")
}

func deliveryTargetOwnershipFromDescriptor(route events.RouteIdentity, descriptor ActiveTargetDescriptor) (events.DeliveryTargetOwnership, error) {
	if descriptor.Materializing {
		return events.NewMaterializingEntityTarget(route)
	}
	return events.NewExistingEntityTarget(route)
}

func (p selectedRunTargetOwnerProjection) targetOwnerCandidates() []runtimepipeline.DeliveryTargetOwnerCandidate {
	out := make([]runtimepipeline.DeliveryTargetOwnerCandidate, 0, len(p.descriptors))
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		out = append(out, runtimepipeline.DeliveryTargetOwnerCandidate{
			Route: events.RouteIdentity{
				FlowID:       runtimeflowidentity.SemanticScopeFromFlowInstanceRef(descriptor.FlowInstance),
				FlowInstance: descriptor.FlowInstance,
				EntityID:     descriptor.EntityID,
			},
			Materializing: descriptor.Materializing,
		})
	}
	return out
}
