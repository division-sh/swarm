package bus

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type selectedRunTargetOwnerProjection struct {
	prospective        runtimepipeline.PreparedWorkflowPublicationState
	context            context.Context
	instances          runtimepipeline.FlowInstanceIndexReader
	agents             map[agentidentity.Identity]ActiveAgentDescriptor
	recordedAgents     map[agentidentity.Identity]ActiveAgentDescriptor
	observedAgents     map[agentidentity.Identity]runtimepipeline.FlowInstanceObservation
	constructingAgents map[agentidentity.Identity]struct{}
	activationOwners   map[events.RouteIdentity]runtimepipeline.FlowInstanceActivationPlan
	agentsAvailable    bool
	descriptors        []ActiveTargetDescriptor
	targetsAvailable   bool
	source             semanticview.Source
	required           bool
}

func (p selectedRunTargetOwnerProjection) resolveRoutePlan(plan RoutePlan) (RoutePlan, error) {
	plan = plan.Normalized()
	var err error
	if err = p.resolveNodeTargetOwners(&plan); err != nil {
		return RoutePlan{}, err
	}
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		if intent.Recipient.IsAgent() {
			if intent.TargetOwnership.Empty() {
				admitted, err := p.admitAgentTarget(plan.Event, *intent)
				if err != nil {
					return RoutePlan{}, fmt.Errorf("admit agent delivery target for %s: %w", intent.Recipient.ID(), err)
				}
				*intent = admitted
			}
			if err := p.validateAdmittedAgentTarget(plan.Event, *intent); err != nil {
				return RoutePlan{}, err
			}
			if err := validateAgentLifecycleTargetOwnership(*intent); err != nil {
				return RoutePlan{}, err
			}
			continue
		}
		if !intent.TargetOwnership.Empty() {
			if err := p.prospective.ValidateTarget(intent.TargetOwnership.Route()); err != nil {
				return RoutePlan{}, err
			}
			if !p.prospective.Empty() && !intent.TargetOwnership.EntitylessReceiver() && intent.TargetOwnership.Route() == p.prospective.Candidate().Route {
				if err := p.prospective.Candidate().Availability.Validate(p.source, intent.TargetOwnership.Route().FlowID); err != nil {
					return RoutePlan{}, err
				}
			}
			if err := intent.TargetOwnership.Validate(); err != nil {
				return RoutePlan{}, fmt.Errorf("validate admitted delivery target for %s: %w", intent.Recipient.ID(), err)
			}
			if intent.TargetBlueprint.Normalized() != intent.TargetOwnership.Route() {
				return RoutePlan{}, fmt.Errorf("validate admitted delivery target for %s: blueprint and typed owner disagree: blueprint=%#v owner=%s %#v", intent.Recipient.ID(), intent.TargetBlueprint.Normalized(), intent.TargetOwnership.Code(), intent.TargetOwnership.Route())
			}
			if err := validateAgentLifecycleTargetOwnership(*intent); err != nil {
				return RoutePlan{}, fmt.Errorf("validate admitted delivery target for %s: %w", intent.Recipient.ID(), err)
			}
			continue
		}
		handler := intent.Handler
		if handler.Empty() {
			return RoutePlan{}, fmt.Errorf("resolve delivery target handler for %s: route intent has no exact admitted handler", intent.Recipient.ID())
		}
		return RoutePlan{}, fmt.Errorf("resolve delivery target for %s: node intent escaped canonical preclassification", intent.Recipient.ID())
	}
	if err := p.bindReceiverMaterializations(&plan); err != nil {
		return RoutePlan{}, err
	}
	if err := completeReceiverInitializations(&plan); err != nil {
		return RoutePlan{}, err
	}
	if err := validateConnectIntentReceivers(plan, p.source); err != nil {
		return RoutePlan{}, err
	}
	if err := events.ValidateReceiverMaterializations(plan.Event, plan.DeliveryRoutes()); err != nil {
		return RoutePlan{}, err
	}
	return plan.Normalized(), nil
}

func validateAgentLifecycleTargetOwnership(intent RoutePlanDeliveryIntent) error {
	if err := intent.AgentLifecycle.validate(); err != nil {
		return err
	}
	if !intent.AgentLifecycle.pending() {
		return nil
	}
	owner := intent.TargetOwnership
	if owner.Empty() {
		return fmt.Errorf("agent lifecycle admission requires exact target ownership")
	}
	if intent.Initialization.Empty() {
		return fmt.Errorf("pending agent lifecycle requires its independent construction dependency")
	}
	return intent.Initialization.ValidateRoute(intent.deliveryRoute())
}

func (p selectedRunTargetOwnerProjection) resolveNodeTargetOwners(plan *RoutePlan) error {
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		if intent.Recipient.IsAgent() {
			continue
		}
		if err := p.prospective.ValidateTarget(intent.TargetBlueprint); err != nil {
			return err
		}
		if intent.TargetOwnership.Empty() {
			handler := intent.Handler
			if handler.Empty() {
				return fmt.Errorf("resolve delivery target handler for %s: route intent has no exact admitted handler", intent.Recipient.ID())
			}
			request := runtimepipeline.DeliveryTargetOwnershipRequest{
				Source: p.source, Event: plan.Event, Recipient: intent.Recipient, Blueprint: intent.TargetBlueprint,
				Handler: handler,
			}
			target, err := runtimepipeline.AdmitDeliveryTargetLookupCoordinate(request)
			if err != nil {
				return err
			}
			request.Candidates, err = p.decisionTargetCandidates(target)
			if err != nil {
				return err
			}
			owner, err := runtimepipeline.ClassifyDeliveryTargetOwnership(request)
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

func (p selectedRunTargetOwnerProjection) withActivationPlans(plans []runtimepipeline.FlowInstanceActivationPlan) (selectedRunTargetOwnerProjection, error) {
	ordered := newOrderedActiveTargetDescriptors(p.descriptors)
	agents := make(map[agentidentity.Identity]ActiveAgentDescriptor, len(p.agents))
	for identity, descriptor := range p.agents {
		agents[identity] = descriptor
	}
	constructing := make(map[agentidentity.Identity]struct{}, len(p.constructingAgents))
	for identity := range p.constructingAgents {
		constructing[identity] = struct{}{}
	}
	activationOwners := make(map[events.RouteIdentity]runtimepipeline.FlowInstanceActivationPlan, len(p.activationOwners))
	for identity, plan := range p.activationOwners {
		activationOwners[identity] = plan
	}
	var constructionPlans []runtimepipeline.FlowInstanceActivationPlan
	for _, plan := range plans {
		constructionPlans = append(constructionPlans, plan.ConstructionPlans()...)
	}
	plans = constructionPlans
	for _, plan := range plans {
		normalized, err := plan.Normalized()
		if err != nil {
			return selectedRunTargetOwnerProjection{}, fmt.Errorf("normalize selected-run activation owner: %w", err)
		}
		if err := normalized.Validate(); err != nil {
			return selectedRunTargetOwnerProjection{}, fmt.Errorf("validate selected-run activation owner: %w", err)
		}
		activationOwners[events.RouteIdentity{FlowID: normalized.Identity.TemplateID,
			FlowInstance: normalized.Identity.InstancePath, EntityID: normalized.Identity.EntityID}] = normalized
		ordered.add(ActiveTargetDescriptor{
			FlowID:        normalized.Identity.TemplateID,
			ID:            normalized.Identity.InstanceID,
			FlowInstance:  normalized.Identity.InstancePath,
			EntityID:      normalized.Identity.EntityID,
			Materializing: true,
		})
		for _, expected := range normalized.Readiness.Agents {
			identity := expected.Identity.Normalize()
			agents[identity] = ActiveAgentDescriptor{Identity: identity, EntityID: expected.EntityID}
			constructing[identity] = struct{}{}
		}
		p.agentsAvailable = true
		p.targetsAvailable = true
	}
	p.agents = agents
	p.constructingAgents = constructing
	p.activationOwners = activationOwners
	p.descriptors = ordered.descriptors
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
	if p.runProposal.Present() {
		event, found := runtimecorrelation.InboundEventFromContext(ctx)
		fact, present := runtimecorrelation.SourceArtifactFactFromContext(ctx)
		if !found || !present {
			return selectedRunTargetOwnerProjection{}, fmt.Errorf("proposed target planning requires its admitted event and source")
		}
		if err := p.runProposal.Validate(event.RunID(), fact); err != nil {
			return selectedRunTargetOwnerProjection{}, err
		}
		return selectedRunTargetOwnerProjection{prospective: p.prospective, context: ctx, source: p.semanticSource, required: p.requireTargetOwners, targetsAvailable: true}, nil
	}
	agents, agentsAvailable, err := p.loadActiveAgentDescriptors(ctx)
	if err != nil {
		return selectedRunTargetOwnerProjection{}, err
	}
	ordered := newOrderedActiveTargetDescriptors(nil)
	targetsAvailable := false
	if p.loadActiveTargetDescriptors != nil {
		loaded, available, err := p.loadActiveTargetDescriptors(ctx)
		if err != nil {
			return selectedRunTargetOwnerProjection{}, err
		}
		targetsAvailable = targetsAvailable || available
		for _, descriptor := range loaded {
			ordered.add(descriptor)
		}
	}
	projection := selectedRunTargetOwnerProjection{
		prospective: p.prospective,
		context:     ctx,
		instances:   p.instances,
		agents:      agents, agentsAvailable: agentsAvailable,
		descriptors: ordered.descriptors, targetsAvailable: targetsAvailable,
		source: p.semanticSource, required: p.requireTargetOwners,
	}
	if projection.required {
		if err := projection.validate(); err != nil {
			return selectedRunTargetOwnerProjection{}, err
		}
	}
	return projection.withObservedAgentConfigurations()
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

func (p selectedRunTargetOwnerProjection) resolveSelectedRoute(blueprint events.RouteIdentity) (events.DeliveryTargetOwnership, error) {
	blueprint = blueprint.Normalized()
	if err := p.prospective.ValidateTarget(blueprint); err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	if blueprint.Empty() {
		if !p.required {
			return events.DeliveryTargetOwnership{}, nil
		}
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target blueprint is required")
	}
	owners := make(map[events.DeliveryTargetOwnership]struct{})
	candidates, err := p.decisionTargetCandidates(blueprint)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	for _, candidate := range candidates {
		if blueprint.FlowInstance != "" && candidate.Route.FlowInstance != blueprint.FlowInstance {
			continue
		}
		if blueprint.EntityID != "" && candidate.Route.EntityID != blueprint.EntityID {
			continue
		}
		if !candidate.Materializing {
			if err := candidate.Availability.Validate(p.source, blueprint.FlowID); err != nil {
				return events.DeliveryTargetOwnership{}, err
			}
		}
		owner := blueprint
		owner.EntityID = candidate.Route.EntityID
		ownership, err := deliveryTargetOwnershipFromDescriptor(owner, ActiveTargetDescriptor{Materializing: candidate.Materializing})
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
	prospective := p.prospective.Candidate()
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		if !p.prospective.Empty() && descriptor.FlowInstance == prospective.Route.FlowInstance && descriptor.EntityID == prospective.Route.EntityID {
			continue
		}
		out = append(out, runtimepipeline.DeliveryTargetOwnerCandidate{
			Route: events.RouteIdentity{
				FlowID:       descriptor.FlowID,
				FlowInstance: descriptor.FlowInstance,
				EntityID:     descriptor.EntityID,
			},
			Materializing: descriptor.Materializing,
			Availability:  descriptor.Availability,
		})
	}
	if !p.prospective.Empty() {
		out = append(out, prospective)
	}
	return out
}
