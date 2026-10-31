package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// This evidence ends at publication preparation. Neither RoutePlan nor the
// persisted/dispatch command carries its lookup context or planner.
type closedPublicationRoutes struct {
	plan   RoutePlan
	owners *publicationTargetOwnerEvidence
}

type publicationTargetOwnerEvidence struct {
	event              events.Event
	planner            deliveryPlanner
	present            bool
	origins            map[string]events.RouteIdentity
	prospective        pipeline.PreparedWorkflowPublicationState
	context            context.Context
	instances          pipeline.FlowInstanceIndexReader
	agents             map[agentidentity.Identity]ActiveAgentDescriptor
	recordedAgents     map[agentidentity.Identity]ActiveAgentDescriptor
	observedAgents     map[agentidentity.Identity]pipeline.FlowInstanceObservation
	constructingAgents map[agentidentity.Identity]struct{}
	activationOwners   map[events.RouteIdentity]pipeline.FlowInstanceActivationPlan
	agentsAvailable    bool
	candidates         []pipeline.DeliveryTargetOwnerCandidate
	targetsAvailable   bool
	source             semanticview.Source
	required           bool
}

func (p *publicationTargetOwnerEvidence) replacePlanningEvidence(next publicationTargetOwnerEvidence) {
	// A scoped refresh cannot replace the captured publication boundary.
	next.event, next.planner, next.present, next.origins = p.event, p.planner, p.present, p.origins
	*p = next
}

func (p deliveryPlanner) resolvePreparedRoutePlan(projection publicationTargetOwnerEvidence, plan RoutePlan) (RoutePlan, error) {
	var err error
	if p.preparedOwners != nil && len(plan.ActivationPlans) > 0 {
		projection, err = projection.withActivationPlans(plan.ActivationPlans)
		if err != nil {
			return RoutePlan{}, err
		}
	}
	plan, err = projection.resolveRoutePlan(plan)
	if err != nil {
		return RoutePlan{}, err
	}
	if p.preparedOwners != nil {
		var origins map[string]events.RouteIdentity
		if len(plan.ReplyCreations) > 0 {
			origins = make(map[string]events.RouteIdentity, len(plan.ReplyCreations))
		}
		for _, record := range plan.ReplyCreations {
			if err := record.Validate(); err != nil {
				return RoutePlan{}, err
			}
			if record.RequestEventID != p.preparedOwners.event.ID() || record.RunID != p.preparedOwners.event.RunID() {
				return RoutePlan{}, fmt.Errorf("reply creation crosses its admitted publication")
			}
			origins[record.ID] = record.Origin
		}
		projection.instances = nil
		p.preparedOwners.replacePlanningEvidence(projection)
		p.preparedOwners.present = true
		p.preparedOwners.origins = origins
	}
	return plan, nil
}

func (p *publicationTargetOwnerEvidence) validate(ctx context.Context, event events.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || !p.present || p.context == nil {
		return fmt.Errorf("reply admission requires its publication-local target owners")
	}
	if err := p.context.Err(); err != nil {
		return err
	}
	if event.ID() != p.event.ID() || event.RunID() != p.event.RunID() ||
		event.AdmissionClass() != p.event.AdmissionClass() || event.ExecutionMode() != p.event.ExecutionMode() ||
		event.Type() != p.event.Type() || event.Producer() != p.event.Producer() ||
		event.ParentEventID() != p.event.ParentEventID() ||
		event.RoutingSource() != p.event.RoutingSource() || event.SourceRoute() != p.event.SourceRoute() {
		return fmt.Errorf("reply target owners cross their admitted publication")
	}
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	captured, admitted := correlation.SourceArtifactFactFromContext(p.context)
	if !present || !admitted || !fact.Matches(captured) {
		return fmt.Errorf("reply target owners cross their admitted source")
	}
	scope, scoped := p.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
	if !scoped || !scope.lookup.Valid() || scope.lookup.RunID() != event.RunID() || !scope.lookup.SourceFact().Matches(fact) {
		return fmt.Errorf("reply target owners require their compiled instance lookup scope")
	}
	return nil
}

func (p *publicationTargetOwnerEvidence) returnOwner(ctx context.Context, event events.Event, plan *RoutePlan, record replycontext.Record) (events.DeliveryTargetOwnership, error) {
	if err := p.validate(ctx, event); err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	// Ordinary planning has ended its cut; groups are still inside theirs.
	// Use this owner's live reader, never retain the ordinary expired reader.
	p.instances = p.planner.connectPlanner.lifecycle.index
	origin, captured := p.origins[record.ID]
	if !captured || origin != record.Origin || record.RunID != event.RunID() || record.RequestEventID != event.ID() {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("reply origin differs from its prepared publication")
	}
	coordinates, err := p.planner.connectPlanner.exactLookupCoordinates(event.RunID(), []events.RouteIdentity{origin})
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	if len(coordinates) != 1 {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("reply origin requires one exact compiled coordinate")
	}
	scope := p.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
	covered := false
	for _, flow := range scope.lookup.FlowIDs() {
		covered = covered || flow == coordinates[0].Route.ScopeKey
	}
	for _, coordinate := range scope.lookup.Coordinates() {
		covered = covered || coordinate == coordinates[0]
	}
	if !covered {
		flows := make(map[string]struct{})
		for _, flow := range scope.lookup.FlowIDs() {
			flows[flow] = struct{}{}
		}
		coordinates = append(scope.lookup.Coordinates(), coordinates...)
		scope, err = p.planner.connectPlanner.lookupTargetScope(p.context, event.RunID(), flows, coordinates)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		bounded := withSelectedTargetOwnerLookupScope(p.context, scope)
		policy := p.planner.recipientPolicy
		policy.instances = p.planner.connectPlanner.lifecycle.index
		reloaded, err := policy.loadPublicationTargetOwners(bounded)
		p.replacePlanningEvidence(reloaded)
		if err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
	}
	updated, err := p.withActivationPlans(plan.ActivationPlans)
	p.replacePlanningEvidence(updated)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	return p.replyOriginOwner(origin)
}

func (p publicationTargetOwnerEvidence) replyOriginOwner(origin events.RouteIdentity) (events.DeliveryTargetOwnership, error) {
	origin = origin.Normalized()
	if err := p.prospective.ValidateTarget(origin); err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	if origin.Empty() {
		if !p.required {
			return events.DeliveryTargetOwnership{}, nil
		}
		return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target blueprint is required")
	}
	candidates, err := p.decisionTargetCandidates(origin)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	// An explicit reply origin narrows candidates; node admission instead treats
	// a contradictory entity at its exact instance as an error.
	matching := make([]pipeline.DeliveryTargetOwnerCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if origin.FlowInstance != "" && candidate.Route.FlowInstance != origin.FlowInstance || origin.EntityID != "" && candidate.Route.EntityID != origin.EntityID {
			continue
		}
		matching = append(matching, candidate)
	}
	owner, found, err := pipeline.SelectDeliveryTargetOwner(p.source, origin, matching)
	if err != nil || found {
		return owner, err
	}
	if !p.required {
		if origin.EntityID != "" {
			return events.NewExistingEntityTarget(origin)
		}
		return events.NewEntitylessReceiverTarget(origin)
	}
	return events.DeliveryTargetOwnership{}, fmt.Errorf("receiver target owner is missing for flow instance %q", origin.FlowInstance)
}

func (p publicationTargetOwnerEvidence) resolveRoutePlan(plan RoutePlan) (RoutePlan, error) {
	plan = plan.Normalized()
	var err error
	if err = p.resolveNodeTargetOwners(&plan, nil); err != nil {
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
					if refusal, ok := err.(*pipeline.NoHandlerError); ok {
						refusal.FlowInstance = intent.TargetOwnership.Route().FlowInstance
					}
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

func (p publicationTargetOwnerEvidence) resolveNodeTargetOwners(plan *RoutePlan, lookups []pipeline.DeliveryTargetLookup) error {
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
			var lookup pipeline.DeliveryTargetLookup
			if lookups != nil {
				lookup = lookups[index]
			} else {
				var err error
				lookup, err = pipeline.AdmitDeliveryTargetLookup(pipeline.DeliveryTargetOwnershipRequest{
					Source: p.source, Event: plan.Event, Recipient: intent.Recipient, Blueprint: intent.TargetBlueprint,
					Handler: handler,
				})
				if err != nil {
					return err
				}
			}
			candidates, err := p.decisionTargetCandidates(lookup.Coordinate())
			if err != nil {
				return err
			}
			owner, err := pipeline.ClassifyDeliveryTargetOwnership(lookup, candidates)
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

func (p publicationTargetOwnerEvidence) withActivationPlans(plans []pipeline.FlowInstanceActivationPlan) (publicationTargetOwnerEvidence, error) {
	ordered := newOrderedTargetOwnerCandidates(p.candidates)
	agents := make(map[agentidentity.Identity]ActiveAgentDescriptor, len(p.agents))
	for identity, descriptor := range p.agents {
		agents[identity] = descriptor
	}
	constructing := make(map[agentidentity.Identity]struct{}, len(p.constructingAgents))
	for identity := range p.constructingAgents {
		constructing[identity] = struct{}{}
	}
	activationOwners := make(map[events.RouteIdentity]pipeline.FlowInstanceActivationPlan, len(p.activationOwners))
	for identity, plan := range p.activationOwners {
		activationOwners[identity] = plan
	}
	var constructionPlans []pipeline.FlowInstanceActivationPlan
	for _, plan := range plans {
		constructionPlans = append(constructionPlans, plan.ConstructionPlans()...)
	}
	plans = constructionPlans
	for _, plan := range plans {
		normalized, err := plan.Normalized()
		if err != nil {
			return publicationTargetOwnerEvidence{}, fmt.Errorf("normalize selected-run activation owner: %w", err)
		}
		if err := normalized.Validate(); err != nil {
			return publicationTargetOwnerEvidence{}, fmt.Errorf("validate selected-run activation owner: %w", err)
		}
		activationOwners[events.RouteIdentity{FlowID: normalized.Identity.TemplateID,
			FlowInstance: normalized.Identity.InstancePath, EntityID: normalized.Identity.EntityID}] = normalized
		ordered.add(pipeline.DeliveryTargetOwnerCandidate{
			Route: events.RouteIdentity{FlowID: normalized.Identity.TemplateID,
				FlowInstance: normalized.Identity.InstancePath,
				EntityID:     normalized.Identity.EntityID},
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
	p.candidates = ordered.candidates
	return p, nil
}

type publicationTargetOwnerEvidenceKey struct{}

func withPublicationTargetOwners(ctx context.Context, projection publicationTargetOwnerEvidence) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, publicationTargetOwnerEvidenceKey{}, projection)
}

func publicationTargetOwnersFromContext(ctx context.Context) (publicationTargetOwnerEvidence, bool) {
	if ctx == nil {
		return publicationTargetOwnerEvidence{}, false
	}
	projection, ok := ctx.Value(publicationTargetOwnerEvidenceKey{}).(publicationTargetOwnerEvidence)
	return projection, ok
}

func (p deliveryRecipientPolicy) loadPublicationTargetOwners(ctx context.Context) (publicationTargetOwnerEvidence, error) {
	if p.runProposal.Present() {
		event, found := correlation.InboundEventFromContext(ctx)
		fact, present := correlation.SourceArtifactFactFromContext(ctx)
		if !found || !present {
			return publicationTargetOwnerEvidence{}, fmt.Errorf("proposed target planning requires its admitted event and source")
		}
		if err := p.runProposal.Validate(event.RunID(), fact); err != nil {
			return publicationTargetOwnerEvidence{}, err
		}
		return publicationTargetOwnerEvidence{prospective: p.prospective, context: ctx, source: p.semanticSource, required: p.requireTargetOwners, targetsAvailable: true}, nil
	}
	agents, agentsAvailable, err := p.loadActiveAgentDescriptors(ctx)
	if err != nil {
		return publicationTargetOwnerEvidence{}, err
	}
	ordered := newOrderedTargetOwnerCandidates(nil)
	targetsAvailable := false
	if p.loadTargetOwnerCandidates != nil {
		loaded, available, err := p.loadTargetOwnerCandidates(ctx)
		if err != nil {
			return publicationTargetOwnerEvidence{}, err
		}
		targetsAvailable = targetsAvailable || available
		for _, descriptor := range loaded {
			ordered.add(descriptor)
		}
	}
	projection := publicationTargetOwnerEvidence{
		prospective: p.prospective,
		context:     ctx,
		instances:   p.instances,
		agents:      agents, agentsAvailable: agentsAvailable,
		candidates: ordered.candidates, targetsAvailable: targetsAvailable,
		source: p.semanticSource, required: p.requireTargetOwners,
	}
	if projection.required {
		if err := projection.validatePlanningShape(); err != nil {
			return publicationTargetOwnerEvidence{}, err
		}
	}
	return projection.withObservedAgentConfigurations()
}

func (p publicationTargetOwnerEvidence) validatePlanningShape() error {
	for _, descriptor := range p.candidates {
		descriptor = normalizedTargetOwnerCandidate(descriptor)
		if descriptor.Route.FlowInstance == "" || descriptor.Route.EntityID == "" {
			return fmt.Errorf("selected-run target owner is missing exact entity identity or flow-instance identity: flow_instance=%q entity_id=%q", descriptor.Route.FlowInstance, descriptor.Route.EntityID)
		}
	}
	return nil
}
