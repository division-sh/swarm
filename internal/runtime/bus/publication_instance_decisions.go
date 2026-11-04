package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (p publicationTargetOwnerEvidence) requireCurrentSourceLife(event events.Event) error {
	source := event.RoutingSource()
	if source.Life() == 0 || !p.required {
		return nil
	}
	if p.context == nil || p.source == nil {
		return fmt.Errorf("instance publication requires its exact source-life observation")
	}
	route := source.Route()
	if source.Kind() == events.RoutingSourceRoot {
		route.FlowID, route.FlowInstance = semanticview.RootExecutionFlowID(p.source), event.RunID()
	}
	if publicationSourceMatches(route, p.prospective.Candidate().Route) && source.Life() == p.prospective.Candidate().Life {
		fact, _ := correlation.SourceArtifactFactFromContext(p.context)
		return p.prospective.ValidatePublication(fact, event)
	}
	selected, err := p.sourceLifeObservation(route)
	if err != nil {
		return err
	}
	if selected.Valid() {
		return p.requireObservedSourceLife(selected, source.Life())
	}
	return p.requireConstructedSourceLife(event, route)
}

func publicationSourceMatches(route, candidate events.RouteIdentity) bool {
	return route.FlowID == candidate.FlowID && route.FlowInstance == candidate.FlowInstance && (route.EntityID == "" || route.EntityID == candidate.EntityID)
}

func (p publicationTargetOwnerEvidence) sourceLifeObservation(route events.RouteIdentity) (pipeline.FlowInstanceObservation, error) {
	scope, _ := p.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
	var selected pipeline.FlowInstanceObservation
	for _, listed := range scope.observations {
		identity := listed.Identity()
		if !publicationSourceMatches(route, events.RouteIdentity{FlowID: identity.TemplateID, FlowInstance: identity.InstancePath, EntityID: identity.EntityID}) {
			continue
		}
		if selected.Valid() {
			return pipeline.FlowInstanceObservation{}, fmt.Errorf("publication source has ambiguous instance observations")
		}
		selected = listed
	}
	return selected, nil
}

func (p publicationTargetOwnerEvidence) requireObservedSourceLife(selected pipeline.FlowInstanceObservation, captured int64) error {
	current, err := p.decisionObservation(selected)
	if err != nil {
		return err
	}
	instance, err := current.WorkflowInstance()
	if err != nil {
		return err
	}
	if instance.Life != captured {
		return pipeline.InstanceRestarted(current.Owner(), captured, instance.Life)
	}
	return nil
}

func (p publicationTargetOwnerEvidence) requireConstructedSourceLife(event events.Event, route events.RouteIdentity) error {
	found := false
	for candidate, plan := range p.activationOwners {
		if publicationSourceMatches(route, candidate) && plan.Instance.Life == event.RoutingSource().Life() {
			if found {
				return fmt.Errorf("publication source has ambiguous admitted constructors")
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: event.RunID(), Route: plan.Identity.Route()}
			if err := owner.Validate(); err != nil || plan.Readiness.RunID != owner.RunID {
				return fmt.Errorf("publication source construction crosses its exact run")
			}
			found = true
		}
	}
	if found {
		return nil
	}
	return fmt.Errorf("publication source is absent from its admitted instance observations")
}

func workflowInstanceTargetAvailability(instance pipeline.WorkflowInstance) pipeline.DeliveryTargetAvailability {
	stage := ""
	if instance.StageDefined {
		stage = instance.CurrentState
	}
	return pipeline.NewDeliveryTargetAvailability(stage, instance.Status, !instance.TerminatedAt.IsZero())
}

// Inventory narrows immutable coordinates. Only this exact read binds a
// mutable decision to the live preparation owner; neither escapes in a route.
func (p publicationTargetOwnerEvidence) decisionObservation(listed pipeline.FlowInstanceObservation) (pipeline.FlowInstanceObservation, error) {
	if p.context == nil || p.instances == nil {
		return pipeline.FlowInstanceObservation{}, fmt.Errorf("mutable instance decision requires its live canonical reader")
	}
	if err := p.context.Err(); err != nil {
		return pipeline.FlowInstanceObservation{}, err
	}
	fact, found := correlation.SourceArtifactFactFromContext(p.context)
	scope, scoped := p.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
	if !found || !fact.Matches(listed.SourceFact()) || !scoped || !scope.lookup.Valid() ||
		scope.lookup.RunID() != listed.Owner().RunID || !scope.lookup.SourceFact().Matches(fact) {
		return pipeline.FlowInstanceObservation{}, fmt.Errorf("mutable instance decision crosses its admitted source")
	}
	request, err := pipeline.NewExactFlowInstanceLookup(p.source, fact, listed.Owner())
	if err != nil {
		return pipeline.FlowInstanceObservation{}, err
	}
	observed, found, err := p.instances.LookupFlowInstance(p.context, request)
	if err != nil {
		return pipeline.FlowInstanceObservation{}, err
	}
	if !found || observed.Identity() != listed.Identity() || observed.InstanceKey() != listed.InstanceKey() {
		return pipeline.FlowInstanceObservation{}, &pipeline.FlowInstanceConstructionCorruption{
			RunID: request.RunID(), FlowID: request.FlowID(), InstancePath: request.ExactPath(),
			Cause: fmt.Errorf("exact decision read disagrees with its admitted inventory coordinate"),
		}
	}
	if err := observed.ValidateSelection(request); err != nil {
		return pipeline.FlowInstanceObservation{}, err
	}
	return observed, nil
}

func (p publicationTargetOwnerEvidence) decisionTargetCandidate(descriptor pipeline.DeliveryTargetOwnerCandidate) (pipeline.DeliveryTargetOwnerCandidate, error) {
	if plan, present := p.activationOwners[descriptor.Route]; present && plan.Restart != nil {
		return descriptor, nil // The same transaction must discharge this restart.
	}
	if descriptor.Materializing || p.context == nil {
		return descriptor, nil
	}
	scope, _ := p.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
	var selected pipeline.FlowInstanceObservation
	for _, listed := range scope.observations {
		identity := listed.Identity()
		if identity.InstancePath != descriptor.Route.FlowInstance || identity.EntityID != descriptor.Route.EntityID {
			continue
		}
		if selected.Valid() {
			return descriptor, fmt.Errorf("mutable target has ambiguous inventory coordinates")
		}
		selected = listed
	}
	if !selected.Valid() {
		return descriptor, fmt.Errorf("mutable target is absent from its admitted inventory")
	}
	observed, err := p.decisionObservation(selected)
	if err != nil {
		return descriptor, err
	}
	instance, err := observed.WorkflowInstance()
	if err != nil {
		return descriptor, err
	}
	descriptor.Availability = workflowInstanceTargetAvailability(instance)
	descriptor.Life = instance.Life
	return descriptor, nil
}

func (p publicationTargetOwnerEvidence) decisionTargetCandidates(target events.RouteIdentity) ([]pipeline.DeliveryTargetOwnerCandidate, error) {
	out := make([]pipeline.DeliveryTargetOwnerCandidate, 0, len(p.candidates)+1)
	prospective := p.prospective.Candidate()
	for _, candidate := range p.candidates {
		if candidate.Route.FlowInstance == target.FlowInstance && (target.EntityID == "" || candidate.Route.EntityID == target.EntityID) {
			var err error
			candidate, err = p.decisionTargetCandidate(candidate)
			if err != nil {
				return nil, err
			}
		}
		candidate = normalizedTargetOwnerCandidate(candidate)
		if !p.prospective.Empty() && candidate.Route.FlowInstance == prospective.Route.FlowInstance && candidate.Route.EntityID == prospective.Route.EntityID {
			continue
		}
		out = append(out, candidate)
	}
	if !p.prospective.Empty() {
		out = append(out, prospective)
	}
	return out, nil
}

// Every candidate, including a currently ineligible one, is read after identity
// narrowing and before eligibility. Other instances' progress is not a decision.
func (p publicationTargetOwnerEvidence) decisionAgentDescriptor(identity agentidentity.Identity) (ActiveAgentDescriptor, bool, bool, error) {
	listed, candidate := p.observedAgents[identity]
	if !candidate {
		descriptor, found := p.agents[identity]
		_, pending := p.constructingAgents[identity]
		return descriptor, found, pending, nil
	}
	observed, err := p.decisionObservation(listed)
	if err != nil {
		return ActiveAgentDescriptor{}, false, false, err
	}
	active, registered := p.recordedAgents[identity]
	readiness, found := observed.Readiness()
	if !found || !readiness.Eligible() {
		return active, registered, false, nil
	}
	for _, expected := range readiness.Plan.Agents {
		if expected.Identity.Normalize() != identity {
			continue
		}
		if registered && active.EntityID != expected.EntityID {
			return ActiveAgentDescriptor{}, false, false, fmt.Errorf("agent configuration contradicts exact desired attachment entity")
		}
		if readiness.Phase == pipeline.FlowAttachmentReady {
			return active, registered, false, nil
		}
		if !registered {
			active = ActiveAgentDescriptor{Identity: identity, EntityID: expected.EntityID}
		}
		return active, true, true, nil
	}
	return ActiveAgentDescriptor{}, false, false, fmt.Errorf("exact agent decision lost its immutable desired configuration")
}
