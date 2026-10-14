package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// Inventory narrows immutable coordinates. Only this exact read binds a
// mutable decision to the live preparation owner; neither escapes in a route.
func (p selectedRunTargetOwnerProjection) decisionObservation(listed pipeline.FlowInstanceObservation) (pipeline.FlowInstanceObservation, error) {
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

func (p selectedRunTargetOwnerProjection) decisionTargetDescriptor(descriptor ActiveTargetDescriptor) (ActiveTargetDescriptor, error) {
	if descriptor.Materializing || p.context == nil {
		return descriptor, nil
	}
	scope, _ := p.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
	var selected pipeline.FlowInstanceObservation
	for _, listed := range scope.observations {
		identity := listed.Identity()
		if identity.InstancePath != descriptor.FlowInstance || identity.EntityID != descriptor.EntityID {
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
	stage := ""
	if instance.StageDefined {
		stage = instance.CurrentState
	}
	descriptor.Availability = pipeline.NewDeliveryTargetAvailability(stage, instance.Status, !instance.TerminatedAt.IsZero())
	return descriptor, nil
}

func (p selectedRunTargetOwnerProjection) decisionTargetCandidates(target events.RouteIdentity) ([]pipeline.DeliveryTargetOwnerCandidate, error) {
	bound := p
	bound.descriptors = append([]ActiveTargetDescriptor(nil), p.descriptors...)
	for i, descriptor := range bound.descriptors {
		if descriptor.FlowInstance != target.FlowInstance || target.EntityID != "" && descriptor.EntityID != target.EntityID {
			continue
		}
		var err error
		bound.descriptors[i], err = p.decisionTargetDescriptor(descriptor)
		if err != nil {
			return nil, err
		}
	}
	return bound.targetOwnerCandidates(), nil
}

// Every candidate, including a currently ineligible one, is read after identity
// narrowing and before eligibility. Other instances' progress is not a decision.
func (p selectedRunTargetOwnerProjection) decisionAgentDescriptor(identity agentidentity.Identity) (ActiveAgentDescriptor, bool, bool, error) {
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
