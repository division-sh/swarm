package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (p selectedRunTargetOwnerProjection) withObservedAgentConfigurations() (selectedRunTargetOwnerProjection, error) {
	if p.context == nil {
		return p, nil
	}
	scope, _ := p.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
	agents := make(map[agentidentity.Identity]ActiveAgentDescriptor, len(p.agents))
	for identity, descriptor := range p.agents {
		agents[identity] = descriptor
	}
	pending := make(map[agentidentity.Identity]struct{})
	for _, observation := range scope.observations {
		readiness, found := observation.Readiness()
		if !found || !readiness.Eligible() {
			continue
		}
		for _, expected := range readiness.Plan.Agents {
			identity := expected.Identity.Normalize()
			if active, present := agents[identity]; present && active.EntityID != expected.EntityID {
				return p, fmt.Errorf("agent configuration contradicts exact desired attachment entity")
			}
			if readiness.Phase != pipeline.FlowAttachmentReady {
				if _, present := agents[identity]; !present {
					agents[identity] = ActiveAgentDescriptor{Identity: identity, EntityID: expected.EntityID}
				}
				pending[identity] = struct{}{}
			}
		}
	}
	p.agents, p.constructingAgents = agents, pending
	return p, nil
}

func (p selectedRunTargetOwnerProjection) validateAdmittedAgentTarget(event events.Event, intent RoutePlanDeliveryIntent) error {
	if intent.AgentIdentity.Validate() != nil || intent.AgentIdentity.RunID != event.RunID() || intent.AgentIdentity.AgentID() != intent.Recipient.ID() {
		return fmt.Errorf("admitted agent target has a foreign publication identity")
	}
	if err := intent.TargetOwnership.Validate(); err != nil {
		return err
	}
	if intent.TargetBlueprint != intent.TargetOwnership.Route() {
		return fmt.Errorf("admitted agent blueprint and typed owner disagree")
	}
	if !intent.TargetOwnership.Empty() {
		if err := p.validateAgentTargetCoordinates(intent.AgentIdentity, intent.TargetOwnership.Route()); err != nil {
			return err
		}
	}
	if descriptor, present := p.agents[intent.AgentIdentity]; present && !intent.TargetOwnership.Empty() && descriptor.EntityID != intent.TargetOwnership.Route().EntityID {
		return fmt.Errorf("admitted target contradicts exact agent configuration entity")
	}
	if !intent.Initialization.Empty() {
		return intent.Initialization.ValidateRoute(intent.deliveryRoute())
	}
	return nil
}

// Agent state ownership comes from configuration; the separately selected flow
// supplies construction/readiness, never an entity for an entityless actor.
func (p selectedRunTargetOwnerProjection) admitAgentTarget(event events.Event, intent RoutePlanDeliveryIntent) (RoutePlanDeliveryIntent, error) {
	identity := intent.AgentIdentity.Normalize()
	if identity.Validate() != nil || identity.RunID != event.RunID() || identity.AgentID() != intent.Recipient.ID() {
		return intent, fmt.Errorf("agent delivery requires its exact publication run and recipient identity")
	}
	descriptor, found := p.agents[identity]
	if !found {
		if !p.required && intent.TargetBlueprint.Empty() && intent.ReceiverBlueprint.Empty() {
			return intent, nil
		}
		return intent, fmt.Errorf("exact admitted agent configuration is missing for %s", identity.Description())
	}
	if descriptor.Identity.Normalize() != identity {
		return intent, fmt.Errorf("agent configuration identity disagrees with recipient")
	}
	descriptor = descriptor.Normalized()
	dependency, selected, err := p.agentConstructionDependency(event, identity, intent.ReceiverBlueprint)
	if err != nil {
		return intent, err
	}
	route := intent.TargetBlueprint.Normalized()
	if route.Empty() && selected.Empty() {
		return intent, nil
	}
	if route.Empty() {
		route = selected
		if route.Empty() && identity.Route.Presence == agentidentity.RoutePresent {
			route = events.RouteIdentity{FlowID: identity.Route.ScopeKey, FlowInstance: identity.Route.InstancePath}
		}
		if route.Empty() && p.source != nil {
			root, err := semanticview.AdmitRootExecutionCoordinate(p.source, identity.RunID)
			if err != nil {
				return intent, err
			}
			route = events.RouteIdentity{FlowID: root.FlowID(), FlowInstance: root.RunID()}
		}
		// Selected construction is context, not a caller's actor-entity request.
		route.EntityID = descriptor.EntityID
	} else if route.EntityID != descriptor.EntityID {
		if descriptor.EntityID == "" {
			return intent, fmt.Errorf("target entity %q disagrees with entityless active agent", route.EntityID)
		}
		return intent, fmt.Errorf("target entity %q disagrees with exact active agent entity %q", route.EntityID, descriptor.EntityID)
	}
	if route.Empty() {
		return intent, nil
	}
	if err := p.validateAgentTargetCoordinates(identity, route); err != nil {
		return intent, err
	}
	if !selected.Empty() && (route.FlowID != selected.FlowID || route.FlowInstance != selected.FlowInstance) {
		return intent, fmt.Errorf("actor target contradicts selected flow construction")
	}
	route.EntityID = descriptor.EntityID
	switch {
	case descriptor.EntityID == "":
		intent.TargetOwnership, err = events.NewEntitylessReceiverTarget(route)
	case !dependency.Empty() && dependency.ConstructionTarget().MaterializingEntity() && selected.EntityID == descriptor.EntityID:
		intent.TargetOwnership, err = events.NewMaterializingEntityTarget(route)
	default:
		intent.TargetOwnership, err = events.NewExistingEntityTarget(route)
	}
	if err != nil {
		return intent, err
	}
	intent.TargetBlueprint, intent.Initialization = intent.TargetOwnership.Route(), dependency
	if !selected.Empty() {
		intent.ReceiverBlueprint = selected
	}
	if !dependency.Empty() {
		if err := dependency.ValidateRoute(intent.deliveryRoute()); err != nil {
			return intent, err
		}
	}
	return intent, nil
}

func (p selectedRunTargetOwnerProjection) validateAgentTargetCoordinates(identity agentidentity.Identity, route events.RouteIdentity) error {
	if identity.Route.Presence == agentidentity.RoutePresent {
		if route.FlowInstance != identity.Route.InstancePath {
			return fmt.Errorf("target disagrees with exact agent identity instance")
		}
		if flowidentity.ScopeKey(p.source, route.FlowID) != identity.Route.ScopeKey {
			return fmt.Errorf("target disagrees with exact agent identity scope")
		}
		return nil
	}
	if route.FlowInstance != identity.RunID {
		return fmt.Errorf("target disagrees with exact root agent identity")
	}
	if p.source != nil {
		root, err := semanticview.AdmitRootExecutionCoordinate(p.source, identity.RunID)
		if err != nil || !root.Matches(route.FlowID, route.FlowInstance) {
			return fmt.Errorf("target disagrees with exact root agent identity")
		}
	}
	return nil
}

func (p selectedRunTargetOwnerProjection) agentConstructionDependency(event events.Event, actor agentidentity.Identity, requested events.RouteIdentity) (events.ReceiverInitialization, events.RouteIdentity, error) {
	var selectedPlan pipeline.FlowInstanceActivationPlan
	var plannedTarget events.RouteIdentity
	for target, plan := range p.activationOwners {
		owner := flowidentity.RunScopedFlowInstance{RunID: plan.Readiness.RunID, Route: plan.Identity.Route()}
		if !owner.MatchesAgentRoute(actor) {
			continue
		}
		if !plannedTarget.Empty() {
			return events.ReceiverInitialization{}, events.RouteIdentity{}, fmt.Errorf("agent has ambiguous planned construction")
		}
		selectedPlan, plannedTarget = plan, target
	}
	if !plannedTarget.Empty() {
		plan, target := selectedPlan, plannedTarget
		if !requested.Empty() && requested.Normalized() != target {
			return events.ReceiverInitialization{}, events.RouteIdentity{}, fmt.Errorf("agent receiver contradicts planned construction")
		}
		if plan.CreatingInput.EventID != event.ID() {
			return events.ReceiverInitialization{}, events.RouteIdentity{}, fmt.Errorf("agent construction belongs to another publication")
		}
		receiver, err := plan.Identity.Route().AgentIdentityRoute()
		if err != nil {
			return events.ReceiverInitialization{}, events.RouteIdentity{}, err
		}
		dependency, err := events.AdmitFlowReceiverInitialization(event, events.MustMaterializingEntityTarget(target), receiver)
		return dependency, target, err
	}
	if p.context != nil {
		scope, _ := p.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
		var selected pipeline.FlowInstanceObservation
		for _, observation := range scope.observations {
			if !observation.Owner().MatchesAgentRoute(actor) {
				continue
			}
			if selected.Valid() {
				return events.ReceiverInitialization{}, events.RouteIdentity{}, fmt.Errorf("agent has ambiguous recorded construction")
			}
			selected = observation
		}
		if selected.Valid() {
			observation := selected
			identity := observation.Identity()
			target := events.RouteIdentity{FlowID: identity.TemplateID, FlowInstance: identity.InstancePath, EntityID: identity.EntityID}
			if !requested.Empty() && requested.Normalized() != target {
				return events.ReceiverInitialization{}, events.RouteIdentity{}, fmt.Errorf("agent receiver contradicts recorded construction")
			}
			receipt, native, err := observation.NativeConstruction()
			if err != nil {
				return events.ReceiverInitialization{}, events.RouteIdentity{}, err
			}
			if !native {
				// Historical construction is already admitted by its fixed-cut owner;
				// it must not be converted into a fresh native creating publication.
				return events.ReceiverInitialization{}, target, nil
			}
			receiver, err := identity.Route().AgentIdentityRoute()
			if err != nil {
				return events.ReceiverInitialization{}, events.RouteIdentity{}, err
			}
			dependency, err := events.AdmitStoredFlowReceiverInitialization(actor.RunID, receipt.CreatingInput.EventID, events.MustExistingEntityTarget(target), receiver)
			return dependency, target, err
		}
	}
	if p.source != nil && actor.Name.Source == agentidentity.NameSourceDeclared {
		return events.ReceiverInitialization{}, events.RouteIdentity{}, fmt.Errorf("declared agent has no exact admitted flow construction")
	}
	return events.ReceiverInitialization{}, events.RouteIdentity{}, nil
}
