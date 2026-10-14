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
	if runtimeCreatedTargetlessAgentIntent(intent) {
		return intent, nil
	}
	dependency, selected, err := p.agentConstructionDependency(event, identity, intent.ReceiverBlueprint)
	if err != nil {
		return intent, err
	}
	route := intent.TargetBlueprint.Normalized()
	if selector := explicitRootPublicationTarget(event); !selector.Empty() && route == selector && identity.Name.Source == agentidentity.NameSourceDeclared && !dependency.Empty() {
		if !publicSelectorMatchesConstruction(selector, selected) {
			return intent, fmt.Errorf("public selector contradicts exact agent construction")
		}
		route = selected
		route.EntityID = descriptor.EntityID
	} else if route.Empty() && selected.Empty() {
		return intent, nil
	} else if route.Empty() {
		route = selected
		// Selected construction is context, not a caller's actor-entity request.
		route.EntityID = descriptor.EntityID
	} else if route.EntityID != descriptor.EntityID {
		if descriptor.EntityID == "" {
			return intent, fmt.Errorf("target entity %q disagrees with entityless active agent", route.EntityID)
		}
		return intent, fmt.Errorf("target entity %q disagrees with exact active agent entity %q", route.EntityID, descriptor.EntityID)
	}
	if err := p.validateAgentTargetCoordinates(identity, route); err != nil {
		return intent, err
	}
	if !selected.Empty() && (route.FlowID != selected.FlowID || route.FlowInstance != selected.FlowInstance) {
		return intent, fmt.Errorf("actor target contradicts selected flow construction")
	}
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

func runtimeCreatedTargetlessAgentIntent(intent RoutePlanDeliveryIntent) bool {
	return intent.AgentIdentity.Name.Source == agentidentity.NameSourceRuntimeCreated &&
		intent.TargetBlueprint.Empty() && intent.ReceiverBlueprint.Empty() &&
		intent.ConnectPlan.Empty() && intent.ConnectClaim.Empty()
}

func (p selectedRunTargetOwnerProjection) agentRecipientTarget(event events.Event, descriptor ActiveAgentDescriptor, singular events.RouteIdentity, targets []events.RouteIdentity, root semanticview.RootExecutionCoordinate) (events.RouteIdentity, bool, error) {
	selector := explicitRootPublicationTarget(event)
	if selector.Empty() || descriptor.Identity.Name.Source != agentidentity.NameSourceDeclared {
		target, found := deliveryTargetForDescriptor(descriptor, singular, targets, root)
		return target, found, nil
	}
	path := descriptor.Identity.Route.InstancePath
	if descriptor.Identity.Route.Presence == agentidentity.RouteRoot {
		path = descriptor.Identity.RunID
	}
	if selector.FlowInstance != path {
		return events.RouteIdentity{}, false, nil
	}
	dependency, selected, err := p.agentConstructionDependency(event, descriptor.Identity, events.RouteIdentity{})
	if err != nil {
		return events.RouteIdentity{}, false, err
	}
	if dependency.Empty() {
		// Admitted historical actors retain direct target semantics; absent native
		// evidence is rejected by the construction owner, not rescued here.
		target, found := deliveryTargetForDescriptor(descriptor, singular, targets, root)
		return target, found, nil
	}
	return selector, publicSelectorMatchesConstruction(selector, selected), nil
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
			return admitObservedAgentConstructionDependency(actor.RunID, selected, requested)
		}
	}
	if p.source != nil && actor.Name.Source == agentidentity.NameSourceDeclared {
		return events.ReceiverInitialization{}, events.RouteIdentity{}, fmt.Errorf("declared agent has no exact admitted flow construction")
	}
	return events.ReceiverInitialization{}, events.RouteIdentity{}, nil
}

func admitObservedAgentConstructionDependency(runID string, observation pipeline.FlowInstanceObservation, requested events.RouteIdentity) (events.ReceiverInitialization, events.RouteIdentity, error) {
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
		historical, admitted := observation.HistoricalConstruction()
		if !admitted {
			return events.ReceiverInitialization{}, events.RouteIdentity{}, fmt.Errorf("agent construction lacks native or admitted historical evidence")
		}
		receiver, err := identity.Route().AgentIdentityRoute()
		if err != nil {
			return events.ReceiverInitialization{}, events.RouteIdentity{}, err
		}
		sourceReceiver, err := historical.SourceOwner.Route.AgentIdentityRoute()
		if err != nil {
			return events.ReceiverInitialization{}, events.RouteIdentity{}, err
		}
		dependency, err := events.AdmitHistoricalFlowReceiverInitialization(runID, events.MustExistingEntityTarget(target), receiver,
			events.HistoricalReceiverConstruction{SourceRunID: historical.SourceOwner.RunID, SourceRevision: historical.SourceRevision, SourceReceiver: sourceReceiver})
		return dependency, target, err
	}
	receiver, err := identity.Route().AgentIdentityRoute()
	if err != nil {
		return events.ReceiverInitialization{}, events.RouteIdentity{}, err
	}
	dependency, err := events.AdmitStoredFlowReceiverInitialization(runID, receipt.CreatingInput.EventID, events.MustExistingEntityTarget(target), receiver)
	return dependency, target, err
}
