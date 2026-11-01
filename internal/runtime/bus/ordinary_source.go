package bus

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ordinaryPublicationSource resolves an admitted source, not a receiver grant.
// Provider events carry a declaration, never a concrete sender. Construction
// and selected-run ownership supply the local receiver without changing it.
type ordinaryPublicationSource struct {
	route           events.RouteIdentity
	root            bool
	declarationOnly bool
}

func (p publicationTargetOwnerEvidence) ordinarySource(evt events.Event) (ordinaryPublicationSource, error) {
	source := evt.RoutingSource()
	if p.source != nil && !explicitRootPublicationTarget(evt).Empty() {
		coordinate, err := semanticview.AdmitRootExecutionCoordinate(p.source, evt.RunID())
		if err != nil {
			return ordinaryPublicationSource{}, err
		}
		return ordinaryPublicationSource{route: events.RouteIdentity{
			FlowID: coordinate.FlowID(), FlowInstance: coordinate.RunID(), EntityID: coordinate.RunID(),
		}, root: true}, nil
	}
	if source.Kind() != events.RoutingSourceExternalIngress {
		return ordinaryPublicationSource{}, nil
	}
	route := source.Route()
	if source.Authority() != events.RoutingSourceAuthorityProviderAdmissionPlan || p.source == nil || route.FlowID == "" || route.EntityID != "" || route.FlowInstance != "" {
		return ordinaryPublicationSource{}, fmt.Errorf("provider publication requires an exact admitted declaration without a concrete sender")
	}
	if _, ok := p.source.FlowScopeByID(route.FlowID); !ok {
		return ordinaryPublicationSource{}, fmt.Errorf("provider declaring flow %q is not in the selected source", route.FlowID)
	}
	root := route.FlowID == semanticview.RootExecutionFlowID(p.source)
	expected, err := p.selectedProviderExecutionIdentity(evt, route.FlowID, root)
	if err != nil {
		return ordinaryPublicationSource{}, err
	}
	if expected.InstancePath == "" {
		return ordinaryPublicationSource{route: route, root: root, declarationOnly: true}, nil
	}
	owners := make(map[events.RouteIdentity]struct{})
	for _, descriptor := range p.candidates {
		descriptor = normalizedTargetOwnerCandidate(descriptor)
		if descriptor.Route.FlowInstance != expected.InstancePath {
			continue
		}
		if err := p.validateProviderExecutionDescriptor(route.FlowID, expected, descriptor); err != nil {
			return ordinaryPublicationSource{}, err
		}
		owners[events.RouteIdentity{FlowID: route.FlowID, FlowInstance: descriptor.Route.FlowInstance, EntityID: descriptor.Route.EntityID}] = struct{}{}
	}
	if len(owners) == 0 {
		return ordinaryPublicationSource{route: route, root: root, declarationOnly: true}, nil
	}
	if len(owners) != 1 {
		return ordinaryPublicationSource{}, fmt.Errorf("provider declaring flow %q entity %q requires one selected-run execution owner; got %d", route.FlowID, route.EntityID, len(owners))
	}
	for owner := range owners {
		return ordinaryPublicationSource{route: owner, root: root}, nil
	}
	return ordinaryPublicationSource{}, fmt.Errorf("provider source ownership resolution failed")
}

func (p publicationTargetOwnerEvidence) selectedProviderExecutionIdentity(event events.Event, flowID string, root bool) (runtimeflowidentity.Instance, error) {
	if account, selected, err := p.selectedAccountInstance(p.context, event); selected || err != nil {
		return account, err
	}
	expected, err := p.providerExecutionIdentity(event, flowID, root)
	if err != nil || expected.InstancePath != "" || p.context == nil {
		return expected, err
	}
	preview, _ := p.context.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	if preview == nil {
		return expected, nil
	}
	expected, err = providerConstructionCandidate(flowID, preview.selected[flowID])
	if err != nil || expected.InstancePath == "" {
		return expected, err
	}
	return expected, expected.ValidateConstruction(p.source, event.RunID())
}

func providerConstructionCandidate(flowID string, candidates []runtimeflowidentity.Instance) (runtimeflowidentity.Instance, error) {
	switch len(candidates) {
	case 0:
		return runtimeflowidentity.Instance{}, nil
	case 1:
		return candidates[0], nil
	default:
		return runtimeflowidentity.Instance{}, fmt.Errorf("provider declaring flow %q requires one selected-run execution owner; got %d", flowID, len(candidates))
	}
}

func (p publicationTargetOwnerEvidence) providerExecutionIdentity(event events.Event, flowID string, root bool) (runtimeflowidentity.Instance, error) {
	if root {
		return runtimeflowidentity.Stored(p.source, flowID, event.RunID(), event.RunID(), runtimeflowidentity.EntityID(event.RunID()), ""), nil
	}
	keyless, err := pipeline.StandingConstructionIsKeyless(p.source, flowID)
	if err != nil || !keyless {
		return runtimeflowidentity.Instance{}, err
	}
	return runtimeflowidentity.StandingForGeneration(p.source, flowID, event.RunID())
}

func (p publicationTargetOwnerEvidence) validateProviderExecutionDescriptor(flowID string, expected runtimeflowidentity.Instance, descriptor pipeline.DeliveryTargetOwnerCandidate) error {
	if expected.InstancePath != "" && (descriptor.Route.FlowInstance != expected.InstancePath || descriptor.Route.EntityID != expected.EntityID) {
		return fmt.Errorf("provider receiver disagrees with its canonical constructed identity")
	}
	if descriptor.Materializing {
		owner := events.RouteIdentity{FlowID: flowID, FlowInstance: descriptor.Route.FlowInstance, EntityID: descriptor.Route.EntityID}
		if _, admitted := p.activationOwners[owner]; !admitted {
			return fmt.Errorf("provider receiver has no admitted same-publication constructor")
		}
	}
	var err error
	descriptor, err = p.decisionTargetCandidate(descriptor)
	if err != nil {
		return err
	}
	err = descriptor.Availability.Validate(p.source, flowID)
	if refusal, ok := err.(*pipeline.NoHandlerError); ok {
		refusal.FlowInstance = descriptor.Route.FlowInstance
	}
	return err
}

func (s ordinaryPublicationSource) eventKeys(evt events.Event) []string {
	if s.declarationOnly {
		return nil
	}
	if s.route.Empty() {
		return routedEventKeysForPlan(evt)
	}
	local := eventContextLocalEventForFlowInstance(string(evt.Type()), s.route.FlowID)
	if local == "" {
		return nil
	}
	if s.root {
		// Root direct subscriptions use local names; flow-scoped node patterns
		// use the root flow identity. Both still require the same exact owner.
		return uniqueStrings([]string{local, s.route.FlowID + "/" + local, s.route.FlowInstance + "/" + local})
	}
	return uniqueStrings([]string{s.route.FlowID + "/" + local, s.route.FlowInstance + "/" + local})
}

func (s ordinaryPublicationSource) ownsAgent(instance string) bool {
	if s.declarationOnly {
		return false
	}
	if s.root {
		return instance == "" || instance == s.route.FlowInstance
	}
	return instance == s.route.FlowInstance
}

func (s ordinaryPublicationSource) includesSubscriber(subscriber Subscriber) bool {
	if s.declarationOnly {
		return false
	}
	if s.route.Empty() {
		return true
	}
	if subscriber.Recipient.IsAgent() {
		return !subscriber.AgentPlan.IsZero() && s.ownsAgent(subscriber.AgentPlan.FlowInstance())
	}
	if !subscriber.Recipient.IsNode() {
		return false
	}
	flowID := subscriber.handlerNode.FlowPath()
	if flowID == "" && s.root {
		flowID = s.route.FlowID
	}
	if flowID != s.route.FlowID {
		return false
	}
	path := strings.Trim(subscriber.Path, "/")
	return path == s.route.FlowInstance || path == s.route.FlowID || (s.root && path == ".")
}

func (s ordinaryPublicationSource) includesCandidate(candidate deliveryRecipientCandidate) bool {
	if s.declarationOnly {
		return !candidate.PersistAsDelivery
	}
	if s.route.Empty() || !candidate.PersistAsDelivery {
		return true
	}
	identity := candidate.AgentIdentity.Normalize()
	return identity.Validate() == nil && s.ownsAgent(identity.FlowInstance())
}

func (s ordinaryPublicationSource) nodeIntents(source semanticview.Source, localEvent, originalEvent events.Event, routed []Subscriber) []RoutePlanDeliveryIntent {
	rootRoutes, concreteRoutes := s.localNodeRoutes(source, localEvent, routed)
	var targeted []plannedDeliveryRoute
	for _, target := range eventDeliveryTargetRoutes(originalEvent) {
		target = target.Normalized()
		if target.Empty() {
			continue
		}
		for _, subscriber := range routed {
			if !subscriber.Recipient.IsNode() || !routeMatchesInternalSubscriber(target, subscriber, rootExecutionCoordinate(source, originalEvent.RunID())) {
				continue
			}
			targeted = append(targeted, plannedDeliveryRoute{
				Recipient: subscriber.Recipient, Target: target,
				Handler: routedSubscriberTargetHandler(subscriber, originalEvent.Type()),
			})
		}
	}
	out := routePlanDeliveryIntentsFromRoutes(rootRoutes, routeIntentProducerOrdinary)
	out = append(out, routePlanDeliveryIntentsFromRoutes(concreteRoutes, routeIntentProducerConcreteNodeRoute)...)
	return append(out, routePlanDeliveryIntentsFromRoutes(normalizePlannedDeliveryRoutes(targeted), routeIntentProducerOrdinary)...)
}

func (s ordinaryPublicationSource) localNodeRoutes(source semanticview.Source, evt events.Event, routed []Subscriber) ([]plannedDeliveryRoute, []plannedDeliveryRoute) {
	if s.declarationOnly || len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil, nil
	}
	var root semanticview.RootExecutionCoordinate
	if s.route.Empty() {
		if admitted, err := semanticview.AdmitRootExecutionCoordinate(source, evt.RunID()); err == nil {
			root = admitted
		}
	}
	var rootRoutes, concreteRoutes []plannedDeliveryRoute
	for _, subscriber := range routed {
		if !subscriber.Recipient.IsNode() {
			continue
		}
		route := plannedDeliveryRoute{Recipient: subscriber.Recipient, Handler: routedSubscriberTargetHandler(subscriber, evt.Type())}
		if !s.route.Empty() {
			if s.includesSubscriber(subscriber) {
				route.Target = s.route
				concreteRoutes = append(concreteRoutes, route)
			}
			continue
		}
		if routedRootNodeMatchesNoTargetEvent(evt, subscriber, root) {
			route.Target = events.RouteIdentity{FlowID: root.FlowID(), FlowInstance: root.RunID()}
			rootRoutes = append(rootRoutes, route)
		}
		if exactEventFlowInstance(evt) != "" && routedNodeMatchesConcreteFlowInstanceEvent(evt, subscriber) {
			route.Target = sameInstanceNodeTarget(source, evt, subscriber)
			concreteRoutes = append(concreteRoutes, route)
		}
	}
	return rootRoutes, concreteRoutes
}

func sameInstanceNodeTarget(source semanticview.Source, evt events.Event, subscriber Subscriber) events.RouteIdentity {
	instance := exactEventFlowInstance(evt)
	if node, ok := subscriber.Recipient.Node(); ok && source != nil {
		if scope, found := source.FlowScopeByID(node.FlowPath()); found {
			switch strings.ToLower(strings.TrimSpace(scope.Mode)) {
			case runtimecontracts.FlowModeStatic:
				instance = strings.Trim(strings.TrimSpace(subscriber.Path), "/")
			}
		}
	}
	return events.RouteIdentity{FlowInstance: instance}.Normalized()
}
