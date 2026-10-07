package bus

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
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

func (p selectedRunTargetOwnerProjection) ordinarySource(evt events.Event) (ordinaryPublicationSource, error) {
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
	var expected runtimeflowidentity.Instance
	if root {
		expected = runtimeflowidentity.Stored(p.source, route.FlowID, evt.RunID(), evt.RunID(), runtimeflowidentity.EntityID(evt.RunID()), "")
	} else if keyless, err := pipeline.StandingConstructionIsKeyless(p.source, route.FlowID); err != nil {
		return ordinaryPublicationSource{}, err
	} else if keyless {
		expected, err = runtimeflowidentity.StandingForGeneration(p.source, route.FlowID, evt.RunID())
		if err != nil {
			return ordinaryPublicationSource{}, err
		}
	}
	owners := make(map[events.RouteIdentity]struct{})
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		if descriptor.FlowInstance == "" {
			continue
		}
		if root {
			coordinate, err := semanticview.AdmitRootExecutionCoordinate(p.source, evt.RunID())
			if err != nil {
				return ordinaryPublicationSource{}, err
			}
			if !coordinate.Matches(route.FlowID, descriptor.FlowInstance) {
				continue
			}
		} else if !runtimeflowidentity.OwnedByFlow(p.source, route.FlowID, descriptor.FlowInstance) {
			continue
		}
		if expected.InstancePath != "" && (descriptor.FlowInstance != expected.InstancePath || descriptor.EntityID != expected.EntityID) {
			return ordinaryPublicationSource{}, fmt.Errorf("provider receiver disagrees with its canonical constructed identity")
		}
		if descriptor.Materializing {
			owner := events.RouteIdentity{FlowID: route.FlowID, FlowInstance: descriptor.FlowInstance, EntityID: descriptor.EntityID}
			if _, admitted := p.activationOwners[owner]; !admitted {
				return ordinaryPublicationSource{}, fmt.Errorf("provider receiver has no admitted same-publication constructor")
			}
		}
		if err := descriptor.Availability.Validate(p.source, route.FlowID); err != nil {
			return ordinaryPublicationSource{}, err
		}
		owners[events.RouteIdentity{FlowID: route.FlowID, FlowInstance: descriptor.FlowInstance, EntityID: descriptor.EntityID}] = struct{}{}
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

func (s ordinaryPublicationSource) localNodeIntents(source semanticview.Source, evt events.Event, routed []Subscriber) []RoutePlanDeliveryIntent {
	if s.declarationOnly {
		return nil
	}
	if s.route.Empty() {
		out := routedRootNodeDeliveryIntentsForNoTargetEvent(source, evt, routed)
		return append(out, routedExactSameInstanceNoTargetNodeDeliveryIntents(source, evt, routed)...)
	}
	if len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	var out []plannedDeliveryRoute
	for _, subscriber := range routed {
		if subscriber.Recipient.IsNode() && s.includesSubscriber(subscriber) {
			out = append(out, plannedDeliveryRoute{Recipient: subscriber.Recipient, Target: s.route, Handler: routedSubscriberTargetHandler(subscriber, evt.Type())})
		}
	}
	return routePlanDeliveryIntentsFromRoutes(out, routeIntentProducerConcreteNodeRoute)
}
