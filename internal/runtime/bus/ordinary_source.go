package bus

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ordinaryPublicationSource resolves an admitted source, not a receiver grant.
// Provider events intentionally omit FlowInstance; selected-run ownership supplies
// it without changing the persisted event or its authenticated source carrier.
type ordinaryPublicationSource struct {
	route events.RouteIdentity
	root  bool
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
	if source.Authority() != events.RoutingSourceAuthorityProviderAdmissionPlan || p.source == nil || route.FlowID == "" || route.EntityID == "" {
		return ordinaryPublicationSource{}, fmt.Errorf("provider local publication requires an admitted declaring flow and entity")
	}
	if _, ok := p.source.FlowScopeByID(route.FlowID); !ok {
		return ordinaryPublicationSource{}, fmt.Errorf("provider declaring flow %q is not in the selected source", route.FlowID)
	}
	root := route.FlowID == semanticview.RootExecutionFlowID(p.source)
	owners := make(map[events.RouteIdentity]struct{})
	for _, descriptor := range p.descriptors {
		descriptor = descriptor.Normalized()
		if descriptor.Materializing || descriptor.EntityID != route.EntityID || descriptor.FlowInstance == "" {
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
		owners[events.RouteIdentity{FlowID: route.FlowID, FlowInstance: descriptor.FlowInstance, EntityID: route.EntityID}] = struct{}{}
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
	if s.root {
		return instance == "" || instance == s.route.FlowInstance
	}
	return instance == s.route.FlowInstance
}

func (s ordinaryPublicationSource) includesSubscriber(subscriber Subscriber) bool {
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
	if s.route.Empty() || !candidate.PersistAsDelivery {
		return true
	}
	identity := candidate.AgentIdentity.Normalize()
	return identity.Validate() == nil && s.ownsAgent(identity.FlowInstance())
}

func (s ordinaryPublicationSource) localNodeIntents(source semanticview.Source, evt events.Event, routed []Subscriber) []RoutePlanDeliveryIntent {
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
