package bus

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// An unresolved subscription stays a candidate until composition and the late
// materializer have contributed their routes. It is never an executable route.
func (i RoutePlanDeliveryIntent) pendingNodeCandidate() bool {
	return i.Recipient.IsNode() && len(i.nodeSubscribers) != 0 && i.TargetBlueprint.Empty() && i.TargetOwnership.Empty()
}

func (p RoutePlan) routedRecipients() []Subscriber {
	out := append([]Subscriber(nil), p.routedAgents...)
	for _, intent := range p.DeliveryIntents {
		if intent.Recipient.IsNode() && !intent.TargetOwnership.Empty() {
			out = append(out, intent.nodeSubscribers...)
		}
	}
	return dedupeSubscribers(out)
}

// This projection carries descriptions only. Node authority stays with the
// candidate that the publication target owner admits, not a second node list.
func (p *RoutePlan) describeRoutedCandidates(subscribers []Subscriber) {
	for _, subscriber := range subscribers {
		if subscriber.Recipient.IsAgent() {
			p.routedAgents = append(p.routedAgents, subscriber)
		}
	}
	for index := range p.DeliveryIntents {
		intent := &p.DeliveryIntents[index]
		if !intent.Recipient.IsNode() || len(intent.nodeSubscribers) != 0 {
			continue
		}
		for _, subscriber := range subscribers {
			if intent.Recipient == subscriber.Recipient && intent.Handler.Node().Equal(subscriber.handlerNode) &&
				(intent.TargetBlueprint.FlowInstance == "" || subscriber.Path == intent.TargetBlueprint.FlowInstance) {
				intent.nodeSubscribers = dedupeSubscribers(append(intent.nodeSubscribers, subscriber))
			}
		}
	}
	p.routedAgents = dedupeSubscribers(p.routedAgents)
}

func (p publicationTargetOwnerEvidence) admitIntentIngress(event events.Event, intent RoutePlanDeliveryIntent) error {
	if event.AdmissionClass() != events.EventAdmissionRootIngress && event.AdmissionClass() != events.EventAdmissionOperatorInjected {
		return nil
	}
	if intent.Producer == routeIntentProducerConnectRoutePlan && !intent.ConnectClaim.Empty() {
		return nil
	}
	sourceFlow := event.RoutingSource().Route().FlowID
	if event.RoutingSource().Kind() == events.RoutingSourceRoot {
		sourceFlow = semanticview.RootExecutionFlowID(p.source)
	}
	target := intent.TargetOwnership.Route().Normalized()
	if target.Empty() {
		target = intent.TargetBlueprint.Normalized()
	}
	if target.FlowID != "" && target.FlowID != sourceFlow {
		return fmt.Errorf("ingress source %q cannot authorize private delivery to %q without a compiled connection", sourceFlow, target.FlowID)
	}
	return nil
}

// Resolve candidates against the exact admitted node results of the composed
// publication. Reusing a result retains its claim, context and projection.
func (p publicationTargetOwnerEvidence) bindNodeCandidates(plan *RoutePlan) {
	type receiver struct {
		node    events.DeliveryRecipient
		handler pipeline.DeliveryTargetHandler
	}
	admitted := make(map[receiver][]RoutePlanDeliveryIntent)
	for _, intent := range plan.DeliveryIntents {
		if !intent.Recipient.IsNode() || intent.TargetOwnership.Empty() || intent.Handler.Empty() {
			continue
		}
		key := receiver{node: intent.Recipient, handler: intent.Handler}
		admitted[key] = append(admitted[key], intent)
	}
	root := rootExecutionCoordinate(p.source, plan.Event.RunID())
	for index, candidate := range plan.DeliveryIntents {
		if !candidate.pendingNodeCandidate() {
			continue
		}
		key := receiver{node: candidate.Recipient, handler: candidate.Handler}
		for _, result := range admitted[key] {
			if !result.Handler.Equal(candidate.Handler) {
				continue
			}
			subscriber := candidate.nodeSubscribers[0]
			target := result.TargetOwnership.Route()
			path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
			matches := path == "" || routeMatchesInternalSubscriber(target, subscriber, root)
			if !matches && root.Matches(target.FlowID, target.FlowInstance) {
				node, present := result.Recipient.Node()
				flow := subscriber.handlerNode.FlowPath()
				if flow == "" {
					flow = target.FlowID
				}
				matches = present && node.Equal(subscriber.handlerNode) && path == flow && target.FlowID == flow
			}
			if matches {
				result.nodeSubscribers = dedupeSubscribers(append(append([]Subscriber(nil), result.nodeSubscribers...), candidate.nodeSubscribers...))
				plan.DeliveryIntents[index] = result
				break
			}
		}
	}
	plan.DeliveryIntents = normalizeRoutePlanDeliveryIntents(plan.DeliveryIntents)
}
