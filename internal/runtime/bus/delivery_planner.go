package bus

import (
	"context"

	"swarm/internal/events"
)

type deliveryRoutingResult struct {
	Recipients           []string
	RoutedRecipients     []Subscriber
	SubscribedRecipients []string
	ExtraDetail          map[string]any
}

type deliveryRouteResolver struct {
	resolveRoutedSubscribers    func(string) []Subscriber
	resolveSubscribedRecipients func(string) []string
	describeSubscribersForEvent func(string, []Subscriber) []PublishDiagnosticRecipient
}

func (r deliveryRouteResolver) Resolve(evt events.Event) deliveryRoutingResult {
	routedRecipients := r.resolveRoutedSubscribers(string(evt.Type))
	subscribedRecipients := r.resolveSubscribedRecipients(string(evt.Type))
	recipients := uniqueStrings(append(subscriberIDs(routedRecipients), subscribedRecipients...))
	result := deliveryRoutingResult{
		Recipients:           recipients,
		RoutedRecipients:     routedRecipients,
		SubscribedRecipients: subscribedRecipients,
		ExtraDetail: map[string]any{
			"routed_recipients_count":       len(routedRecipients),
			"subscription_recipients_count": len(subscribedRecipients),
		},
	}
	if described := publishDiagnosticRecipientMaps(r.describeSubscribersForEvent(string(evt.Type), routedRecipients)); len(described) > 0 {
		result.ExtraDetail["routed_recipients"] = described
	}
	if direct := uniqueStrings(subscribedRecipients); len(direct) > 0 {
		result.ExtraDetail["subscription_recipients"] = direct
	}
	return result
}

type deliveryRecipientManifest struct {
	Recipients          []string
	PersistedRecipients []string
}

type deliveryRecipientPolicy struct {
	loadActiveAgentDescriptors func(context.Context) (map[string]ActiveAgentDescriptor, bool, error)
}

func (p deliveryRecipientPolicy) Evaluate(ctx context.Context, evt events.Event, recipients []string) (deliveryRecipientManifest, error) {
	descriptors, ok, err := p.loadActiveAgentDescriptors(ctx)
	if err != nil {
		return deliveryRecipientManifest{}, err
	}
	if !ok {
		recipients = uniqueStrings(recipients)
		return deliveryRecipientManifest{
			Recipients:          recipients,
			PersistedRecipients: recipients,
		}, nil
	}
	recipients = filterRecipientsForExplicitAgentScope(evt, recipients, descriptors)
	return deliveryRecipientManifest{
		Recipients:          recipients,
		PersistedRecipients: append([]string(nil), recipients...),
	}, nil
}

type deliveryPlanner struct {
	routeResolver   deliveryRouteResolver
	recipientPolicy deliveryRecipientPolicy
}

func newDeliveryPlanner(routeResolver deliveryRouteResolver, recipientPolicy deliveryRecipientPolicy) deliveryPlanner {
	return deliveryPlanner{
		routeResolver:   routeResolver,
		recipientPolicy: recipientPolicy,
	}
}

func (p deliveryPlanner) Plan(ctx context.Context, evt events.Event) (eventDeliveryPlan, error) {
	plan := eventDeliveryPlan{Event: evt}
	if evt.Type == events.EventType("platform.runtime_log") {
		return plan, nil
	}
	routing := p.routeResolver.Resolve(evt)
	manifest, err := p.recipientPolicy.Evaluate(ctx, evt, routing.Recipients)
	if err != nil {
		return eventDeliveryPlan{}, err
	}
	plan.Recipients = manifest.Recipients
	plan.PersistedRecipients = manifest.PersistedRecipients
	plan.RoutedRecipients = routing.RoutedRecipients
	plan.SubscribedRecipients = routing.SubscribedRecipients
	plan.ExtraDetail = routing.ExtraDetail
	return plan, nil
}

func (eb *EventBus) newEventBusDeliveryPlanner() deliveryPlanner {
	return newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers:    eb.resolveRoutedSubscribers,
			resolveSubscribedRecipients: eb.resolveSubscribedRecipients,
			describeSubscribersForEvent: eb.describeSubscribersForEvent,
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: eb.activeAgentDescriptors,
		},
	)
}
