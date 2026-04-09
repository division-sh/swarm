package bus

import (
	"context"
	"errors"
	"strings"

	"swarm/internal/events"
)

type deliveryRoutingResult struct {
	Recipients           []deliveryRecipientCandidate
	RoutedRecipients     []Subscriber
	SubscribedRecipients []string
	ExtraDetail          map[string]any
}

type deliveryRecipientCandidate struct {
	ID                string
	PersistAsDelivery bool
}

type deliveryRouteResolver struct {
	resolveRoutedSubscribers    func(string) []Subscriber
	resolveSubscribedRecipients func(string) []deliveryRecipientCandidate
	describeSubscribersForEvent func(string, []Subscriber) []PublishDiagnosticRecipient
}

func (r deliveryRouteResolver) Resolve(evt events.Event) deliveryRoutingResult {
	routedRecipients := r.resolveRoutedSubscribers(string(evt.Type))
	subscribedRecipients := r.resolveSubscribedRecipients(string(evt.Type))
	recipients := normalizeDeliveryRecipientCandidates(append(routedSubscriberCandidates(routedRecipients), subscribedRecipients...))
	result := deliveryRoutingResult{
		Recipients:           recipients,
		RoutedRecipients:     routedRecipients,
		SubscribedRecipients: deliveryRecipientIDs(subscribedRecipients),
		ExtraDetail: map[string]any{
			"routed_recipients_count":       len(routedRecipients),
			"subscription_recipients_count": len(subscribedRecipients),
		},
	}
	if described := publishDiagnosticRecipientMaps(r.describeSubscribersForEvent(string(evt.Type), routedRecipients)); len(described) > 0 {
		result.ExtraDetail["routed_recipients"] = described
	}
	if direct := deliveryRecipientIDs(subscribedRecipients); len(direct) > 0 {
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

func (p deliveryRecipientPolicy) Evaluate(ctx context.Context, evt events.Event, recipients []deliveryRecipientCandidate) (deliveryRecipientManifest, error) {
	descriptors, ok, err := p.loadActiveAgentDescriptors(ctx)
	if err != nil {
		return deliveryRecipientManifest{}, err
	}
	if !ok {
		return deliveryRecipientManifest{
			Recipients:          deliveryRecipientIDs(recipients),
			PersistedRecipients: persistedDeliveryRecipientIDs(recipients),
		}, nil
	}
	return filterDeliveryRecipientCandidates(evt, recipients, descriptors), nil
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

func (p deliveryPlanner) PlanDirect(ctx context.Context, evt events.Event, recipients []string) (eventDeliveryPlan, error) {
	plan := eventDeliveryPlan{Event: evt}
	if evt.Type == events.EventType("platform.runtime_log") {
		return plan, nil
	}
	requested := uniqueStrings(recipients)
	if len(requested) == 0 {
		return eventDeliveryPlan{}, errors.New("direct delivery recipients are required")
	}
	manifest, err := p.recipientPolicy.Evaluate(ctx, evt, agentDeliveryRecipientCandidates(requested))
	if err != nil {
		return eventDeliveryPlan{}, err
	}
	plan.Recipients = manifest.Recipients
	plan.PersistedRecipients = manifest.PersistedRecipients
	plan.ExtraDetail = map[string]any{
		"direct":                     true,
		"requested_recipients":       append([]string(nil), requested...),
		"requested_recipients_count": len(requested),
	}
	if filtered := filteredRecipients(requested, manifest.Recipients); len(filtered) > 0 {
		plan.ExtraDetail["filtered_out_recipients"] = filtered
		plan.ExtraDetail["filtered_out_recipients_count"] = len(filtered)
	}
	return plan, nil
}

func filteredRecipients(requested, allowed []string) []string {
	if len(requested) == 0 {
		return nil
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, recipient := range allowed {
		allowedSet[recipient] = struct{}{}
	}
	out := make([]string, 0, len(requested))
	for _, recipient := range requested {
		if _, ok := allowedSet[recipient]; ok {
			continue
		}
		out = append(out, recipient)
	}
	return out
}

func (eb *EventBus) newEventBusDeliveryPlanner() deliveryPlanner {
	return newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers:    eb.resolveRoutedSubscribers,
			resolveSubscribedRecipients: eb.resolveSubscribedRecipientsForPlanning,
			describeSubscribersForEvent: eb.describeSubscribersForEvent,
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: eb.activeAgentDescriptors,
		},
	)
}

func normalizeDeliveryRecipientCandidates(in []deliveryRecipientCandidate) []deliveryRecipientCandidate {
	if len(in) == 0 {
		return nil
	}
	out := make([]deliveryRecipientCandidate, 0, len(in))
	indexByID := make(map[string]int, len(in))
	for _, candidate := range in {
		candidate.ID = strings.TrimSpace(candidate.ID)
		if candidate.ID == "" {
			continue
		}
		if idx, ok := indexByID[candidate.ID]; ok {
			out[idx].PersistAsDelivery = out[idx].PersistAsDelivery || candidate.PersistAsDelivery
			continue
		}
		indexByID[candidate.ID] = len(out)
		out = append(out, candidate)
	}
	return out
}

func routedSubscriberCandidates(in []Subscriber) []deliveryRecipientCandidate {
	if len(in) == 0 {
		return nil
	}
	out := make([]deliveryRecipientCandidate, 0, len(in))
	for _, subscriber := range in {
		id := strings.TrimSpace(subscriber.ID)
		if id == "" {
			continue
		}
		if strings.TrimSpace(subscriber.Type) != "agent" {
			continue
		}
		out = append(out, deliveryRecipientCandidate{
			ID:                id,
			PersistAsDelivery: true,
		})
	}
	return out
}

func agentDeliveryRecipientCandidates(in []string) []deliveryRecipientCandidate {
	if len(in) == 0 {
		return nil
	}
	out := make([]deliveryRecipientCandidate, 0, len(in))
	for _, recipient := range in {
		if recipient = strings.TrimSpace(recipient); recipient != "" {
			out = append(out, deliveryRecipientCandidate{ID: recipient, PersistAsDelivery: true})
		}
	}
	return normalizeDeliveryRecipientCandidates(out)
}

func deliveryRecipientIDs(in []deliveryRecipientCandidate) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, candidate := range in {
		if candidate.ID = strings.TrimSpace(candidate.ID); candidate.ID != "" {
			out = append(out, candidate.ID)
		}
	}
	return uniqueStrings(out)
}

func persistedDeliveryRecipientIDs(in []deliveryRecipientCandidate) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, candidate := range in {
		if !candidate.PersistAsDelivery {
			continue
		}
		if candidate.ID = strings.TrimSpace(candidate.ID); candidate.ID != "" {
			out = append(out, candidate.ID)
		}
	}
	return uniqueStrings(out)
}

func filterDeliveryRecipientCandidates(evt events.Event, recipients []deliveryRecipientCandidate, descriptors map[string]ActiveAgentDescriptor) deliveryRecipientManifest {
	recipients = normalizeDeliveryRecipientCandidates(recipients)
	if len(recipients) == 0 {
		return deliveryRecipientManifest{}
	}
	eventEntityID := strings.TrimSpace(evt.EntityID())
	allowed := make([]string, 0, len(recipients))
	persisted := make([]string, 0, len(recipients))
	for _, recipient := range recipients {
		if !recipient.PersistAsDelivery {
			allowed = append(allowed, recipient.ID)
			continue
		}
		descriptor, ok := descriptors[recipient.ID]
		if !ok {
			continue
		}
		if descriptor.EntityID != "" {
			if eventEntityID == "" || descriptor.EntityID != eventEntityID {
				continue
			}
		}
		allowed = append(allowed, recipient.ID)
		persisted = append(persisted, recipient.ID)
	}
	return deliveryRecipientManifest{
		Recipients:          uniqueStrings(allowed),
		PersistedRecipients: uniqueStrings(persisted),
	}
}
