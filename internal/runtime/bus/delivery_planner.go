package bus

import (
	"context"
	"errors"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
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
	resolveRoutedSubscribers            func(string) []Subscriber
	resolveSubscribedRecipients         func(string) []deliveryRecipientCandidate
	resolveRoutedNodeInternalRecipients func(events.Event, []Subscriber) []deliveryRecipientCandidate
	describeSubscribersForEvent         func(string, []Subscriber) []PublishDiagnosticRecipient
}

func (r deliveryRouteResolver) Resolve(evt events.Event) deliveryRoutingResult {
	routedRecipients := r.resolveRoutedSubscribers(string(evt.Type))
	subscribedRecipients := r.resolveSubscribedRecipients(string(evt.Type))
	routedCandidates := routedSubscriberCandidates(routedRecipients)
	if r.resolveRoutedNodeInternalRecipients != nil {
		routedCandidates = append(routedCandidates, r.resolveRoutedNodeInternalRecipients(evt, routedRecipients)...)
	}
	recipients := normalizeDeliveryRecipientCandidates(append(routedCandidates, subscribedRecipients...))
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
	DeliveryTargets     map[string]events.RouteIdentity
	DeliveryRoutes      []events.DeliveryRoute
	TargetFailure       runtimepinrouting.TargetFailure
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
			DeliveryTargets:     deliveryTargetsForManifest(evt, persistedDeliveryRecipientIDs(recipients), nil),
			DeliveryRoutes:      agentDeliveryRoutesForRecipients(persistedDeliveryRecipientIDs(recipients), deliveryTargetsForManifest(evt, persistedDeliveryRecipientIDs(recipients), nil)),
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
	plan.DeliveryTargets = manifest.DeliveryTargets
	plan.DeliveryRoutes = append([]events.DeliveryRoute(nil), manifest.DeliveryRoutes...)
	plan.DeliveryRoutes = append(plan.DeliveryRoutes, routedRootNodeDeliveryRoutesForNoTargetEvent(evt, routing.RoutedRecipients, plan.Recipients, plan.PersistedRecipients)...)
	plan.DeliveryRoutes = append(plan.DeliveryRoutes, routedNodeDeliveryRoutesForNoTargetEvent(evt, routing.RoutedRecipients, plan.Recipients, plan.PersistedRecipients)...)
	plan.DeliveryRoutes = append(plan.DeliveryRoutes, internalDeliveryRoutesForPlan(evt, plan.Recipients, plan.PersistedRecipients, routing.RoutedRecipients)...)
	plan.DeliveryRoutes = events.NormalizeDeliveryRoutes(plan.DeliveryRoutes)
	plan.TargetFailure = manifest.TargetFailure
	if plan.TargetFailure != "" && hasInternalRoutedSubscriberForTarget(evt, routing.RoutedRecipients) {
		plan.TargetFailure = ""
	}
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
	plan.DeliveryTargets = manifest.DeliveryTargets
	plan.DeliveryRoutes = append([]events.DeliveryRoute(nil), manifest.DeliveryRoutes...)
	plan.TargetFailure = manifest.TargetFailure
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
			resolveRoutedSubscribers:            eb.resolveRoutedSubscribers,
			resolveSubscribedRecipients:         eb.resolveSubscribedRecipientsForPlanning,
			resolveRoutedNodeInternalRecipients: eb.resolveInternalRecipientsForRoutedNodePlanning,
			describeSubscribersForEvent:         eb.describeSubscribersForEvent,
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
	eventEntityID := strings.TrimSpace(evt.EntityID())
	targets := eventDeliveryTargetRoutes(evt)
	if len(recipients) == 0 {
		return deliveryRecipientManifest{
			TargetFailure: targetDeliveryFailure(evt, descriptors),
		}
	}
	singularTarget := evt.TargetRoute()
	allowed := make([]string, 0, len(recipients))
	persisted := make([]string, 0, len(recipients))
	deliveryTargets := map[string]events.RouteIdentity{}
	deliveryRoutes := make([]events.DeliveryRoute, 0, len(recipients))
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
				if len(targets) == 0 {
					continue
				}
			}
		}
		target, targetOK := deliveryTargetForDescriptor(descriptor, singularTarget, targets)
		if len(targets) > 0 && !targetOK {
			continue
		}
		allowed = append(allowed, recipient.ID)
		persisted = append(persisted, recipient.ID)
		if !target.Empty() {
			deliveryTargets[recipient.ID] = target
		}
		deliveryRoutes = append(deliveryRoutes, events.DeliveryRoute{
			SubscriberType: "agent",
			SubscriberID:   recipient.ID,
			Target:         target,
		})
	}
	persisted = uniqueStrings(persisted)
	manifest := deliveryRecipientManifest{
		Recipients:          uniqueStrings(allowed),
		PersistedRecipients: persisted,
		DeliveryTargets:     deliveryTargetsForManifest(evt, persisted, deliveryTargets),
		DeliveryRoutes:      events.NormalizeDeliveryRoutes(deliveryRoutes),
	}
	if len(targets) > 0 && len(manifest.Recipients) == 0 {
		manifest.TargetFailure = targetDeliveryFailure(evt, descriptors)
	}
	return manifest
}

func agentDeliveryRoutesForRecipients(recipients []string, deliveryTargets map[string]events.RouteIdentity) []events.DeliveryRoute {
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(recipients))
	for _, recipient := range recipients {
		out = append(out, events.DeliveryRoute{
			SubscriberType: "agent",
			SubscriberID:   recipient,
			Target:         deliveryTargets[strings.TrimSpace(recipient)],
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

func internalDeliveryRoutesForPlan(evt events.Event, recipients, persisted []string, routed []Subscriber) []events.DeliveryRoute {
	internalRecipients := filterOutAgentIDs(recipients, persisted)
	if len(internalRecipients) == 0 {
		return nil
	}
	targets := matchedInternalDeliveryTargets(evt, routed)
	if len(targets) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(internalRecipients)*len(targets))
	for _, recipient := range internalRecipients {
		for _, target := range targets {
			out = append(out, events.DeliveryRoute{
				SubscriberType: "node",
				SubscriberID:   recipient,
				Target:         target,
			})
		}
	}
	return events.NormalizeDeliveryRoutes(out)
}

func routedNodeDeliveryRoutesForNoTargetEvent(evt events.Event, routed []Subscriber, recipients, persisted []string) []events.DeliveryRoute {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	internalRecipients := filterOutAgentIDs(recipients, persisted)
	if len(internalRecipients) == 0 {
		return nil
	}
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if flowInstance == "" {
		return nil
	}
	eventEntityID := strings.TrimSpace(evt.EntityID())
	routedNodeMatched := false
	for _, subscriber := range routed {
		if !routedNodeMatchesConcreteFlowInstanceEvent(evt, subscriber) {
			continue
		}
		routedNodeMatched = true
		break
	}
	if !routedNodeMatched {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(internalRecipients))
	for _, recipient := range internalRecipients {
		out = append(out, events.DeliveryRoute{
			SubscriberType: "node",
			SubscriberID:   recipient,
			Target: events.RouteIdentity{
				FlowInstance: flowInstance,
				EntityID:     eventEntityID,
			},
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

func routedRootNodeDeliveryRoutesForNoTargetEvent(evt events.Event, routed []Subscriber, recipients, persisted []string) []events.DeliveryRoute {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	if strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/") != "" {
		return nil
	}
	rootNodeIDs := routedRootNodeSubscriberIDsForNoTargetEvent(evt, routed)
	if len(rootNodeIDs) == 0 {
		return nil
	}
	internalRecipients := filterOutAgentIDs(recipients, persisted)
	if len(internalRecipients) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(rootNodeIDs))
	for _, recipient := range sortedStringKeys(rootNodeIDs) {
		out = append(out, events.DeliveryRoute{
			SubscriberType: "node",
			SubscriberID:   recipient,
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

func routedRootNodeSubscriberIDsForNoTargetEvent(evt events.Event, routed []Subscriber) map[string]struct{} {
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type)), "/")
	if eventType == "" || strings.Contains(eventType, "/") {
		return nil
	}
	out := make(map[string]struct{}, len(routed))
	for _, subscriber := range routed {
		if !routedRootNodeMatchesNoTargetEvent(evt, subscriber) {
			continue
		}
		out[strings.TrimSpace(subscriber.ID)] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func routedRootNodeMatchesNoTargetEvent(evt events.Event, subscriber Subscriber) bool {
	if strings.TrimSpace(subscriber.ID) == "" || strings.TrimSpace(subscriber.Type) == "agent" {
		return false
	}
	if strings.Trim(strings.TrimSpace(subscriber.Path), "/") != "" {
		return false
	}
	if strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/") != "" {
		return false
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type)), "/")
	return eventType != "" && !strings.Contains(eventType, "/")
}

func routedNodeInternalSubscriptionAliases(evt events.Event, routed []Subscriber) []string {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type)), "/")
	if eventType == "" {
		return nil
	}
	out := []string{eventType}
	for _, subscriber := range routed {
		if !routedNodeMatchesConcreteFlowInstanceEvent(evt, subscriber) {
			continue
		}
		instancePath := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
		if instancePath == "" || !strings.HasPrefix(eventType, instancePath+"/") {
			continue
		}
		localEvent := strings.TrimPrefix(eventType, instancePath+"/")
		staticScope := runtimeflowidentity.SemanticScopeFromInstancePath(instancePath)
		if localEvent == "" || staticScope == "" {
			continue
		}
		out = append(out, staticScope+"/"+localEvent)
	}
	return uniqueStrings(out)
}

func routedNodeMatchesConcreteFlowInstanceEvent(evt events.Event, subscriber Subscriber) bool {
	if strings.TrimSpace(subscriber.ID) == "" || strings.TrimSpace(subscriber.Type) == "agent" {
		return false
	}
	instancePath := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if instancePath == "" || flowInstance == "" || instancePath != flowInstance {
		return false
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type)), "/")
	return eventType != "" && strings.HasPrefix(eventType, instancePath+"/")
}

func matchedInternalDeliveryTargets(evt events.Event, subscribers []Subscriber) []events.RouteIdentity {
	targets := eventDeliveryTargetRoutes(evt)
	if len(targets) == 0 {
		return nil
	}
	out := make([]events.RouteIdentity, 0, len(targets))
	for _, target := range targets {
		for _, subscriber := range subscribers {
			if strings.TrimSpace(subscriber.ID) == "" || strings.TrimSpace(subscriber.Type) == "agent" {
				continue
			}
			if routeMatchesInternalSubscriber(target, subscriber) {
				out = append(out, target.Normalized())
				break
			}
		}
	}
	return uniqueRouteIdentities(out)
}

func uniqueRouteIdentities(in []events.RouteIdentity) []events.RouteIdentity {
	if len(in) == 0 {
		return nil
	}
	out := make([]events.RouteIdentity, 0, len(in))
	seen := map[string]struct{}{}
	for _, route := range in {
		route = route.Normalized()
		if route.Empty() {
			continue
		}
		key := strings.Join([]string{route.FlowID, route.FlowInstance, route.EntityID}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, route)
	}
	return out
}

func hasInternalRoutedSubscriberForTarget(evt events.Event, subscribers []Subscriber) bool {
	targets := eventDeliveryTargetRoutes(evt)
	for _, subscriber := range subscribers {
		if strings.TrimSpace(subscriber.ID) == "" {
			continue
		}
		if strings.TrimSpace(subscriber.Type) == "agent" {
			continue
		}
		if len(targets) == 0 {
			return true
		}
		for _, target := range targets {
			if routeMatchesInternalSubscriber(target, subscriber) {
				return true
			}
		}
	}
	return false
}

func routeMatchesInternalSubscriber(route events.RouteIdentity, subscriber Subscriber) bool {
	route = route.Normalized()
	if route.Empty() {
		return false
	}
	path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	if path == "" {
		return route.FlowInstance == "" && route.FlowID == "" && route.EntityID != ""
	}
	return route.FlowInstance != "" && route.FlowInstance == path
}

func targetDeliveryFailure(evt events.Event, descriptors map[string]ActiveAgentDescriptor) runtimepinrouting.TargetFailure {
	targets := eventDeliveryTargetRoutes(evt)
	if len(targets) == 0 {
		return ""
	}
	if !allTargetsHaveLiveDescriptor(targets, descriptors) {
		return runtimepinrouting.FailureTargetUnreachableTerminated
	}
	return runtimepinrouting.FailureTargetNotSubscribed
}

func eventDeliveryTargetRoutes(evt events.Event) []events.RouteIdentity {
	if singular := evt.TargetRoute(); !singular.Empty() {
		return []events.RouteIdentity{singular}
	}
	return evt.TargetRoutes()
}

func allTargetsHaveLiveDescriptor(targets []events.RouteIdentity, descriptors map[string]ActiveAgentDescriptor) bool {
	if len(targets) == 0 {
		return true
	}
	if len(descriptors) == 0 {
		return false
	}
	for _, target := range targets {
		found := false
		for _, descriptor := range descriptors {
			if routeMatchesDescriptor(target, descriptor) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func deliveryTargetForDescriptor(descriptor ActiveAgentDescriptor, singular events.RouteIdentity, targets []events.RouteIdentity) (events.RouteIdentity, bool) {
	descriptor = descriptor.Normalized()
	if !singular.Empty() {
		return singular, routeMatchesDescriptor(singular, descriptor)
	}
	if len(targets) == 0 {
		return events.RouteIdentity{}, true
	}
	for _, target := range targets {
		if routeMatchesDescriptor(target, descriptor) {
			return target.Normalized(), true
		}
	}
	return events.RouteIdentity{}, false
}

func routeMatchesDescriptor(route events.RouteIdentity, descriptor ActiveAgentDescriptor) bool {
	route = route.Normalized()
	descriptor = descriptor.Normalized()
	if route.EntityID != "" && descriptor.EntityID != "" && route.EntityID != descriptor.EntityID {
		return false
	}
	if route.FlowInstance != "" && descriptor.FlowInstance != "" && route.FlowInstance != descriptor.FlowInstance {
		return false
	}
	return route.EntityID != "" || route.FlowInstance != ""
}

func deliveryTargetsForManifest(evt events.Event, recipients []string, existing map[string]events.RouteIdentity) map[string]events.RouteIdentity {
	out := map[string]events.RouteIdentity{}
	for recipient, target := range existing {
		if target = target.Normalized(); !target.Empty() {
			out[strings.TrimSpace(recipient)] = target
		}
	}
	if singular := evt.TargetRoute(); !singular.Empty() {
		for _, recipient := range recipients {
			recipient = strings.TrimSpace(recipient)
			if recipient == "" {
				continue
			}
			out[recipient] = singular
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
