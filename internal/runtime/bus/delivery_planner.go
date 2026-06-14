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
	resolveRoutedSubscribers            func(events.Event) []Subscriber
	resolveSubscribedRecipients         func(string) []deliveryRecipientCandidate
	resolveRoutedNodeInternalRecipients func(events.Event, []Subscriber) []deliveryRecipientCandidate
	describeSubscribersForEvent         func(string, []Subscriber) []PublishDiagnosticRecipient
}

func (r deliveryRouteResolver) Resolve(evt events.Event) deliveryRoutingResult {
	routedRecipients := r.resolveRoutedSubscribers(evt)
	subscribedRecipients := r.resolveSubscribedRecipients(string(evt.Type()))
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
	if described := publishDiagnosticRecipientMaps(r.describeSubscribersForEvent(string(evt.Type()), routedRecipients)); len(described) > 0 {
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
	loadActiveAgentDescriptors  func(context.Context) (map[string]ActiveAgentDescriptor, bool, error)
	loadActiveTargetDescriptors func(context.Context) ([]ActiveTargetDescriptor, bool, error)
}

func (p deliveryRecipientPolicy) Evaluate(ctx context.Context, evt events.Event, recipients []deliveryRecipientCandidate) (deliveryRecipientManifest, error) {
	descriptors, ok, err := p.loadActiveAgentDescriptors(ctx)
	if err != nil {
		return deliveryRecipientManifest{}, err
	}
	targetDescriptors := activeTargetDescriptorsFromAgents(descriptors)
	targetDescriptorsOK := ok || len(targetDescriptors) > 0
	if p.loadActiveTargetDescriptors != nil {
		loaded, loadedOK, err := p.loadActiveTargetDescriptors(ctx)
		if err != nil {
			return deliveryRecipientManifest{}, err
		}
		if loadedOK {
			targetDescriptors = loaded
			targetDescriptorsOK = true
		}
	}
	if !ok {
		manifest := deliveryRecipientManifest{
			Recipients:          deliveryRecipientIDs(recipients),
			PersistedRecipients: persistedDeliveryRecipientIDs(recipients),
			DeliveryTargets:     deliveryTargetsForManifest(evt, persistedDeliveryRecipientIDs(recipients), nil),
			DeliveryRoutes:      agentDeliveryRoutesForRecipients(persistedDeliveryRecipientIDs(recipients), deliveryTargetsForManifest(evt, persistedDeliveryRecipientIDs(recipients), nil)),
		}
		if targetDescriptorsOK && len(eventDeliveryTargetRoutes(evt)) > 0 && len(manifest.Recipients) == 0 {
			manifest.TargetFailure = targetDeliveryFailure(evt, targetDescriptors)
		}
		return manifest, nil
	}
	return filterDeliveryRecipientCandidates(evt, recipients, descriptors, targetDescriptors), nil
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
	routePlan := newRoutePlan(evt)
	if evt.Type() == events.EventType("platform.runtime_log") {
		return routePlan.EventDeliveryPlan(), nil
	}
	routing := p.routeResolver.Resolve(evt)
	manifest, err := p.recipientPolicy.Evaluate(ctx, evt, routing.Recipients)
	if err != nil {
		return eventDeliveryPlan{}, err
	}
	routePlan = routePlanFromManifest(evt, manifest, routePlanSourceAgentPolicy, routePlanReasonMatchedAgentSubscription)
	recipients := routePlan.RecipientIDs()
	persisted := routePlan.PersistedRecipientIDs()
	routePlan.AddDeliveryIntents(routedRootNodeDeliveryIntentsForNoTargetEvent(evt, routing.RoutedRecipients)...)
	routePlan.AddDeliveryIntents(routedRootInputFlowNodeDeliveryIntentsForNoTargetEvent(evt, routing.RoutedRecipients)...)
	routePlan.AddDeliveryIntents(routedNodeDeliveryIntentsForNoRecipientFlowInstanceEvent(evt, routing.RoutedRecipients, recipients, persisted)...)
	routePlan.AddDeliveryIntents(routedNodeDeliveryIntentsForNoTargetEvent(evt, routing.RoutedRecipients, recipients, persisted)...)
	routePlan.AddDeliveryIntents(internalDeliveryIntentsForPlan(evt, recipients, persisted, routing.RoutedRecipients)...)
	if routePlan.TargetFailure != "" && hasInternalRoutedSubscriberForTarget(evt, routing.RoutedRecipients) {
		routePlan.TargetFailure = ""
	}
	routePlan.RoutedRecipients = routing.RoutedRecipients
	routePlan.SubscribedRecipients = routing.SubscribedRecipients
	routePlan.ExtraDetail = routing.ExtraDetail
	return routePlan.EventDeliveryPlan(), nil
}

func (p deliveryPlanner) PlanDirect(ctx context.Context, evt events.Event, recipients []string) (eventDeliveryPlan, error) {
	routePlan := newRoutePlan(evt)
	if evt.Type() == events.EventType("platform.runtime_log") {
		return routePlan.EventDeliveryPlan(), nil
	}
	requested := uniqueStrings(recipients)
	if len(requested) == 0 {
		return eventDeliveryPlan{}, errors.New("direct delivery recipients are required")
	}
	manifest, err := p.recipientPolicy.Evaluate(ctx, evt, agentDeliveryRecipientCandidates(requested))
	if err != nil {
		return eventDeliveryPlan{}, err
	}
	routePlan = routePlanFromManifest(evt, manifest, routePlanSourceDirectPolicy, routePlanReasonDirectPublish)
	routePlan.ExtraDetail = map[string]any{
		"direct":                     true,
		"requested_recipients":       append([]string(nil), requested...),
		"requested_recipients_count": len(requested),
	}
	if filtered := filteredRecipients(requested, manifest.Recipients); len(filtered) > 0 {
		routePlan.ExtraDetail["filtered_out_recipients"] = filtered
		routePlan.ExtraDetail["filtered_out_recipients_count"] = len(filtered)
	}
	return routePlan.EventDeliveryPlan(), nil
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
			resolveRoutedSubscribers:            eb.resolveRoutedSubscribersForEvent,
			resolveSubscribedRecipients:         eb.resolveSubscribedRecipientsForPlanning,
			resolveRoutedNodeInternalRecipients: eb.resolveInternalRecipientsForRoutedNodePlanning,
			describeSubscribersForEvent:         eb.describeSubscribersForEvent,
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors:  eb.activeAgentDescriptors,
			loadActiveTargetDescriptors: eb.activeTargetDescriptors,
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

func routedEventKeysForPlan(evt events.Event) []string {
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if eventType == "" {
		return nil
	}
	out := []string{eventType}
	if concrete := concreteFlowInstanceEventKey(evt); concrete != "" {
		out = append(out, concrete)
	}
	return uniqueStrings(out)
}

func concreteFlowInstanceEventKey(evt events.Event) string {
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if eventType == "" || flowInstance == "" {
		return ""
	}
	staticScope := runtimeflowidentity.SemanticScopeFromInstancePath(flowInstance)
	if staticScope == "" {
		return ""
	}
	localEvent := eventContextLocalEventForFlowInstance(eventType, staticScope)
	if localEvent == "" {
		return ""
	}
	return flowInstance + "/" + localEvent
}

func eventContextLocalEventForFlowInstance(eventType, staticScope string) string {
	eventType = strings.Trim(strings.TrimSpace(eventType), "/")
	staticScope = strings.Trim(strings.TrimSpace(staticScope), "/")
	if eventType == "" || staticScope == "" {
		return ""
	}
	if strings.HasPrefix(eventType, staticScope+"/") {
		localEvent := strings.TrimPrefix(eventType, staticScope+"/")
		if localEvent == "" || strings.Contains(localEvent, "/") {
			return ""
		}
		return localEvent
	}
	if strings.Contains(eventType, "/") {
		return ""
	}
	return eventType
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

func filterDeliveryRecipientCandidates(evt events.Event, recipients []deliveryRecipientCandidate, descriptors map[string]ActiveAgentDescriptor, targetDescriptors []ActiveTargetDescriptor) deliveryRecipientManifest {
	recipients = normalizeDeliveryRecipientCandidates(recipients)
	eventEntityID := strings.TrimSpace(evt.EntityID())
	targets := eventDeliveryTargetRoutes(evt)
	if len(recipients) == 0 {
		return deliveryRecipientManifest{
			TargetFailure: targetDeliveryFailure(evt, targetDescriptors),
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
		manifest.TargetFailure = targetDeliveryFailure(evt, targetDescriptors)
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

func internalDeliveryIntentsForPlan(evt events.Event, recipients, persisted []string, routed []Subscriber) []RoutePlanDeliveryIntent {
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
	return routePlanDeliveryIntentsFromRoutes(out, routePlanSourceInternalTarget, routePlanReasonInternalCarrier)
}

func routedNodeDeliveryIntentsForNoTargetEvent(evt events.Event, routed []Subscriber, recipients, persisted []string) []RoutePlanDeliveryIntent {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	var intents []RoutePlanDeliveryIntent
	if routes := routedConcreteNoTargetNodeDeliveryRoutes(evt, routed); len(routes) > 0 {
		intents = append(intents, routePlanDeliveryIntentsFromRoutes(routes, routePlanSourceConcreteNodeRoute, routePlanReasonRouteTableNode)...)
	}
	if routes := routedScopedNoTargetNodeDeliveryRoutes(evt, routed); len(routes) > 0 {
		intents = append(intents, routePlanDeliveryIntentsFromRoutes(routes, routePlanSourceScopedNodeRoute, routePlanReasonRouteTableNode)...)
	}
	if routes := routedWildcardStaticServiceNoTargetNodeDeliveryRoutes(evt, routed); len(routes) > 0 {
		intents = append(intents, routePlanDeliveryIntentsFromRoutes(routes, routePlanSourceScopedNodeRoute, routePlanReasonRouteTableNode)...)
	}
	if len(intents) > 0 {
		return intents
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
	return routePlanDeliveryIntentsFromRoutes(out, routePlanSourceConcreteNodeRoute, routePlanReasonRouteTableNode)
}

func routedConcreteNoTargetNodeDeliveryRoutes(evt events.Event, routed []Subscriber) []events.DeliveryRoute {
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if flowInstance == "" || eventType == "" || !strings.HasPrefix(eventType, flowInstance+"/") {
		return nil
	}
	eventEntityID := strings.TrimSpace(evt.EntityID())
	nodeIDs := make(map[string]struct{}, len(routed))
	for _, subscriber := range routed {
		if !routedNodeMatchesConcreteEventTypeFlowInstance(evt, subscriber) {
			continue
		}
		id := strings.TrimSpace(subscriber.ID)
		if id == "" {
			continue
		}
		nodeIDs[id] = struct{}{}
	}
	if len(nodeIDs) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(nodeIDs))
	for _, recipient := range sortedStringKeys(nodeIDs) {
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

func routedScopedNoTargetNodeDeliveryRoutes(evt events.Event, routed []Subscriber) []events.DeliveryRoute {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if eventType == "" {
		return nil
	}
	if !strings.Contains(eventType, "/") && (flowInstance == "" || concreteFlowInstanceEventKey(evt) == "") {
		return nil
	}
	eventEntityID := strings.TrimSpace(evt.EntityID())
	out := make([]events.DeliveryRoute, 0, len(routed))
	for _, subscriber := range routed {
		targetFlowInstance, ok := routedScopedNoTargetNodeDeliveryFlowInstance(evt, subscriber)
		if !ok {
			continue
		}
		out = append(out, events.DeliveryRoute{
			SubscriberType: "node",
			SubscriberID:   strings.TrimSpace(subscriber.ID),
			Target: events.RouteIdentity{
				FlowInstance: targetFlowInstance,
				EntityID:     eventEntityID,
			},
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

func routedWildcardStaticServiceNoTargetNodeDeliveryRoutes(evt events.Event, routed []Subscriber) []events.DeliveryRoute {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if eventType == "" {
		return nil
	}
	eventEntityID := strings.TrimSpace(evt.EntityID())
	out := make([]events.DeliveryRoute, 0, len(routed))
	for _, subscriber := range routed {
		id := strings.TrimSpace(subscriber.ID)
		if id == "" || strings.TrimSpace(subscriber.Type) != "node" {
			continue
		}
		path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
		if path == "" {
			continue
		}
		matchPattern := strings.Trim(strings.TrimSpace(subscriber.MatchPattern), "/")
		if matchPattern == "" || !strings.Contains(matchPattern, "*") || !RouteMatches(matchPattern, eventType) {
			continue
		}
		if routedNodeMatchesConcreteEventTypeFlowInstance(evt, subscriber) {
			continue
		}
		out = append(out, events.DeliveryRoute{
			SubscriberType: "node",
			SubscriberID:   id,
			Target: events.RouteIdentity{
				FlowInstance: path,
				EntityID:     eventEntityID,
			},
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

func routedScopedNoTargetNodeDeliveryFlowInstance(evt events.Event, subscriber Subscriber) (string, bool) {
	if strings.TrimSpace(subscriber.ID) == "" || strings.TrimSpace(subscriber.Type) == "agent" {
		return "", false
	}
	if strings.Contains(strings.TrimSpace(subscriber.MatchPattern), "*") {
		return "", false
	}
	path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	if path == "" {
		return "", false
	}
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if flowInstance == "" {
		if routedNodeMatchesScopedNoTargetEvent(evt, subscriber) {
			return path, true
		}
		return "", false
	}
	if routedNodeMatchesConcreteFlowInstanceEvent(evt, subscriber) {
		return flowInstance, true
	}
	if target := routedDescendantStaticFlowInstanceTarget(evt, subscriber); target != "" {
		return target, true
	}
	if target := routedStaticCrossFlowInstanceTarget(evt, subscriber); target != "" {
		return target, true
	}
	return "", false
}

func routedDescendantStaticFlowInstanceTarget(evt events.Event, subscriber Subscriber) string {
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	matchPattern := strings.Trim(strings.TrimSpace(subscriber.MatchPattern), "/")
	if flowInstance == "" || path == "" || eventType == "" || eventType != matchPattern || !strings.HasPrefix(eventType, path+"/") {
		return ""
	}
	staticScope := strings.Trim(strings.TrimSpace(runtimeflowidentity.SemanticScopeFromInstancePath(flowInstance)), "/")
	if staticScope == "" || path == staticScope || !strings.HasPrefix(path, staticScope+"/") {
		return ""
	}
	return flowInstance + strings.TrimPrefix(path, staticScope)
}

func routedStaticCrossFlowInstanceTarget(evt events.Event, subscriber Subscriber) string {
	path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	matchPattern := strings.Trim(strings.TrimSpace(subscriber.MatchPattern), "/")
	if path == "" || eventType == "" || eventType != matchPattern {
		return ""
	}
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if flowInstance == "" {
		return ""
	}
	staticScope := strings.Trim(strings.TrimSpace(runtimeflowidentity.SemanticScopeFromInstancePath(flowInstance)), "/")
	if staticScope != "" && (path == staticScope || strings.HasPrefix(path, staticScope+"/")) {
		return ""
	}
	return path
}

func routedNodeDeliveryIntentsForNoRecipientFlowInstanceEvent(evt events.Event, routed []Subscriber, recipients, persisted []string) []RoutePlanDeliveryIntent {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	if len(recipients) > 0 || len(persisted) > 0 {
		return nil
	}
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if flowInstance == "" {
		return nil
	}
	eventEntityID := strings.TrimSpace(evt.EntityID())
	nodeIDs := make(map[string]struct{}, len(routed))
	for _, subscriber := range routed {
		if !routedNodeMatchesConcreteFlowInstanceEvent(evt, subscriber) {
			continue
		}
		id := strings.TrimSpace(subscriber.ID)
		if id == "" {
			continue
		}
		nodeIDs[id] = struct{}{}
	}
	if len(nodeIDs) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(nodeIDs))
	for _, recipient := range sortedStringKeys(nodeIDs) {
		out = append(out, events.DeliveryRoute{
			SubscriberType: "node",
			SubscriberID:   recipient,
			Target: events.RouteIdentity{
				FlowInstance: flowInstance,
				EntityID:     eventEntityID,
			},
		})
	}
	return routePlanDeliveryIntentsFromRoutes(out, routePlanSourceConcreteNodeRoute, routePlanReasonRouteTableNode)
}

func routedRootNodeDeliveryIntentsForNoTargetEvent(evt events.Event, routed []Subscriber) []RoutePlanDeliveryIntent {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	rootNodeIDs := routedRootNodeSubscriberIDsForNoTargetEvent(evt, routed)
	if len(rootNodeIDs) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(rootNodeIDs))
	for _, recipient := range sortedStringKeys(rootNodeIDs) {
		out = append(out, events.DeliveryRoute{
			SubscriberType: "node",
			SubscriberID:   recipient,
		})
	}
	return routePlanDeliveryIntentsFromRoutes(out, routePlanSourceRootNodeRoute, routePlanReasonRouteTableNode)
}

func routedRootInputFlowNodeDeliveryIntentsForNoTargetEvent(evt events.Event, routed []Subscriber) []RoutePlanDeliveryIntent {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	if strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/") != "" {
		return nil
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if eventType == "" || strings.Contains(eventType, "/") {
		return nil
	}
	nodeIDs := make(map[string]struct{}, len(routed))
	for _, subscriber := range routed {
		if !routedRootInputFlowNodeMatchesNoTargetEvent(evt, subscriber) {
			continue
		}
		nodeIDs[strings.TrimSpace(subscriber.ID)] = struct{}{}
	}
	if len(nodeIDs) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(nodeIDs))
	for _, recipient := range sortedStringKeys(nodeIDs) {
		out = append(out, events.DeliveryRoute{
			SubscriberType: "node",
			SubscriberID:   recipient,
		})
	}
	return routePlanDeliveryIntentsFromRoutes(out, routePlanSourceRootInputFlowNode, routePlanReasonRouteTableNode)
}

func routedRootInputFlowNodeMatchesNoTargetEvent(evt events.Event, subscriber Subscriber) bool {
	if strings.TrimSpace(subscriber.ID) == "" || strings.TrimSpace(subscriber.Type) != "node" {
		return false
	}
	if strings.TrimSpace(subscriber.RouteSource) != "root_input_flow" {
		return false
	}
	if strings.Trim(strings.TrimSpace(subscriber.Path), "/") == "" {
		return false
	}
	if strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/") != "" {
		return false
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	return eventType != "" && !strings.Contains(eventType, "/")
}

func routedRootNodeSubscriberIDsForNoTargetEvent(evt events.Event, routed []Subscriber) map[string]struct{} {
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if eventType == "" {
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
	if strings.TrimSpace(subscriber.ID) == "" || strings.TrimSpace(subscriber.Type) != "node" {
		return false
	}
	if strings.Trim(strings.TrimSpace(subscriber.Path), "/") != "" {
		return false
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if eventType == "" {
		return false
	}
	if !strings.Contains(eventType, "/") {
		return true
	}
	matchPattern := strings.Trim(strings.TrimSpace(subscriber.MatchPattern), "/")
	return matchPattern != "" && !strings.Contains(matchPattern, "*") && eventType == matchPattern
}

func routedNodeInternalSubscriptionAliases(evt events.Event, routed []Subscriber) []string {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if eventType == "" {
		return nil
	}
	out := []string{eventType}
	if concrete := concreteFlowInstanceEventKey(evt); concrete != "" {
		out = append(out, concrete)
	}
	for _, subscriber := range routed {
		if !routedNodeMatchesConcreteFlowInstanceEvent(evt, subscriber) {
			continue
		}
		eventType := routedNodeConcreteEventKey(evt, subscriber)
		instancePath := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
		flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
		if instancePath == "" || instancePath != flowInstance || !strings.HasPrefix(eventType, instancePath+"/") {
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
	return routedNodeConcreteEventKey(evt, subscriber) != ""
}

func routedNodeMatchesScopedNoTargetEvent(evt events.Event, subscriber Subscriber) bool {
	if strings.TrimSpace(subscriber.ID) == "" || strings.TrimSpace(subscriber.Type) == "agent" {
		return false
	}
	if strings.Contains(strings.TrimSpace(subscriber.MatchPattern), "*") {
		return false
	}
	path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	if path == "" {
		return false
	}
	if strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/") != "" {
		return routedNodeMatchesConcreteFlowInstanceEvent(evt, subscriber)
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	matchPattern := strings.Trim(strings.TrimSpace(subscriber.MatchPattern), "/")
	return eventType != "" && eventType == matchPattern
}

func routedNodeMatchesConcreteEventTypeFlowInstance(evt events.Event, subscriber Subscriber) bool {
	if strings.TrimSpace(subscriber.ID) == "" || strings.TrimSpace(subscriber.Type) == "agent" {
		return false
	}
	instancePath := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if instancePath == "" || flowInstance == "" || eventType == "" {
		return false
	}
	return instancePath == flowInstance && strings.HasPrefix(eventType, flowInstance+"/")
}

func routedNodeConcreteEventKey(evt events.Event, subscriber Subscriber) string {
	if strings.TrimSpace(subscriber.ID) == "" || strings.TrimSpace(subscriber.Type) == "agent" {
		return ""
	}
	instancePath := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if instancePath == "" || flowInstance == "" || instancePath != flowInstance {
		staticScope := runtimeflowidentity.SemanticScopeFromInstancePath(flowInstance)
		if staticScope == "" || instancePath != staticScope {
			return ""
		}
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if eventType != "" && strings.HasPrefix(eventType, flowInstance+"/") {
		if instancePath == flowInstance {
			return eventType
		}
		return ""
	}
	return concreteFlowInstanceEventKey(evt)
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

func targetDeliveryFailure(evt events.Event, descriptors []ActiveTargetDescriptor) runtimepinrouting.TargetFailure {
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

func allTargetsHaveLiveDescriptor(targets []events.RouteIdentity, descriptors []ActiveTargetDescriptor) bool {
	if len(targets) == 0 {
		return true
	}
	if len(descriptors) == 0 {
		return false
	}
	for _, target := range targets {
		found := false
		for _, descriptor := range descriptors {
			if routeMatchesTargetDescriptor(target, descriptor) {
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
		return singular, routeMatchesAgentDescriptor(singular, descriptor)
	}
	if len(targets) == 0 {
		return events.RouteIdentity{}, true
	}
	for _, target := range targets {
		if routeMatchesAgentDescriptor(target, descriptor) {
			return target.Normalized(), true
		}
	}
	return events.RouteIdentity{}, false
}

func routeMatchesAgentDescriptor(route events.RouteIdentity, descriptor ActiveAgentDescriptor) bool {
	return routeMatchesTargetDescriptor(route, descriptor.TargetDescriptor())
}

func routeMatchesTargetDescriptor(route events.RouteIdentity, descriptor ActiveTargetDescriptor) bool {
	route = route.Normalized()
	descriptor = descriptor.Normalized()
	if descriptor.EntityID == "" && descriptor.FlowInstance == "" {
		return false
	}
	matched := false
	if route.EntityID != "" {
		if descriptor.EntityID != route.EntityID {
			return false
		}
		matched = true
	}
	if route.FlowInstance != "" {
		if descriptor.FlowInstance != route.FlowInstance {
			return false
		}
		matched = true
	}
	return matched
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
