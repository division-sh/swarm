package bus

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/google/uuid"
)

type deliveryRoutingResult struct {
	Recipients           []deliveryRecipientCandidate
	RoutedRecipients     []Subscriber
	SubscribedRecipients []string
	ExtraDetail          map[string]any
}

type deliveryRecipientCandidate struct {
	ID                string
	AgentIdentity     agentidentity.Identity
	PersistAsDelivery bool
	LiveAuthority     liveRecipientAuthority
	AgentRoute        *agentRouteHandle
}

type exactDirectRecipientsUnavailableError struct {
	identities []agentidentity.Identity
}

func (e *exactDirectRecipientsUnavailableError) Error() string {
	descriptions := make([]string, 0, len(e.identities))
	for _, identity := range e.identities {
		descriptions = append(descriptions, identity.Description())
	}
	return fmt.Sprintf("%s: %s", ErrExactDirectRecipientUnavailable, strings.Join(descriptions, ", "))
}

func (e *exactDirectRecipientsUnavailableError) Unwrap() error {
	return ErrExactDirectRecipientUnavailable
}

type deliveryRouteResolver struct {
	resolveRoutedSubscribers            func(events.Event) []Subscriber
	resolveSubscribedRecipients         func(string) []deliveryRecipientCandidate
	resolveRoutedNodeInternalRecipients func(events.Event, []Subscriber) []deliveryRecipientCandidate
	describeSubscribersForEvent         func(string, []Subscriber) []PublishDiagnosticRecipient
}

func (r deliveryRouteResolver) Resolve(evt events.Event) deliveryRoutingResult {
	routedRecipients := r.resolveRoutedSubscribers(evt)
	subscribedRecipients := make([]deliveryRecipientCandidate, 0, 8)
	for _, eventKey := range routedEventKeysForPlan(evt) {
		subscribedRecipients = append(subscribedRecipients, r.resolveSubscribedRecipients(eventKey)...)
	}
	subscribedRecipients = normalizeDeliveryRecipientCandidates(subscribedRecipients)
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
	LiveRecipients      []deliveryRecipientCandidate
	Recipients          []string
	PersistedRecipients []string
	DeliveryTargets     map[string]events.RouteIdentity
	DeliveryRoutes      []events.DeliveryRoute
	TargetFailure       runtimepinrouting.TargetFailure
}

type deliveryRecipientPolicy struct {
	loadActiveAgentDescriptors  func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error)
	loadActiveTargetDescriptors func(context.Context) ([]ActiveTargetDescriptor, bool, error)
	requireTargetOwners         bool
}

func (p deliveryRecipientPolicy) Evaluate(ctx context.Context, evt events.Event, recipients []deliveryRecipientCandidate) (deliveryRecipientManifest, error) {
	projection, projected := selectedRunTargetOwnerProjectionFromContext(ctx)
	if !projected {
		var err error
		projection, err = p.loadSelectedRunTargetOwnerProjection(ctx)
		if err != nil {
			return deliveryRecipientManifest{}, err
		}
	}
	descriptors := projection.agents
	ok := projection.agentsAvailable
	targetDescriptors := projection.descriptors
	targetDescriptorsOK := projection.targetsAvailable
	if !ok {
		liveRecipients := normalizeDeliveryRecipientCandidates(recipients)
		persistedRecipients := persistedDeliveryRecipientCandidates(liveRecipients)
		targets := deliveryTargetsForManifest(evt, deliveryRecipientIDs(persistedRecipients), nil)
		manifest := deliveryRecipientManifest{
			LiveRecipients:      liveRecipients,
			Recipients:          deliveryRecipientIDs(recipients),
			PersistedRecipients: deliveryRecipientIDs(persistedRecipients),
			DeliveryTargets:     targets,
			DeliveryRoutes:      agentDeliveryRoutesForCandidates(persistedRecipients, targets),
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
	connectPlanner  connectRoutePlanResolver
	rootFlowID      string
}

func newDeliveryPlanner(routeResolver deliveryRouteResolver, recipientPolicy deliveryRecipientPolicy, connectPlanners ...connectRoutePlanResolver) deliveryPlanner {
	planner := deliveryPlanner{
		routeResolver:   routeResolver,
		recipientPolicy: recipientPolicy,
	}
	if len(connectPlanners) > 0 {
		planner.connectPlanner = connectPlanners[0]
	}
	return planner
}

func (p deliveryPlanner) Plan(ctx context.Context, evt events.Event) (RoutePlan, error) {
	routePlan := newRoutePlan(evt)
	if evt.Type() == events.EventType("platform.runtime_log") {
		return routePlan, nil
	}
	for attempt := 0; attempt < 3; attempt++ {
		snapshot := p.connectPlanner.routeTable.snapshotGeneration()
		planned, err := p.planAtGeneration(ctx, evt)
		if err != nil {
			return RoutePlan{}, err
		}
		if p.connectPlanner.routeTable.snapshotGenerationCurrent(snapshot) {
			return planned, nil
		}
	}
	return RoutePlan{}, staleConnectRoutePlanSnapshotError{}
}

func (p deliveryPlanner) planAtGeneration(ctx context.Context, evt events.Event) (RoutePlan, error) {
	routePlan := newRoutePlan(evt)
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	projection, err := p.recipientPolicy.loadSelectedRunTargetOwnerProjection(ctx)
	if err != nil {
		return RoutePlan{}, err
	}
	ctx = withSelectedRunTargetOwnerProjection(ctx, projection)
	connectPlan, err := p.connectPlanner.Plan(ctx, evt)
	if err != nil {
		return RoutePlan{}, err
	}
	if connectPlan.Matched {
		projection, err = projection.withActivationPlans(connectPlan.ActivationPlans)
		if err != nil {
			return RoutePlan{}, err
		}
		return projection.resolveRoutePlan(routePlanFromConnectRouteDispatch(evt, connectPlan))
	}
	routing := p.routeResolver.Resolve(evt)
	manifest, err := p.recipientPolicy.Evaluate(ctx, evt, routing.Recipients)
	if err != nil {
		return RoutePlan{}, err
	}
	routePlan = routePlanFromManifest(evt, manifest, routeIntentProducerAgentPolicy)
	routePlan.ConnectEvaluation = connectPlan.Evaluation
	recipients := routePlan.RecipientIDs()
	persisted := routePlan.PersistedRecipientIDs()
	routePlan.AddDeliveryIntents(routedRootNodeDeliveryIntentsForNoTargetEvent(evt, routing.RoutedRecipients, p.rootFlowID)...)
	routePlan.AddDeliveryIntents(routedRootInputFlowNodeDeliveryIntentsForNoTargetEvent(evt, routing.RoutedRecipients)...)
	routePlan.AddDeliveryIntents(routedNodeDeliveryIntentsForNoRecipientFlowInstanceEvent(evt, routing.RoutedRecipients, recipients, persisted)...)
	routePlan.AddDeliveryIntents(routedNodeDeliveryIntentsForNoTargetEvent(evt, routing.RoutedRecipients, recipients, persisted)...)
	routePlan.AddDeliveryIntents(targetedRoutedNodeDeliveryIntents(evt, routing.RoutedRecipients)...)
	routePlan.AddDeliveryIntents(internalDeliveryIntentsForPlan(evt, recipients, persisted, routing.RoutedRecipients)...)
	extraDetail := cloneAnyMap(routing.ExtraDetail)
	if !routePlan.TargetFailure.Empty() && hasInternalRoutedSubscriberForTarget(evt, routing.RoutedRecipients) {
		routePlan.TargetFailure = 0
	}
	routePlan.RoutedRecipients = routing.RoutedRecipients
	routePlan.SubscribedRecipients = routing.SubscribedRecipients
	routePlan.ExtraDetail = extraDetail
	return projection.resolveRoutePlan(routePlan)
}

func routePlanFromConnectRouteDispatch(evt events.Event, connectPlan connectRoutePlanDispatch) RoutePlan {
	routePlan := newRoutePlan(evt)
	routePlan.ConnectEvaluation = connectPlan.Evaluation
	if !connectPlan.Failure.Empty() {
		routePlan.MarkCanonicalRouteFailedClosed(routeIntentProducerConnectRoutePlan, connectPlan.Failure)
	} else {
		routePlan.MarkCanonicalRouteMatched(routeIntentProducerConnectRoutePlan)
		routePlan.AddLiveRecipients(connectPlan.LiveRecipients...)
		routePlan.AddDeliveryIntents(connectPlan.DeliveryIntents...)
	}
	routePlan.RoutedRecipients = dedupeSubscribers(connectPlan.RoutedRecipients)
	routePlan.ExtraDetail = cloneAnyMap(connectPlan.ExtraDetail)
	routePlan.ReplyContextConsumed = connectPlan.ReplyContextConsumed
	routePlan.ActivationPlans = append([]runtimepipeline.FlowInstanceActivationPlan(nil), connectPlan.ActivationPlans...)
	routePlan.ReplyCreations = append([]runtimereplycontext.Record(nil), connectPlan.ReplyCreations...)
	routePlan.ReplyClaims = append([]runtimereplycontext.ClaimCommand(nil), connectPlan.ReplyClaims...)
	return routePlan.Normalized()
}

func (p deliveryPlanner) PlanDirect(ctx context.Context, evt events.Event, recipients []string) (RoutePlan, error) {
	routePlan := newRoutePlan(evt)
	if evt.Type() == events.EventType("platform.runtime_log") {
		return routePlan, nil
	}
	requested := uniqueStrings(recipients)
	if len(requested) == 0 {
		return RoutePlan{}, errors.New("direct delivery recipients are required")
	}
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	projection, err := p.recipientPolicy.loadSelectedRunTargetOwnerProjection(ctx)
	if err != nil {
		return RoutePlan{}, err
	}
	ctx = withSelectedRunTargetOwnerProjection(ctx, projection)
	manifest, err := p.recipientPolicy.Evaluate(ctx, evt, agentDeliveryRecipientCandidates(requested))
	if err != nil {
		return RoutePlan{}, err
	}
	if err := rejectAmbiguousDirectManifest(requested, manifest.LiveRecipients); err != nil {
		return RoutePlan{}, err
	}
	routePlan = routePlanFromManifest(evt, manifest, routeIntentProducerDirectPolicy)
	routePlan.ExtraDetail = map[string]any{
		"direct":                     true,
		"requested_recipients":       append([]string(nil), requested...),
		"requested_recipients_count": len(requested),
	}
	if filtered := filteredRecipients(requested, manifest.Recipients); len(filtered) > 0 {
		routePlan.ExtraDetail["filtered_out_recipients"] = filtered
		routePlan.ExtraDetail["filtered_out_recipients_count"] = len(filtered)
	}
	return projection.resolveRoutePlan(routePlan)
}

func (p deliveryPlanner) PlanExactDirect(ctx context.Context, evt events.Event, routes []events.DeliveryRoute) (RoutePlan, error) {
	routePlan := newRoutePlan(evt)
	if evt.Type() == events.EventType("platform.runtime_log") {
		return routePlan, nil
	}
	routes = events.NormalizeDeliveryRoutes(routes)
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		return RoutePlan{}, err
	}
	candidates := make([]deliveryRecipientCandidate, 0, len(routes))
	for _, route := range routes {
		if !route.Recipient.IsAgent() {
			return RoutePlan{}, fmt.Errorf("exact direct delivery route must identify an agent subscriber")
		}
		candidates = append(candidates, deliveryRecipientCandidate{
			ID:                route.Recipient.ID(),
			AgentIdentity:     route.AgentIdentity,
			PersistAsDelivery: true,
		})
	}
	if len(candidates) == 0 {
		return RoutePlan{}, errors.New("exact direct delivery routes are required")
	}
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	projection, err := p.recipientPolicy.loadSelectedRunTargetOwnerProjection(ctx)
	if err != nil {
		return RoutePlan{}, err
	}
	ctx = withSelectedRunTargetOwnerProjection(ctx, projection)
	manifest, err := p.recipientPolicy.Evaluate(ctx, evt, candidates)
	if err != nil {
		return RoutePlan{}, err
	}
	available := make(map[agentidentity.Identity]struct{}, len(manifest.LiveRecipients))
	for _, recipient := range manifest.LiveRecipients {
		if !recipient.AgentIdentity.IsZero() {
			available[recipient.AgentIdentity.Normalize()] = struct{}{}
		}
	}
	missing := make([]agentidentity.Identity, 0)
	for _, route := range routes {
		identity := route.AgentIdentity.Normalize()
		if _, ok := available[identity]; !ok {
			missing = append(missing, identity)
		}
	}
	if len(missing) > 0 {
		return RoutePlan{}, &exactDirectRecipientsUnavailableError{identities: missing}
	}
	routePlan = routePlanFromManifest(evt, manifest, routeIntentProducerDirectPolicy)
	routePlan.DeliveryIntents = routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerDirectPolicy)
	routePlan.ExtraDetail = map[string]any{
		"direct":                true,
		"exact_routes":          true,
		"requested_route_count": len(routes),
	}
	return projection.resolveRoutePlan(routePlan)
}

func rejectAmbiguousDirectManifest(recipients []string, liveRecipients []deliveryRecipientCandidate) error {
	for _, recipient := range recipients {
		matches := make([]agentidentity.Identity, 0, 2)
		seen := map[agentidentity.Identity]struct{}{}
		for _, liveRecipient := range liveRecipients {
			identity := liveRecipient.AgentIdentity.Normalize()
			if liveRecipient.ID != recipient || identity.IsZero() {
				continue
			}
			if _, ok := seen[identity]; ok {
				continue
			}
			seen[identity] = struct{}{}
			matches = append(matches, identity)
		}
		if len(matches) <= 1 {
			continue
		}
		sort.Slice(matches, func(left, right int) bool {
			return agentidentity.Less(matches[left], matches[right])
		})
		candidates := make([]string, 0, len(matches))
		for _, identity := range matches {
			candidates = append(candidates, identity.Description())
		}
		return fmt.Errorf(
			"direct recipient agent_id %q is ambiguous; provide an exact agent route; candidates: %s",
			recipient,
			strings.Join(candidates, ", "),
		)
	}
	return nil
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

func cloneAnyMap(in map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (eb *EventBus) newEventBusDeliveryPlanner() deliveryPlanner {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers:            eb.resolveRoutedSubscribersForEvent,
			resolveSubscribedRecipients:         eb.resolveSubscribedRecipientsForPlanning,
			resolveRoutedNodeInternalRecipients: eb.resolveInternalRecipientsForRoutedNodePlanning,
			describeSubscribersForEvent:         eb.describeSubscribersForEvent,
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors:  eb.activeAgentDescriptors,
			loadActiveTargetDescriptors: eb.activeTargetDescriptors,
			requireTargetOwners:         !eb.ephemeral,
		},
		eb.connectRoutePlanner,
	)
	if eb.semanticSource != nil {
		planner.rootFlowID = strings.TrimSpace(eb.semanticSource.WorkflowName())
	}
	return planner
}

func normalizeDeliveryRecipientCandidates(in []deliveryRecipientCandidate) []deliveryRecipientCandidate {
	if len(in) == 0 {
		return nil
	}
	out := make([]deliveryRecipientCandidate, 0, len(in))
	type candidateKey struct {
		identity agentidentity.Identity
		id       string
	}
	indexByKey := make(map[candidateKey]int, len(in))
	for _, candidate := range in {
		candidate.ID = strings.TrimSpace(candidate.ID)
		if candidate.ID == "" {
			continue
		}
		candidate.AgentIdentity = candidate.AgentIdentity.Normalize()
		if !candidate.AgentIdentity.IsZero() {
			if err := candidate.AgentIdentity.Validate(); err != nil || candidate.AgentIdentity.AgentID() != candidate.ID {
				continue
			}
		}
		key := candidateKey{id: candidate.ID}
		if !candidate.AgentIdentity.IsZero() {
			key = candidateKey{identity: candidate.AgentIdentity}
		}
		if idx, ok := indexByKey[key]; ok {
			out[idx].PersistAsDelivery = out[idx].PersistAsDelivery || candidate.PersistAsDelivery
			out[idx] = mergeDeliveryRecipientAuthority(out[idx], candidate)
			continue
		}
		indexByKey[key] = len(out)
		out = append(out, candidate)
	}
	return out
}

func mergeDeliveryRecipientAuthority(current, candidate deliveryRecipientCandidate) deliveryRecipientCandidate {
	if current.LiveAuthority.Normalized() == liveRecipientAuthorityIdentity || candidate.LiveAuthority.Normalized() == liveRecipientAuthorityIdentity {
		current.LiveAuthority = liveRecipientAuthorityIdentity
		current.AgentRoute = nil
		return current
	}
	current.LiveAuthority = liveRecipientAuthorityAgentRoute
	if current.AgentRoute == nil {
		current.AgentRoute = candidate.AgentRoute
	}
	return current
}

func routedSubscriberCandidates(in []Subscriber) []deliveryRecipientCandidate {
	if len(in) == 0 {
		return nil
	}
	out := make([]deliveryRecipientCandidate, 0, len(in))
	for _, subscriber := range in {
		if subscriber.Recipient.Empty() {
			continue
		}
		if !subscriber.Recipient.IsAgent() {
			continue
		}
		out = append(out, deliveryRecipientCandidate{
			ID:                subscriber.Recipient.ID(),
			AgentIdentity:     subscriber.AgentIdentity,
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
	out = append(out, targetedConcreteEventKeysForPlan(evt)...)
	return uniqueStrings(out)
}

func targetedConcreteEventKeysForPlan(evt events.Event) []string {
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if eventType == "" {
		return nil
	}
	targets := eventDeliveryTargetRoutes(evt)
	if len(targets) == 0 {
		return nil
	}
	out := make([]string, 0, len(targets))
	for _, target := range targets {
		target = target.Normalized()
		flowInstance := strings.Trim(strings.TrimSpace(target.FlowInstance), "/")
		if flowInstance == "" {
			continue
		}
		staticScope := runtimeflowidentity.SemanticScopeFromFlowInstanceRef(flowInstance)
		if staticScope == "" {
			continue
		}
		localEvent := eventContextLocalEventForFlowInstance(eventType, staticScope)
		if localEvent == "" {
			continue
		}
		out = append(out, flowInstance+"/"+localEvent)
		out = append(out, localEvent)
	}
	return uniqueStrings(out)
}

func concreteFlowInstanceEventKey(evt events.Event) string {
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if eventType == "" || flowInstance == "" {
		return ""
	}
	staticScope := runtimeflowidentity.SemanticScopeFromFlowInstanceRef(flowInstance)
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

func persistedDeliveryRecipientCandidates(in []deliveryRecipientCandidate) []deliveryRecipientCandidate {
	if len(in) == 0 {
		return nil
	}
	out := make([]deliveryRecipientCandidate, 0, len(in))
	for _, candidate := range normalizeDeliveryRecipientCandidates(in) {
		if candidate.PersistAsDelivery {
			out = append(out, candidate)
		}
	}
	return out
}

func filterDeliveryRecipientCandidates(
	evt events.Event,
	recipients []deliveryRecipientCandidate,
	descriptors map[agentidentity.Identity]ActiveAgentDescriptor,
	targetDescriptors []ActiveTargetDescriptor,
) deliveryRecipientManifest {
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
	allowedCandidates := make([]deliveryRecipientCandidate, 0, len(recipients))
	persisted := make([]string, 0, len(recipients))
	deliveryTargets := map[string]events.RouteIdentity{}
	deliveryRoutes := make([]events.DeliveryRoute, 0, len(recipients))
	for _, recipient := range recipients {
		if !recipient.PersistAsDelivery {
			allowed = append(allowed, recipient.ID)
			allowedCandidates = append(allowedCandidates, recipient)
			continue
		}
		for _, descriptor := range matchingAgentDescriptors(recipient, descriptors) {
			descriptor = descriptor.Normalized()
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
			scoped := recipient
			scoped.AgentIdentity = descriptor.Identity
			allowed = append(allowed, scoped.ID)
			allowedCandidates = append(allowedCandidates, scoped)
			persisted = append(persisted, scoped.ID)
			if !target.Empty() {
				deliveryTargets[scoped.ID] = target
			}
			deliveryRoutes = append(deliveryRoutes, events.DeliveryRoute{
				Recipient:     events.MustAgentDeliveryRecipient(scoped.ID),
				AgentIdentity: scoped.AgentIdentity,
				Target:        target,
			})
		}
	}
	persisted = uniqueStrings(persisted)
	manifest := deliveryRecipientManifest{
		LiveRecipients:      normalizeDeliveryRecipientCandidates(allowedCandidates),
		Recipients:          uniqueStrings(allowed),
		PersistedRecipients: persisted,
		DeliveryTargets:     deliveryTargetsForManifest(evt, persisted, deliveryTargets),
		DeliveryRoutes:      events.NormalizeDeliveryRoutes(deliveryRoutes),
	}
	if len(targets) > 0 && len(manifest.LiveRecipients) == 0 {
		manifest.TargetFailure = targetDeliveryFailure(evt, targetDescriptors)
	}
	return manifest
}

func matchingAgentDescriptors(
	recipient deliveryRecipientCandidate,
	descriptors map[agentidentity.Identity]ActiveAgentDescriptor,
) []ActiveAgentDescriptor {
	if !recipient.AgentIdentity.IsZero() {
		descriptor, ok := descriptors[recipient.AgentIdentity.Normalize()]
		if !ok {
			return nil
		}
		return []ActiveAgentDescriptor{descriptor.Normalized()}
	}
	out := make([]ActiveAgentDescriptor, 0, 1)
	for identity, descriptor := range descriptors {
		descriptor = descriptor.Normalized()
		if identity.AgentID() != recipient.ID || descriptor.Identity != identity {
			continue
		}
		out = append(out, descriptor)
	}
	sort.Slice(out, func(left, right int) bool {
		return agentidentity.Less(out[left].Identity, out[right].Identity)
	})
	return out
}

func agentDeliveryRoutesForCandidates(
	recipients []deliveryRecipientCandidate,
	deliveryTargets map[string]events.RouteIdentity,
) []events.DeliveryRoute {
	recipients = persistedDeliveryRecipientCandidates(recipients)
	if len(recipients) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(recipients))
	for _, recipient := range recipients {
		if err := recipient.AgentIdentity.Validate(); err != nil {
			continue
		}
		out = append(out, events.DeliveryRoute{
			Recipient:     events.MustAgentDeliveryRecipient(recipient.ID),
			AgentIdentity: recipient.AgentIdentity,
			Target:        deliveryTargets[strings.TrimSpace(recipient.ID)],
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
				Recipient: events.MustNodeDeliveryRecipient(recipient),
				Target:    target,
			})
		}
	}
	return routePlanDeliveryIntentsFromRoutes(out, routeIntentProducerInternalTargetCarrier)
}

func targetedRoutedNodeDeliveryIntents(evt events.Event, routed []Subscriber) []RoutePlanDeliveryIntent {
	routes := targetedRoutedNodeDeliveryRoutes(evt, routed)
	if len(routes) == 0 {
		return nil
	}
	return routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerInternalTargetRoute)
}

func targetedRoutedNodeDeliveryRoutes(evt events.Event, routed []Subscriber) []events.DeliveryRoute {
	targets := eventDeliveryTargetRoutes(evt)
	if len(targets) == 0 || len(routed) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(targets)*len(routed))
	for _, target := range targets {
		target = target.Normalized()
		if target.Empty() {
			continue
		}
		for _, subscriber := range routed {
			if !subscriber.Recipient.IsNode() {
				continue
			}
			if !routeMatchesInternalSubscriber(target, subscriber) {
				continue
			}
			out = append(out, events.DeliveryRoute{
				Recipient: subscriber.Recipient,
				Target:    target,
			})
		}
	}
	return events.NormalizeDeliveryRoutes(out)
}

func routedNodeDeliveryIntentsForNoTargetEvent(evt events.Event, routed []Subscriber, recipients, persisted []string) []RoutePlanDeliveryIntent {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	var intents []RoutePlanDeliveryIntent
	if routes := routedConcreteNoTargetNodeDeliveryRoutes(evt, routed); len(routes) > 0 {
		intents = append(intents, routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerConcreteNodeRoute)...)
	}
	if scoped := routedScopedNoTargetNodeDeliveryIntents(evt, routed); len(scoped) > 0 {
		intents = append(intents, scoped...)
	}
	if routes := routedWildcardStaticServiceNoTargetNodeDeliveryRoutes(evt, routed); len(routes) > 0 {
		intents = append(intents, routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerScopedNodeRoute)...)
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
			Recipient: events.MustNodeDeliveryRecipient(recipient),
			Target:    routedNodeTargetRoute(evt, flowInstance),
		})
	}
	return routePlanDeliveryIntentsFromRoutes(out, routeIntentProducerConcreteNodeRoute)
}

func routedConcreteNoTargetNodeDeliveryRoutes(evt events.Event, routed []Subscriber) []events.DeliveryRoute {
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if flowInstance == "" || eventType == "" || !strings.HasPrefix(eventType, flowInstance+"/") {
		return nil
	}
	nodeIDs := make(map[string]struct{}, len(routed))
	for _, subscriber := range routed {
		if !routedNodeMatchesConcreteEventTypeFlowInstance(evt, subscriber) {
			continue
		}
		if !subscriber.Recipient.IsNode() {
			continue
		}
		nodeIDs[subscriber.Recipient.ID()] = struct{}{}
	}
	if len(nodeIDs) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(nodeIDs))
	for _, recipient := range sortedStringKeys(nodeIDs) {
		out = append(out, events.DeliveryRoute{
			Recipient: events.MustNodeDeliveryRecipient(recipient),
			Target:    routedNodeTargetRoute(evt, flowInstance),
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

func routedScopedNoTargetNodeDeliveryIntents(evt events.Event, routed []Subscriber) []RoutePlanDeliveryIntent {
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
	out := make([]RoutePlanDeliveryIntent, 0, len(routed))
	for _, subscriber := range routed {
		targetFlowInstance, structural, ok := routedScopedNoTargetNodeDeliveryFlowInstance(evt, subscriber)
		if !ok {
			continue
		}
		out = append(out, RoutePlanDeliveryIntent{
			Recipient: subscriber.Recipient, Target: routedNodeTargetRoute(evt, targetFlowInstance),
			Producer: routeIntentProducerScopedNodeRoute, Persist: true, AllowStructuralOwner: structural,
		})
	}
	return normalizeRoutePlanDeliveryIntents(out)
}

func routedWildcardStaticServiceNoTargetNodeDeliveryRoutes(evt events.Event, routed []Subscriber) []events.DeliveryRoute {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if eventType == "" {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(routed))
	for _, subscriber := range routed {
		if !subscriber.Recipient.IsNode() {
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
			Recipient: subscriber.Recipient,
			Target:    routedNodeTargetRoute(evt, path),
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

func routedScopedNoTargetNodeDeliveryFlowInstance(evt events.Event, subscriber Subscriber) (string, bool, bool) {
	if !subscriber.Recipient.IsNode() {
		return "", false, false
	}
	if strings.Contains(strings.TrimSpace(subscriber.MatchPattern), "*") {
		return "", false, false
	}
	path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	if path == "" {
		return "", false, false
	}
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if flowInstance == "" {
		if routedNodeMatchesScopedNoTargetEvent(evt, subscriber) {
			return path, true, true
		}
		return "", false, false
	}
	if routedNodeMatchesConcreteFlowInstanceEvent(evt, subscriber) {
		return flowInstance, false, true
	}
	if target := routedDescendantStaticFlowInstanceTarget(evt, subscriber); target != "" {
		return target, true, true
	}
	if target := routedStaticCrossFlowInstanceTarget(evt, subscriber); target != "" {
		return target, true, true
	}
	return "", false, false
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
	nodeIDs := make(map[string]struct{}, len(routed))
	for _, subscriber := range routed {
		if !routedNodeMatchesConcreteFlowInstanceEvent(evt, subscriber) {
			continue
		}
		if !subscriber.Recipient.IsNode() {
			continue
		}
		nodeIDs[subscriber.Recipient.ID()] = struct{}{}
	}
	if len(nodeIDs) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(nodeIDs))
	for _, recipient := range sortedStringKeys(nodeIDs) {
		out = append(out, events.DeliveryRoute{
			Recipient: events.MustNodeDeliveryRecipient(recipient),
			Target:    routedNodeTargetRoute(evt, flowInstance),
		})
	}
	return routePlanDeliveryIntentsFromRoutes(out, routeIntentProducerConcreteNodeRoute)
}

func routedNodeTargetRoute(evt events.Event, targetFlowInstance string) events.RouteIdentity {
	flowID := runtimeflowidentity.SemanticScopeFromFlowInstanceRef(targetFlowInstance)
	return events.RouteIdentity{
		FlowID:       flowID,
		FlowInstance: targetFlowInstance,
	}.Normalized()
}

func routedRootNodeDeliveryIntentsForNoTargetEvent(evt events.Event, routed []Subscriber, rootFlowID string) []RoutePlanDeliveryIntent {
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
			Recipient: events.MustNodeDeliveryRecipient(recipient),
			Target: events.RouteIdentity{
				FlowID: strings.TrimSpace(rootFlowID), FlowInstance: strings.TrimSpace(evt.RunID()),
			},
		})
	}
	return routePlanDeliveryIntentsFromRoutes(out, routeIntentProducerRootNodeRoute)
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
	nodePaths := make(map[string]string, len(routed))
	for _, subscriber := range routed {
		if !routedRootInputFlowNodeMatchesNoTargetEvent(evt, subscriber) {
			continue
		}
		nodePaths[subscriber.Recipient.ID()] = strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	}
	if len(nodePaths) == 0 {
		return nil
	}
	out := make([]RoutePlanDeliveryIntent, 0, len(nodePaths))
	for _, recipient := range sortedStringKeys(nodePaths) {
		path := nodePaths[recipient]
		out = append(out, RoutePlanDeliveryIntent{
			Recipient: events.MustNodeDeliveryRecipient(recipient),
			Target:    events.RouteIdentity{FlowID: runtimeflowidentity.SemanticScopeFromFlowInstanceRef(path), FlowInstance: path},
			Producer:  routeIntentProducerRootInputFlowNode, Persist: true,
		})
	}
	return normalizeRoutePlanDeliveryIntents(out)
}

func routedRootInputFlowNodeMatchesNoTargetEvent(evt events.Event, subscriber Subscriber) bool {
	if !subscriber.Recipient.IsNode() {
		return false
	}
	if subscriber.routeSource != subscriberRouteSourceRootInputFlow {
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
		out[subscriber.Recipient.ID()] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func routedRootNodeMatchesNoTargetEvent(evt events.Event, subscriber Subscriber) bool {
	if !subscriber.Recipient.IsNode() {
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
		if localized := eventidentity.Normalize(subscriber.LocalizedEvent); localized != "" {
			out = append(out, localized)
			if path := eventidentity.Normalize(subscriber.Path); path != "" {
				out = append(out, path+"/"+localized)
			}
		}
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
	if !subscriber.Recipient.IsNode() {
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
	if !subscriber.Recipient.IsNode() {
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
	if !subscriber.Recipient.IsNode() {
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
			if !subscriber.Recipient.IsNode() {
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
	seen := map[events.RouteIdentity]struct{}{}
	for _, route := range in {
		route = route.Normalized()
		if route.Empty() {
			continue
		}
		if _, ok := seen[route]; ok {
			continue
		}
		seen[route] = struct{}{}
		out = append(out, route)
	}
	return out
}

func hasInternalRoutedSubscriberForTarget(evt events.Event, subscribers []Subscriber) bool {
	targets := eventDeliveryTargetRoutes(evt)
	for _, subscriber := range subscribers {
		if !subscriber.Recipient.IsNode() {
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
		return exactRootTarget(route) || entityOnlyRootTarget(route)
	}
	if route.FlowInstance == "" {
		return false
	}
	return route.FlowInstance == path || runtimeflowidentity.SemanticScopeFromInstancePath(route.FlowInstance) == path
}

func exactRootTarget(route events.RouteIdentity) bool {
	route = route.Normalized()
	if route.FlowID == "" || route.FlowInstance == "" || route.EntityID == "" {
		return false
	}
	_, err := uuid.Parse(route.FlowInstance)
	return err == nil
}

func entityOnlyRootTarget(route events.RouteIdentity) bool {
	route = route.Normalized()
	return route.FlowID == "" && route.FlowInstance == "" && route.EntityID != ""
}

func targetDeliveryFailure(evt events.Event, descriptors []ActiveTargetDescriptor) runtimepinrouting.TargetFailure {
	targets := eventDeliveryTargetRoutes(evt)
	if len(targets) == 0 {
		return 0
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
	descriptor = descriptor.Normalized()
	if descriptor.Identity.Route.Presence == agentidentity.RouteRoot {
		return (exactRootTarget(route) || entityOnlyRootTarget(route)) && descriptor.EntityID != "" && descriptor.EntityID == route.Normalized().EntityID
	}
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
