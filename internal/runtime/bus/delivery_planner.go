package bus

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
	AgentLifecycle    agentLifecycleAdmission
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
	resolveRoutedSubscribers            func(context.Context, events.Event, []string, ordinaryPublicationSource) ([]Subscriber, error)
	resolveSubscribedRecipients         func(string) []deliveryRecipientCandidate
	resolveRoutedNodeInternalRecipients func(events.Event, []Subscriber) []deliveryRecipientCandidate
	describeSubscribersForEvent         func(string, []Subscriber) []PublishDiagnosticRecipient
}

func (r deliveryRouteResolver) Resolve(ctx context.Context, evt events.Event) (deliveryRoutingResult, error) {
	return r.resolve(ctx, evt, ordinaryPublicationSource{}, nil)
}

func (r deliveryRouteResolver) ResolveIndependentPubsub(ctx context.Context, evt events.Event, source ordinaryPublicationSource) (deliveryRoutingResult, error) {
	return r.resolve(ctx, evt, source, independentPubsubSubscriber)
}

func (r deliveryRouteResolver) resolve(ctx context.Context, evt events.Event, source ordinaryPublicationSource, include func(Subscriber) bool) (deliveryRoutingResult, error) {
	eventKeys := pubsubEventKeys(evt, source)
	routedRecipients, err := r.resolveRoutedSubscribers(ctx, evt, eventKeys, source)
	if err != nil {
		return deliveryRoutingResult{}, err
	}
	if include != nil || !source.route.Empty() {
		filtered := make([]Subscriber, 0, len(routedRecipients))
		for _, subscriber := range routedRecipients {
			if source.includesSubscriber(subscriber) && (include == nil || include(subscriber)) {
				filtered = append(filtered, subscriber)
			}
		}
		routedRecipients = filtered
	}
	subscribedRecipients := make([]deliveryRecipientCandidate, 0, 8)
	for _, eventKey := range eventKeys {
		for _, candidate := range r.resolveSubscribedRecipients(eventKey) {
			if source.includesCandidate(candidate) {
				subscribedRecipients = append(subscribedRecipients, candidate)
			}
		}
	}
	subscribedRecipients = normalizeDeliveryRecipientCandidates(subscribedRecipients)
	routedCandidates := routedSubscriberCandidates(evt.RunID(), routedRecipients)
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
	return result, nil
}

type deliveryRecipientManifest struct {
	LiveRecipients      []deliveryRecipientCandidate
	Recipients          []string
	PersistedRecipients []string
	DeliveryRoutes      []events.DeliveryRoute
	TargetFailure       runtimepinrouting.TargetFailure
	AgentLifecycles     map[agentidentity.Identity]agentLifecycleAdmission
}

type deliveryRecipientPolicy struct {
	prospective                 runtimepipeline.PreparedWorkflowPublicationState
	loadActiveAgentDescriptors  func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error)
	loadActiveTargetDescriptors func(context.Context) ([]ActiveTargetDescriptor, bool, error)
	semanticSource              semanticview.Source
	requireTargetOwners         bool
	runProposal                 runtimepipeline.FlowInstanceRunProposal
}

func (p deliveryRecipientPolicy) Evaluate(ctx context.Context, evt events.Event, recipients []deliveryRecipientCandidate) (deliveryRecipientManifest, error) {
	return p.evaluate(ctx, evt, recipients, ordinaryPublicationSource{})
}

func (p deliveryRecipientPolicy) evaluate(ctx context.Context, evt events.Event, recipients []deliveryRecipientCandidate, localSource ordinaryPublicationSource) (deliveryRecipientManifest, error) {
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
		routes, err := agentDeliveryRoutesForCandidates(evt, persistedRecipients, projection)
		if err != nil {
			return deliveryRecipientManifest{}, err
		}
		manifest := deliveryRecipientManifest{
			LiveRecipients:      liveRecipients,
			Recipients:          deliveryRecipientIDs(recipients),
			PersistedRecipients: deliveryRecipientIDs(persistedRecipients),
			DeliveryRoutes:      routes,
			AgentLifecycles:     agentLifecycleAdmissionsForCandidates(persistedRecipients),
		}
		if targetDescriptorsOK && len(eventDeliveryTargetRoutes(evt)) > 0 && len(manifest.Recipients) == 0 {
			manifest.TargetFailure = targetDeliveryFailure(evt, targetDescriptors)
		}
		return manifest, nil
	}
	return filterDeliveryRecipientCandidates(p.semanticSource, evt, recipients, descriptors, targetDescriptors, projection, localSource)
}

func agentLifecycleAdmissionsForCandidates(recipients []deliveryRecipientCandidate) map[agentidentity.Identity]agentLifecycleAdmission {
	out := make(map[agentidentity.Identity]agentLifecycleAdmission)
	for _, candidate := range normalizeDeliveryRecipientCandidates(recipients) {
		if !candidate.AgentLifecycle.pending() || candidate.AgentIdentity.IsZero() {
			continue
		}
		out[candidate.AgentIdentity.Normalize()] = candidate.AgentLifecycle
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

type deliveryPlanner struct {
	routeResolver   deliveryRouteResolver
	recipientPolicy deliveryRecipientPolicy
	connectPlanner  connectRoutePlanResolver
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
	if err := validateEventRootTargetCoordinates(p.recipientPolicy.semanticSource, evt); err != nil {
		return RoutePlan{}, err
	}
	plan, err := p.planForRecipientMaterialization(ctx, evt)
	if err != nil {
		return RoutePlan{}, err
	}
	if err := validateRoutedNodeDeliveryAuthority(p.recipientPolicy.semanticSource, evt, plan.RoutedRecipients, plan); err != nil {
		return RoutePlan{}, err
	}
	return plan, nil
}

// planForRecipientMaterialization returns a provisional plan; publish callers
// must validate routed-node authority after adding materialized delivery intents.
func (p deliveryPlanner) planForRecipientMaterialization(ctx context.Context, evt events.Event) (RoutePlan, error) {
	routePlan := newRoutePlan(evt)
	if evt.Type() == events.EventType("platform.runtime_log") {
		return routePlan, nil
	}
	return p.planRecipients(ctx, evt)
}

func (p deliveryPlanner) planRecipients(ctx context.Context, evt events.Event) (RoutePlan, error) {
	routePlan := newRoutePlan(evt)
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	ctx = withConnectRoutePlanPreview(ctx)
	rootPlans, err := p.prepareRootConstruction(ctx, evt)
	if err != nil {
		return RoutePlan{}, err
	}
	ctx, err = p.previewPreparedConstructions(ctx, evt.RunID(), rootPlans)
	if err != nil {
		return RoutePlan{}, err
	}
	scope, bounded, err := p.connectPlanner.selectedTargetScope(ctx, evt)
	if err != nil {
		return RoutePlan{}, err
	}
	if bounded {
		ctx = withSelectedTargetOwnerLookupScope(ctx, scope)
	}
	projection, err := p.recipientPolicy.loadSelectedRunTargetOwnerProjection(ctx)
	if err != nil {
		return RoutePlan{}, err
	}
	ctx = withSelectedRunTargetOwnerProjection(ctx, projection)
	joinRecipient, joinTarget, joinHandler, joinOccurrence, err := runtimepipeline.ResolveWorkflowJoinOccurrenceDeliveryTarget(
		p.recipientPolicy.semanticSource, evt,
	)
	if err != nil {
		return RoutePlan{}, err
	}
	if joinOccurrence {
		routePlan.AddDeliveryIntents(RoutePlanDeliveryIntent{
			Recipient: joinRecipient, TargetBlueprint: joinTarget, Handler: joinHandler,
			Producer: routeIntentProducerInternalTargetCarrier, Persist: true,
		})
		return projection.resolveRoutePlan(routePlan)
	}
	projection, err = projection.withActivationPlans(preparedConnectPlans(ctx))
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
		ctx = withSelectedRunTargetOwnerProjection(ctx, projection)
		connectPlan.ActivationPlans = append(rootPlans, connectPlan.ActivationPlans...)
		routePlan = routePlanFromConnectRouteDispatch(evt, connectPlan)
		if routePlan.AuthorityState == RoutePlanAuthorityCanonicalFailedClosed {
			localPlan, localErr := p.planIndependentPubsubBranch(ctx, evt, true)
			if localErr != nil {
				return RoutePlan{}, localErr
			}
			if len(localPlan.LiveRecipients) > 0 || len(localPlan.DeliveryIntents) > 0 {
				return RoutePlan{}, mixedPubsubConnectCompositionFailure{failure: routePlan.TargetFailure}
			}
			return projection.resolveRoutePlan(routePlan)
		}
		if routePlan.ReplyContextConsumed {
			return projection.resolveRoutePlan(routePlan)
		}
		localPlan, err := p.planIndependentPubsubBranch(ctx, evt, true)
		if err != nil {
			return RoutePlan{}, err
		}
		routePlan = composeIndependentPubsubBranch(routePlan, localPlan)
		return projection.resolveRoutePlan(routePlan)
	}
	routePlan, err = p.planIndependentPubsubBranch(ctx, evt, false)
	if err != nil {
		return RoutePlan{}, err
	}
	routePlan.ConnectEvaluation = connectPlan.Evaluation
	routePlan.ActivationPlans = rootPlans
	return projection.resolveRoutePlan(routePlan)
}

func (p deliveryPlanner) previewPreparedConstructions(ctx context.Context, runID string, plans []runtimepipeline.FlowInstanceActivationPlan) (context.Context, error) {
	if len(plans) == 0 {
		return ctx, nil
	}
	ctx = withConnectRoutePlanPreview(ctx)
	for _, plan := range plans {
		if err := p.connectPlanner.installFlowConstructionPreview(ctx, runID, plan); err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

type mixedPubsubConnectCompositionFailure struct {
	failure runtimepinrouting.TargetFailure
}

func (e mixedPubsubConnectCompositionFailure) Error() string {
	return fmt.Sprintf("mixed pubsub/connect routing failed closed: %s", e.failure.Code())
}

func (p deliveryPlanner) planIndependentPubsubBranch(ctx context.Context, evt events.Event, clearConnectProjection bool) (RoutePlan, error) {
	localEvent, err := independentPubsubEvent(evt, clearConnectProjection)
	if err != nil {
		return RoutePlan{}, err
	}
	projection, ok := selectedRunTargetOwnerProjectionFromContext(ctx)
	if !ok {
		return RoutePlan{}, fmt.Errorf("ordinary publication requires selected-run ownership projection")
	}
	source, err := projection.ordinarySource(localEvent)
	if err != nil {
		return RoutePlan{}, err
	}
	include := independentPubsubSubscriber
	if input, ok := selectedInputValidationFromContext(ctx, evt); ok {
		include = func(subscriber Subscriber) bool {
			return independentPubsubSubscriber(subscriber) && input.AllowsSubscriber(subscriber)
		}
	}
	routing, err := p.routeResolver.resolve(ctx, localEvent, source, include)
	if err != nil {
		return RoutePlan{}, err
	}
	manifest, err := p.recipientPolicy.evaluate(ctx, localEvent, routing.Recipients, source)
	if err != nil {
		return RoutePlan{}, err
	}
	routePlan := routePlanFromManifest(localEvent, manifest, routeIntentProducerAgentPolicy)
	routePlan.ordinarySource = source
	routePlan.AddDeliveryIntents(source.localNodeIntents(p.recipientPolicy.semanticSource, localEvent, routing.RoutedRecipients)...)
	routePlan.AddDeliveryIntents(targetedRoutedNodeDeliveryIntents(p.recipientPolicy.semanticSource, evt, routing.RoutedRecipients)...)
	extraDetail := cloneAnyMap(routing.ExtraDetail)
	if !routePlan.TargetFailure.Empty() && hasInternalRoutedSubscriberForTarget(p.recipientPolicy.semanticSource, evt, routing.RoutedRecipients) {
		routePlan.TargetFailure = 0
	}
	routePlan.RoutedRecipients = routing.RoutedRecipients
	routePlan.SubscribedRecipients = routing.SubscribedRecipients
	routePlan.ExtraDetail = extraDetail
	return routePlan.Normalized(), nil
}

func independentPubsubEvent(evt events.Event, clearConnectProjection bool) (events.Event, error) {
	if !clearConnectProjection || len(eventDeliveryTargetRoutes(evt)) == 0 || !explicitRootPublicationTarget(evt).Empty() {
		return evt, nil
	}
	localEvent, err := events.ResolveEnvelope(evt, events.EnvelopeForBroadcast(evt.NormalizedEnvelope()))
	if err != nil {
		return evt, fmt.Errorf("resolve independent pubsub source projection: %w", err)
	}
	return localEvent, nil
}

func explicitRootPublicationTarget(evt events.Event) events.RouteIdentity {
	if evt.RoutingSource().Kind() != events.RoutingSourceRoot {
		return events.RouteIdentity{}
	}
	switch evt.AdmissionClass() {
	case events.EventAdmissionRootIngress, events.EventAdmissionOperatorInjected, events.EventAdmissionSelectedForkReplay:
		return evt.TargetRoute().Normalized()
	default:
		return events.RouteIdentity{}
	}
}

func composeIndependentPubsubBranch(connectPlan, localPlan RoutePlan) RoutePlan {
	connectPlan = connectPlan.Normalized()
	localPlan = localPlan.Normalized()
	if connectPlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || connectPlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		return connectPlan
	}
	connectPlan.ordinarySource = localPlan.ordinarySource
	connectPlan.AddLiveRecipients(localPlan.LiveRecipients...)
	connectPlan.AddDeliveryIntents(localPlan.DeliveryIntents...)
	connectPlan.RoutedRecipients = dedupeSubscribers(append(connectPlan.RoutedRecipients, localPlan.RoutedRecipients...))
	connectPlan.SubscribedRecipients = uniqueStrings(append(connectPlan.SubscribedRecipients, localPlan.SubscribedRecipients...))
	if len(localPlan.ExtraDetail) > 0 {
		if connectPlan.ExtraDetail == nil {
			connectPlan.ExtraDetail = map[string]any{}
		}
		connectPlan.ExtraDetail["independent_pubsub_branch"] = cloneAnyMap(localPlan.ExtraDetail)
	}
	return connectPlan.Normalized()
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
	manifest := exactDirectRecipientManifest(candidates, projection.agents, projection.agentsAvailable)
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
	routePlan.DeliveryIntents = routePlanDeliveryIntentsFromAdmittedRoutes(routes, routeIntentProducerDirectPolicy)
	routePlan.ExtraDetail = map[string]any{
		"direct":                true,
		"exact_routes":          true,
		"requested_route_count": len(routes),
	}
	return projection.resolveRoutePlan(routePlan)
}

// Exact direct delivery consumes caller-supplied routes as its sole route and
// target authority. Current policy may prove the exact agent identity exists,
// but must not reinterpret a committed target through current descriptors.
func exactDirectRecipientManifest(
	recipients []deliveryRecipientCandidate,
	descriptors map[agentidentity.Identity]ActiveAgentDescriptor,
	descriptorsAvailable bool,
) deliveryRecipientManifest {
	recipients = normalizeDeliveryRecipientCandidates(recipients)
	if descriptorsAvailable {
		available := make([]deliveryRecipientCandidate, 0, len(recipients))
		for _, recipient := range recipients {
			identity := recipient.AgentIdentity.Normalize()
			if descriptor, ok := descriptors[identity]; !ok || descriptor.Normalized().Identity != identity {
				continue
			}
			available = append(available, recipient)
		}
		recipients = normalizeDeliveryRecipientCandidates(available)
	}
	persisted := persistedDeliveryRecipientCandidates(recipients)
	return deliveryRecipientManifest{
		LiveRecipients:      recipients,
		Recipients:          deliveryRecipientIDs(recipients),
		PersistedRecipients: deliveryRecipientIDs(persisted),
	}
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
			resolveRoutedSubscribers:            eb.connectRoutePlanner.resolvePubsubSubscribers,
			resolveSubscribedRecipients:         eb.resolveSubscribedRecipientsForPlanning,
			resolveRoutedNodeInternalRecipients: eb.resolveInternalRecipientsForRoutedNodePlanning,
			describeSubscribersForEvent:         eb.describeSubscribersForEvent,
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors:  eb.activeAgentDescriptors,
			loadActiveTargetDescriptors: eb.activeTargetDescriptors,
			semanticSource:              eb.semanticSource,
			requireTargetOwners:         !eb.ephemeral,
		},
		eb.connectRoutePlanner,
	)
	return planner
}

func normalizeDeliveryRecipientCandidates(in []deliveryRecipientCandidate) []deliveryRecipientCandidate {
	if len(in) == 0 {
		return nil
	}
	out := make([]deliveryRecipientCandidate, 0, len(in))
	type candidateKey struct {
		identity  agentidentity.Identity
		id        string
		lifecycle agentLifecycleAdmission
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
		key := candidateKey{id: candidate.ID, lifecycle: candidate.AgentLifecycle}
		if !candidate.AgentIdentity.IsZero() {
			key = candidateKey{identity: candidate.AgentIdentity, lifecycle: candidate.AgentLifecycle}
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

func routedSubscriberCandidates(runID string, in []Subscriber) []deliveryRecipientCandidate {
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
		identity, err := subscriber.AgentPlan.Live(runID)
		if err != nil {
			continue
		}
		out = append(out, deliveryRecipientCandidate{
			ID:                subscriber.Recipient.ID(),
			AgentIdentity:     identity,
			PersistAsDelivery: true,
			AgentLifecycle:    subscriber.agentLifecycle,
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
	out := SourceEventRouteKeys(evt.Type(), evt.RoutingSource())
	out = append(out, targetedConcreteEventKeysForPlan(evt)...)
	return uniqueStrings(out)
}

// SourceEventRouteKeys projects the admitted source occurrence, never a receiver
// address. Live publication and selected history use the same lookup identity.
func SourceEventRouteKeys(name events.EventType, source events.RoutingSource) []string {
	eventType := strings.Trim(strings.TrimSpace(string(name)), "/")
	if eventType == "" {
		return nil
	}
	out := []string{eventType}
	if source.Kind() == events.RoutingSourceRoot {
		out = append(out, "./"+eventType)
	}
	if concrete := sourceFlowInstanceEventKey(name, source); concrete != "" {
		out = append(out, concrete)
	}
	return uniqueStrings(out)
}

func sourceFlowInstanceEventKey(name events.EventType, source events.RoutingSource) string {
	eventType := strings.Trim(strings.TrimSpace(string(name)), "/")
	flowInstance := exactSourceFlowInstance(source)
	if eventType == "" || flowInstance == "" {
		return ""
	}
	staticScope := source.Route().FlowID
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
		staticScope := target.FlowID
		if staticScope == "" {
			continue
		}
		localEvent := eventidentity.LocalizeForFlow(staticScope, nil, eventType)
		if localEvent == "" || strings.Contains(localEvent, "/") {
			continue
		}
		out = append(out, flowInstance+"/"+localEvent)
		out = append(out, localEvent)
	}
	return uniqueStrings(out)
}

func exactEventFlowInstance(evt events.Event) string {
	return exactSourceFlowInstance(evt.RoutingSource())
}

func exactSourceFlowInstance(source events.RoutingSource) string {
	switch source.Kind() {
	case events.RoutingSourceStaticFlow, events.RoutingSourceConcreteTemplateInstance, events.RoutingSourceFlowOwnedControl:
		return strings.Trim(strings.TrimSpace(source.Route().FlowInstance), "/")
	default:
		return ""
	}
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
	source semanticview.Source,
	evt events.Event,
	recipients []deliveryRecipientCandidate,
	descriptors map[agentidentity.Identity]ActiveAgentDescriptor,
	targetDescriptors []ActiveTargetDescriptor,
	projection selectedRunTargetOwnerProjection,
	localSource ordinaryPublicationSource,
) (deliveryRecipientManifest, error) {
	recipients = normalizeDeliveryRecipientCandidates(recipients)
	targetFailureDescriptors := append([]ActiveTargetDescriptor(nil), targetDescriptors...)
	targetFailureDescriptors = append(targetFailureDescriptors, activeTargetDescriptorsFromAgents(descriptors)...)
	eventEntityID := strings.TrimSpace(evt.EntityID())
	targets := eventDeliveryTargetRoutes(evt)
	if len(recipients) == 0 {
		return deliveryRecipientManifest{
			TargetFailure: targetDeliveryFailure(evt, targetFailureDescriptors),
		}, nil
	}
	singularTarget := evt.TargetRoute()
	implicitLocal := len(targets) == 0 && !localSource.route.Empty()
	if implicitLocal {
		eventEntityID = localSource.route.EntityID
	}
	allowed := make([]string, 0, len(recipients))
	allowedCandidates := make([]deliveryRecipientCandidate, 0, len(recipients))
	persisted := make([]string, 0, len(recipients))
	deliveryRoutes := make([]events.DeliveryRoute, 0, len(recipients))
	root := rootExecutionCoordinate(source, evt.RunID())
	for _, recipient := range recipients {
		if !recipient.PersistAsDelivery {
			allowed = append(allowed, recipient.ID)
			allowedCandidates = append(allowedCandidates, recipient)
			continue
		}
		for _, descriptor := range matchingAgentDescriptors(recipient, descriptors) {
			descriptor = descriptor.Normalized()
			if implicitLocal && (descriptor.Identity.RunID != evt.RunID() || !localSource.ownsAgent(descriptor.Identity.FlowInstance())) {
				continue
			}
			if descriptor.EntityID != "" {
				if eventEntityID == "" || descriptor.EntityID != eventEntityID {
					if len(targets) == 0 {
						continue
					}
				}
			}
			target, targetOK := deliveryTargetForDescriptor(descriptor, singularTarget, targets, root)
			if len(targets) > 0 && !targetOK {
				continue
			}
			if implicitLocal {
				// Source-node ownership scopes the publication, not the agent's entity.
				target = localSource.route
				target.EntityID = descriptor.EntityID
			}
			scoped := recipient
			scoped.AgentIdentity = descriptor.Identity
			if scoped.AgentLifecycle == agentLifecycleAdmissionStaticDeclaration {
				scoped.AgentLifecycle = agentLifecycleAdmissionNone
			}
			if _, constructing := projection.constructingAgents[descriptor.Identity]; constructing {
				scoped.AgentLifecycle = agentLifecycleAdmissionMaterializingFlow
			}
			allowed = append(allowed, scoped.ID)
			allowedCandidates = append(allowedCandidates, scoped)
			persisted = append(persisted, scoped.ID)
			owner, err := projection.resolveActiveAgentTarget(descriptor, target)
			if err != nil {
				return deliveryRecipientManifest{}, err
			}
			deliveryRoutes = append(deliveryRoutes, events.DeliveryRoute{
				Recipient:     events.MustAgentDeliveryRecipient(scoped.ID),
				AgentIdentity: scoped.AgentIdentity,
				Target:        owner,
			})
		}
	}
	persisted = uniqueStrings(persisted)
	manifest := deliveryRecipientManifest{
		LiveRecipients:      normalizeDeliveryRecipientCandidates(allowedCandidates),
		Recipients:          uniqueStrings(allowed),
		PersistedRecipients: persisted,
		DeliveryRoutes:      events.NormalizeDeliveryRoutes(deliveryRoutes),
		AgentLifecycles:     agentLifecycleAdmissionsForCandidates(allowedCandidates),
	}
	if len(targets) > 0 && len(manifest.LiveRecipients) == 0 {
		manifest.TargetFailure = targetDeliveryFailure(evt, targetFailureDescriptors)
	}
	return manifest, nil
}

func matchingAgentDescriptors(
	recipient deliveryRecipientCandidate,
	descriptors map[agentidentity.Identity]ActiveAgentDescriptor,
) []ActiveAgentDescriptor {
	if !recipient.AgentIdentity.IsZero() && recipient.AgentLifecycle != agentLifecycleAdmissionStaticDeclaration {
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
		if recipient.AgentLifecycle == agentLifecycleAdmissionStaticDeclaration {
			declared := recipient.AgentIdentity.Normalize()
			if identity.RunID != declared.RunID || identity.Name != declared.Name || identity.Route.Presence != declared.Route.Presence || identity.Route.ScopeKey != declared.Route.ScopeKey {
				continue
			}
		}
		out = append(out, descriptor)
	}
	sort.Slice(out, func(left, right int) bool {
		return agentidentity.Less(out[left].Identity, out[right].Identity)
	})
	return out
}

func agentDeliveryRoutesForCandidates(
	evt events.Event,
	recipients []deliveryRecipientCandidate,
	projection selectedRunTargetOwnerProjection,
) ([]events.DeliveryRoute, error) {
	recipients = persistedDeliveryRecipientCandidates(recipients)
	if len(recipients) == 0 {
		return nil, nil
	}
	out := make([]events.DeliveryRoute, 0, len(recipients))
	target := evt.TargetRoute().Normalized()
	for _, recipient := range recipients {
		if err := recipient.AgentIdentity.Validate(); err != nil {
			continue
		}
		owner := events.DeliveryTargetOwnership{}
		if !target.Empty() {
			var err error
			owner, err = projection.resolveSelectedRoute(target)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, events.DeliveryRoute{
			Recipient:     events.MustAgentDeliveryRecipient(recipient.ID),
			AgentIdentity: recipient.AgentIdentity,
			Target:        owner,
		})
	}
	return events.NormalizeDeliveryRoutes(out), nil
}

func targetedRoutedNodeDeliveryIntents(source semanticview.Source, evt events.Event, routed []Subscriber) []RoutePlanDeliveryIntent {
	routes := targetedRoutedNodeDeliveryRoutes(source, evt, routed)
	if len(routes) == 0 {
		return nil
	}
	return routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerInternalTargetRoute)
}

func targetedRoutedNodeDeliveryRoutes(source semanticview.Source, evt events.Event, routed []Subscriber) []plannedDeliveryRoute {
	targets := eventDeliveryTargetRoutes(evt)
	if len(targets) == 0 || len(routed) == 0 {
		return nil
	}
	out := make([]plannedDeliveryRoute, 0, len(targets)*len(routed))
	for _, target := range targets {
		target = target.Normalized()
		if target.Empty() {
			continue
		}
		for _, subscriber := range routed {
			if !subscriber.Recipient.IsNode() {
				continue
			}
			if !routeMatchesInternalSubscriber(target, subscriber, rootExecutionCoordinate(source, evt.RunID())) {
				continue
			}
			out = append(out, plannedDeliveryRoute{
				Recipient: subscriber.Recipient,
				Target:    target,
				Handler:   routedSubscriberTargetHandler(subscriber, evt.Type()),
			})
		}
	}
	return normalizePlannedDeliveryRoutes(out)
}

func routedExactSameInstanceNoTargetNodeDeliveryIntents(source semanticview.Source, evt events.Event, routed []Subscriber) []RoutePlanDeliveryIntent {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	flowInstance := exactEventFlowInstance(evt)
	if flowInstance == "" {
		return nil
	}
	out := make([]plannedDeliveryRoute, 0, len(routed))
	for _, subscriber := range routed {
		if !subscriber.Recipient.IsNode() || !routedNodeMatchesConcreteFlowInstanceEvent(evt, subscriber) {
			continue
		}
		targetFlowInstance := flowInstance
		if node, ok := subscriber.Recipient.Node(); ok && source != nil {
			if scope, found := source.FlowScopeByID(node.FlowPath()); found {
				switch strings.ToLower(strings.TrimSpace(scope.Mode)) {
				case runtimecontracts.FlowModeStatic:
					targetFlowInstance = strings.Trim(strings.TrimSpace(subscriber.Path), "/")
				}
			}
		}
		out = append(out, plannedDeliveryRoute{
			Recipient: subscriber.Recipient,
			Target:    routedNodeTargetRoute(targetFlowInstance),
			Handler:   routedSubscriberTargetHandler(subscriber, evt.Type()),
		})
	}
	return routePlanDeliveryIntentsFromRoutes(out, routeIntentProducerConcreteNodeRoute)
}

func validateRoutedNodeDeliveryAuthority(source semanticview.Source, evt events.Event, routed []Subscriber, plan RoutePlan) error {
	// Ingress authenticates the declaring scope, not an arbitrary delivery address.
	if evt.AdmissionClass() == events.EventAdmissionRootIngress || evt.AdmissionClass() == events.EventAdmissionOperatorInjected {
		sourceFlow := evt.RoutingSource().Route().FlowID
		if evt.RoutingSource().Kind() == events.RoutingSourceRoot {
			sourceFlow = semanticview.RootExecutionFlowID(source)
		}
		for _, intent := range plan.DeliveryIntents {
			if intent.Producer == routeIntentProducerConnectRoutePlan && !intent.ConnectClaim.Empty() {
				continue
			}
			target := intent.TargetOwnership.Route().Normalized()
			if target.Empty() {
				target = intent.TargetBlueprint.Normalized()
			}
			if target.FlowID != "" && target.FlowID != sourceFlow {
				return fmt.Errorf("ingress source %q cannot authorize private delivery to %q without a compiled connection", sourceFlow, target.FlowID)
			}
		}
	}
	if len(routed) == 0 {
		return nil
	}
	hasExplicitTargets := len(eventDeliveryTargetRoutes(evt)) > 0
	authorized := make(map[routedSubscriberAuthorityKey]struct{}, len(plan.DeliveryIntents))
	for _, subscriber := range routed {
		if !subscriber.Recipient.IsNode() {
			continue
		}
		key := newRoutedSubscriberAuthorityKey(source, evt, subscriber)
		for _, intent := range plan.DeliveryIntents {
			if !routePlanIntentAuthorizesRoutedSubscriber(intent, key, subscriber) {
				continue
			}
			authorized[key] = struct{}{}
		}
	}
	for _, subscriber := range routed {
		if !subscriber.Recipient.IsNode() {
			continue
		}
		key := newRoutedSubscriberAuthorityKey(source, evt, subscriber)
		if hasExplicitTargets {
			continue
		}
		if subscriber.routeSource != subscriberRouteSourceSubscription {
			continue
		}
		if _, ok := authorized[key]; ok {
			continue
		}
		return fmt.Errorf(
			"routed node %q at %q matched target-free event %q without exact same-instance, explicit-target, or compiled-connect authority",
			subscriber.Recipient.ID(), strings.TrimSpace(subscriber.Path), evt.Type(),
		)
	}
	return nil
}

type routedSubscriberAuthorityKey struct {
	recipient   events.DeliveryRecipient
	path        string
	routeSource subscriberRouteSource
	handler     runtimepipeline.DeliveryTargetHandler
	root        semanticview.RootExecutionCoordinate
}

func newRoutedSubscriberAuthorityKey(source semanticview.Source, evt events.Event, subscriber Subscriber) routedSubscriberAuthorityKey {
	return routedSubscriberAuthorityKey{
		recipient:   subscriber.Recipient,
		path:        strings.Trim(strings.TrimSpace(subscriber.Path), "/"),
		routeSource: subscriber.routeSource,
		handler:     routedSubscriberTargetHandler(subscriber, evt.Type()),
		root:        rootExecutionCoordinate(source, evt.RunID()),
	}
}

func routePlanIntentAuthorizesRoutedSubscriber(
	intent RoutePlanDeliveryIntent,
	key routedSubscriberAuthorityKey,
	subscriber Subscriber,
) bool {
	if !intent.Recipient.IsNode() || intent.Recipient != key.recipient || !intent.Handler.Equal(key.handler) {
		return false
	}
	if key.path == "" {
		return true
	}
	target := intent.TargetBlueprint.Normalized()
	if target.Empty() {
		target = intent.TargetOwnership.Route().Normalized()
	}
	if routeMatchesInternalSubscriber(target, subscriber, key.root) {
		return true
	}
	return resolvedSelectedRunRootIntentMatchesSubscriber(intent, key, subscriber, target)
}

func resolvedSelectedRunRootIntentMatchesSubscriber(
	intent RoutePlanDeliveryIntent,
	key routedSubscriberAuthorityKey,
	subscriber Subscriber,
	target events.RouteIdentity,
) bool {
	// Root static routing uses its semantic path while ownership resolves to the selected run ID.
	if intent.Producer != routeIntentProducerConcreteNodeRoute {
		return false
	}
	handlerFlowID := subscriber.handlerNode.FlowPath()
	if handlerFlowID == "" {
		handlerFlowID = target.FlowID
	}
	return handlerFlowID != "" && key.path == handlerFlowID &&
		key.handler.Node().Equal(subscriber.handlerNode) && target.FlowID == handlerFlowID
}

func routedNodeTargetRoute(targetFlowInstance string) events.RouteIdentity {
	return events.RouteIdentity{
		FlowInstance: targetFlowInstance,
	}.Normalized()
}

func eventTargetsRoutedSubscriber(source semanticview.Source, evt events.Event, subscriber Subscriber) bool {
	root := rootExecutionCoordinate(source, evt.RunID())
	for _, target := range eventDeliveryTargetRoutes(evt) {
		if routeMatchesInternalSubscriber(target, subscriber, root) {
			return true
		}
	}
	return false
}

func routedRootNodeDeliveryIntentsForNoTargetEvent(source semanticview.Source, evt events.Event, routed []Subscriber) []RoutePlanDeliveryIntent {
	if len(routed) == 0 || len(eventDeliveryTargetRoutes(evt)) > 0 {
		return nil
	}
	root, err := semanticview.AdmitRootExecutionCoordinate(source, evt.RunID())
	if err != nil {
		return nil
	}
	out := make([]plannedDeliveryRoute, 0, len(routed))
	for _, subscriber := range routed {
		if !routedRootNodeMatchesNoTargetEvent(evt, subscriber, root) {
			continue
		}
		out = append(out, plannedDeliveryRoute{
			Recipient: subscriber.Recipient,
			Target: events.RouteIdentity{
				FlowID: root.FlowID(), FlowInstance: root.RunID(),
			},
			Handler: routedSubscriberTargetHandler(subscriber, evt.Type()),
		})
	}
	return routePlanDeliveryIntentsFromRoutes(out, routeIntentProducerRootNodeRoute)
}

func routedRootNodeMatchesNoTargetEvent(evt events.Event, subscriber Subscriber, root semanticview.RootExecutionCoordinate) bool {
	if !subscriber.Recipient.IsNode() {
		return false
	}
	path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	if !root.Valid() || (path != "." && path != root.RunID()) {
		return false
	}
	handlerFlowID := subscriber.handlerNode.FlowPath()
	if handlerFlowID == "" {
		handlerFlowID = root.FlowID()
	}
	if handlerFlowID != root.FlowID() {
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

func routedSubscriberTargetHandler(subscriber Subscriber, eventType events.EventType) runtimepipeline.DeliveryTargetHandler {
	if subscriber.targetHandler.Empty() {
		return runtimepipeline.DeliveryTargetHandler{}
	}
	if localized := strings.TrimSpace(subscriber.LocalizedEvent); localized != "" {
		return subscriber.targetHandler.ForEvent(events.EventType(localized))
	}
	return subscriber.targetHandler.ForEvent(eventType)
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
		localEvent := eventidentity.Normalize(subscriber.LocalizedEvent)
		if localEvent == "" {
			// A wildcard match has no single declaration-local event. Localize
			// only the exact, already routed execution prefix for its carrier.
			localEvent, _ = strings.CutPrefix(eventType, subscriber.Path+"/")
			if localEvent == eventType {
				continue
			}
		}
		node, _ := subscriber.Recipient.Node()
		staticScope := node.FlowPath()
		if localEvent == "" || staticScope == "" {
			continue
		}
		out = append(out, staticScope+"/"+localEvent)
	}
	return uniqueStrings(out)
}

func routedNodeMatchesConcreteFlowInstanceEvent(evt events.Event, subscriber Subscriber) bool {
	node, ok := subscriber.Recipient.Node()
	return ok && exactEventFlowInstance(evt) != "" &&
		subscriber.Path == exactEventFlowInstance(evt) &&
		node.FlowPath() == evt.RoutingSource().Route().FlowID
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

func hasInternalRoutedSubscriberForTarget(source semanticview.Source, evt events.Event, subscribers []Subscriber) bool {
	targets := eventDeliveryTargetRoutes(evt)
	root := rootExecutionCoordinate(source, evt.RunID())
	for _, subscriber := range subscribers {
		if !subscriber.Recipient.IsNode() {
			continue
		}
		if len(targets) == 0 {
			return true
		}
		for _, target := range targets {
			if routeMatchesInternalSubscriber(target, subscriber, root) {
				return true
			}
		}
	}
	return false
}

func rootExecutionCoordinate(source semanticview.Source, runID string) semanticview.RootExecutionCoordinate {
	coordinate, _ := semanticview.AdmitRootExecutionCoordinate(source, runID)
	return coordinate
}

func validateEventRootTargetCoordinates(source semanticview.Source, evt events.Event) error {
	rootFlowID := strings.TrimSpace(semanticview.RootExecutionFlowID(source))
	for _, route := range eventDeliveryTargetRoutes(evt) {
		route = route.Normalized()
		if rootFlowID == "" || route.FlowID != rootFlowID {
			continue
		}
		coordinate, err := semanticview.AdmitRootExecutionCoordinate(source, evt.RunID())
		if err != nil {
			return err
		}
		if !coordinate.Matches(route.FlowID, route.FlowInstance) {
			return fmt.Errorf("event root target (%q, %q) disagrees with current root coordinate (%q, %q)", route.FlowID, route.FlowInstance, coordinate.FlowID(), coordinate.RunID())
		}
	}
	return nil
}

func routeMatchesInternalSubscriber(route events.RouteIdentity, subscriber Subscriber, root semanticview.RootExecutionCoordinate) bool {
	route = route.Normalized()
	if route.Empty() {
		return false
	}
	path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	if path == "." {
		return exactRootTarget(route, root)
	}
	if path == "" {
		return false
	}
	if route.FlowInstance == "" {
		return false
	}
	return route.FlowInstance == path || runtimeflowidentity.SemanticScopeFromInstancePath(route.FlowInstance) == path
}

func exactRootTarget(route events.RouteIdentity, root semanticview.RootExecutionCoordinate) bool {
	route = route.Normalized()
	return root.Matches(route.FlowID, route.FlowInstance)
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

func deliveryTargetForDescriptor(descriptor ActiveAgentDescriptor, singular events.RouteIdentity, targets []events.RouteIdentity, root semanticview.RootExecutionCoordinate) (events.RouteIdentity, bool) {
	descriptor = descriptor.Normalized()
	if !singular.Empty() {
		return singular, routeMatchesAgentDescriptor(singular, descriptor, root)
	}
	if len(targets) == 0 {
		return events.RouteIdentity{}, true
	}
	for _, target := range targets {
		if routeMatchesAgentDescriptor(target, descriptor, root) {
			return target.Normalized(), true
		}
	}
	return events.RouteIdentity{}, false
}

func routeMatchesAgentDescriptor(route events.RouteIdentity, descriptor ActiveAgentDescriptor, root semanticview.RootExecutionCoordinate) bool {
	descriptor = descriptor.Normalized()
	if descriptor.Identity.Route.Presence == agentidentity.RouteRoot {
		return exactRootTarget(route, root) && descriptor.EntityID == route.Normalized().EntityID
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
