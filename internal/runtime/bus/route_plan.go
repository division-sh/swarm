package bus

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
)

type routePlanSource uint8
type routePlanReason uint8

const (
	routePlanSourceAgentPolicy routePlanSource = iota + 1
	routePlanSourceDirectPolicy
	routePlanSourceInternalTarget
	routePlanSourceConcreteNodeRoute
	routePlanSourceScopedNodeRoute
	routePlanSourceRootNodeRoute
	routePlanSourceRootInputFlowNode
	routePlanSourceAPIEventPublication
	routePlanSourceRecipientMaterializer
	routePlanSourceConnectRoutePlan
)

const (
	routePlanReasonMatchedAgentSubscription routePlanReason = iota + 1
	routePlanReasonDirectPublish
	routePlanReasonInternalCarrier
	routePlanReasonRouteTableNode
	routePlanReasonMaterializedRoute
	routePlanReasonLoweredConnectRoutePlan
)

func (s routePlanSource) code() string {
	switch s {
	case routePlanSourceAgentPolicy:
		return "agent_policy"
	case routePlanSourceDirectPolicy:
		return "direct_policy"
	case routePlanSourceInternalTarget:
		return "internal_target_route"
	case routePlanSourceConcreteNodeRoute:
		return "concrete_node_route"
	case routePlanSourceScopedNodeRoute:
		return "scoped_node_route"
	case routePlanSourceRootNodeRoute:
		return "root_node_route"
	case routePlanSourceRootInputFlowNode:
		return "root_input_flow_node_route"
	case routePlanSourceAPIEventPublication:
		return "api_event_publication_route"
	case routePlanSourceRecipientMaterializer:
		return "recipient_plan_materializer"
	case routePlanSourceConnectRoutePlan:
		return "connect_route_plan"
	default:
		return ""
	}
}

func (r routePlanReason) code() string {
	switch r {
	case routePlanReasonMatchedAgentSubscription:
		return "matched_agent_subscription"
	case routePlanReasonDirectPublish:
		return "direct_publish"
	case routePlanReasonInternalCarrier:
		return "internal_carrier"
	case routePlanReasonRouteTableNode:
		return "route_table_node"
	case routePlanReasonMaterializedRoute:
		return "materialized_route"
	case routePlanReasonLoweredConnectRoutePlan:
		return "lowered_connect_route_plan"
	default:
		return ""
	}
}

type routeIntentProducer uint8

const (
	routeIntentProducerUnknown routeIntentProducer = iota
	routeIntentProducerAgentPolicy
	routeIntentProducerDirectPolicy
	routeIntentProducerInternalTargetCarrier
	routeIntentProducerInternalTargetRoute
	routeIntentProducerConcreteNodeRoute
	routeIntentProducerScopedNodeRoute
	routeIntentProducerRootNodeRoute
	routeIntentProducerRootInputFlowNode
	routeIntentProducerAPIEventPublication
	routeIntentProducerRecipientMaterializer
	routeIntentProducerConnectRoutePlan
)

func (p routeIntentProducer) Normalized() routeIntentProducer {
	switch p {
	case routeIntentProducerAgentPolicy,
		routeIntentProducerDirectPolicy,
		routeIntentProducerInternalTargetCarrier,
		routeIntentProducerInternalTargetRoute,
		routeIntentProducerConcreteNodeRoute,
		routeIntentProducerScopedNodeRoute,
		routeIntentProducerRootNodeRoute,
		routeIntentProducerRootInputFlowNode,
		routeIntentProducerAPIEventPublication,
		routeIntentProducerRecipientMaterializer,
		routeIntentProducerConnectRoutePlan:
		return p
	default:
		return routeIntentProducerUnknown
	}
}

func (p routeIntentProducer) Source() routePlanSource {
	switch p.Normalized() {
	case routeIntentProducerAgentPolicy:
		return routePlanSourceAgentPolicy
	case routeIntentProducerDirectPolicy:
		return routePlanSourceDirectPolicy
	case routeIntentProducerInternalTargetCarrier, routeIntentProducerInternalTargetRoute:
		return routePlanSourceInternalTarget
	case routeIntentProducerConcreteNodeRoute:
		return routePlanSourceConcreteNodeRoute
	case routeIntentProducerScopedNodeRoute:
		return routePlanSourceScopedNodeRoute
	case routeIntentProducerRootNodeRoute:
		return routePlanSourceRootNodeRoute
	case routeIntentProducerRootInputFlowNode:
		return routePlanSourceRootInputFlowNode
	case routeIntentProducerAPIEventPublication:
		return routePlanSourceAPIEventPublication
	case routeIntentProducerRecipientMaterializer:
		return routePlanSourceRecipientMaterializer
	case routeIntentProducerConnectRoutePlan:
		return routePlanSourceConnectRoutePlan
	default:
		return 0
	}
}

func (p routeIntentProducer) Reason() routePlanReason {
	switch p.Normalized() {
	case routeIntentProducerAgentPolicy:
		return routePlanReasonMatchedAgentSubscription
	case routeIntentProducerDirectPolicy:
		return routePlanReasonDirectPublish
	case routeIntentProducerInternalTargetCarrier:
		return routePlanReasonInternalCarrier
	case routeIntentProducerInternalTargetRoute,
		routeIntentProducerConcreteNodeRoute,
		routeIntentProducerScopedNodeRoute,
		routeIntentProducerRootNodeRoute,
		routeIntentProducerRootInputFlowNode,
		routeIntentProducerAPIEventPublication:
		return routePlanReasonRouteTableNode
	case routeIntentProducerRecipientMaterializer:
		return routePlanReasonMaterializedRoute
	case routeIntentProducerConnectRoutePlan:
		return routePlanReasonLoweredConnectRoutePlan
	default:
		return 0
	}
}

func (p routeIntentProducer) Empty() bool {
	return p.Normalized() == routeIntentProducerUnknown
}

func routeIntentProducerCode(p routeIntentProducer) string {
	p = p.Normalized()
	source := p.Source()
	reason := p.Reason()
	if source == 0 {
		return reason.code()
	}
	if reason == 0 {
		return source.code()
	}
	return source.code() + "/" + reason.code()
}

type RoutePlanAuthorityState uint8

const (
	RoutePlanAuthorityNoCanonicalMatch RoutePlanAuthorityState = iota
	RoutePlanAuthorityCanonicalMatched
	RoutePlanAuthorityCanonicalFailedClosed
	RoutePlanAuthorityLowerPrecedence
)

// RoutePlan is the EventBus-owned publish-time route authority. It records the
// typed delivery intents that should be persisted and the live dispatch
// recipients that remain only projections/consumers of that authority.
type RoutePlan struct {
	Event                events.Event
	ConnectEvaluation    events.ConnectEvaluationLedger
	AuthorityState       RoutePlanAuthorityState
	AuthorityOwner       routePlanSource
	LiveRecipients       []RoutePlanLiveRecipient
	DeliveryIntents      []RoutePlanDeliveryIntent
	RoutedRecipients     []Subscriber
	SubscribedRecipients []string
	ExtraDetail          map[string]any
	TargetFailure        runtimepinrouting.TargetFailure
	ContradictionReason  string
	BlockedByCycle       bool
	CycleEscalation      *events.Event
	ReplyContextConsumed bool
	ActivationPlans      []runtimepipeline.FlowInstanceActivationPlan
	ReplyCreations       []runtimereplycontext.Record
	ReplyClaims          []runtimereplycontext.ClaimCommand
}

type RoutePlanLiveRecipient struct {
	Recipient         events.DeliveryRecipient
	AgentIdentity     agentidentity.Identity
	PersistAsDelivery bool
	Producer          routeIntentProducer
	liveAuthority     liveRecipientAuthority
	agentRoute        *agentRouteHandle
}

type liveRecipientAuthority uint8

const (
	liveRecipientAuthorityIdentity liveRecipientAuthority = iota
	liveRecipientAuthorityAgentRoute
)

func (a liveRecipientAuthority) Normalized() liveRecipientAuthority {
	if a == liveRecipientAuthorityAgentRoute {
		return a
	}
	return liveRecipientAuthorityIdentity
}

type RoutePlanDeliveryIntent struct {
	Recipient             events.DeliveryRecipient
	AgentIdentity         agentidentity.Identity
	TargetBlueprint       events.RouteIdentity
	TargetOwnership       events.DeliveryTargetOwnership
	Handler               runtimepipeline.DeliveryTargetHandler
	Context               events.DeliveryContext
	PayloadProjection     events.DeliveryPayloadProjection
	ConnectClaim          events.ConnectExecutionClaim
	Producer              routeIntentProducer
	Persist               bool
	PendingAgentLifecycle bool
	StructuralOwnerProof  runtimepinrouting.StructuralTargetOwnerProof
}

type DeliveryRouteBlueprint struct {
	Recipient         events.DeliveryRecipient
	AgentIdentity     agentidentity.Identity
	Target            events.RouteIdentity
	Handler           runtimepipeline.DeliveryTargetHandler
	Context           events.DeliveryContext
	PayloadProjection events.DeliveryPayloadProjection
	ConnectClaim      events.ConnectExecutionClaim
}

func (r DeliveryRouteBlueprint) normalized() DeliveryRouteBlueprint {
	return DeliveryRouteBlueprint{
		Recipient: r.Recipient, AgentIdentity: r.AgentIdentity.Normalize(), Target: r.Target.Normalized(),
		Handler: r.Handler, Context: r.Context.Normalized(), PayloadProjection: r.PayloadProjection.Normalized(), ConnectClaim: r.ConnectClaim,
	}
}

type plannedDeliveryRoute = DeliveryRouteBlueprint

func normalizePlannedDeliveryRoutes(in []plannedDeliveryRoute) []plannedDeliveryRoute {
	if len(in) == 0 {
		return nil
	}
	out := make([]plannedDeliveryRoute, 0, len(in))
	seen := make(map[deliveryIntentKey]struct{}, len(in))
	for _, route := range in {
		route = route.normalized()
		if route.Recipient.Empty() {
			continue
		}
		key := deliveryIntentKey{
			recipient: route.Recipient, agentIdentity: route.AgentIdentity, target: route.Target,
			handler: route.Handler, replyContextID: route.Context.ReplyContextID(),
			projection: route.PayloadProjection.Fingerprint(), connectClaim: route.ConnectClaim,
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, route)
	}
	return out
}

func newRoutePlan(evt events.Event) RoutePlan {
	ledger, _ := events.NewConnectEvaluationLedger(nil)
	return RoutePlan{Event: evt, AuthorityState: RoutePlanAuthorityNoCanonicalMatch, ConnectEvaluation: ledger}
}

func (p RoutePlan) Normalized() RoutePlan {
	p.AuthorityState = normalizeRoutePlanAuthorityState(p.AuthorityState)
	p.AuthorityOwner = normalizeRoutePlanSource(p.AuthorityOwner)
	p.LiveRecipients = normalizeRoutePlanLiveRecipients(p.LiveRecipients)
	p.DeliveryIntents = normalizeRoutePlanDeliveryIntents(p.DeliveryIntents)
	p.RoutedRecipients = append([]Subscriber(nil), p.RoutedRecipients...)
	p.SubscribedRecipients = uniqueStrings(p.SubscribedRecipients)
	p.ExtraDetail = cloneStringAnyMap(p.ExtraDetail)
	p.ActivationPlans = append([]runtimepipeline.FlowInstanceActivationPlan(nil), p.ActivationPlans...)
	p.ReplyCreations = append([]runtimereplycontext.Record(nil), p.ReplyCreations...)
	p.ReplyClaims = append([]runtimereplycontext.ClaimCommand(nil), p.ReplyClaims...)
	p.ContradictionReason = strings.TrimSpace(p.ContradictionReason)
	if p.CycleEscalation != nil {
		evt := *p.CycleEscalation
		p.CycleEscalation = &evt
	}
	if p.AuthorityState == RoutePlanAuthorityCanonicalMatched && !p.TargetFailure.Empty() {
		p.AuthorityState = RoutePlanAuthorityCanonicalFailedClosed
	}
	if p.AuthorityState == RoutePlanAuthorityCanonicalFailedClosed {
		p.LiveRecipients = nil
		p.DeliveryIntents = nil
		p.RoutedRecipients = nil
		p.SubscribedRecipients = nil
		p.ActivationPlans = nil
		p.ReplyCreations = nil
		p.ReplyClaims = nil
	}
	return p
}

func (p *RoutePlan) MarkCanonicalRouteMatched(producer routeIntentProducer) {
	if p == nil {
		return
	}
	p.AuthorityState = RoutePlanAuthorityCanonicalMatched
	p.AuthorityOwner = producer.Source()
}

func (p *RoutePlan) MarkCanonicalRouteFailedClosed(producer routeIntentProducer, failure runtimepinrouting.TargetFailure) {
	if p == nil {
		return
	}
	p.AuthorityState = RoutePlanAuthorityCanonicalFailedClosed
	p.AuthorityOwner = producer.Source()
	p.TargetFailure = failure
	p.LiveRecipients = nil
	p.DeliveryIntents = nil
	p.RoutedRecipients = nil
	p.SubscribedRecipients = nil
}

func (p *RoutePlan) MarkLowerPrecedenceRouteProduction(producer routeIntentProducer) {
	if p == nil || p.CanonicalRouteOwnerMatched() {
		return
	}
	p.AuthorityState = RoutePlanAuthorityLowerPrecedence
	if p.AuthorityOwner == 0 {
		p.AuthorityOwner = producer.Source()
	}
}

func (p RoutePlan) CanonicalRouteOwnerMatched() bool {
	switch normalizeRoutePlanAuthorityState(p.AuthorityState) {
	case RoutePlanAuthorityCanonicalMatched, RoutePlanAuthorityCanonicalFailedClosed:
		return true
	default:
		return false
	}
}

func (p RoutePlan) AllowsLowerPrecedenceRouteProduction() bool {
	switch normalizeRoutePlanAuthorityState(p.AuthorityState) {
	case RoutePlanAuthorityNoCanonicalMatch, RoutePlanAuthorityLowerPrecedence:
		return true
	default:
		return false
	}
}

func (p PublishRecipientPlan) UsesCanonicalRouteAuthority() bool {
	return p.canonicalAuthority
}

func (p *RoutePlan) AddLiveRecipients(recipients ...RoutePlanLiveRecipient) {
	if p == nil {
		return
	}
	p.LiveRecipients = normalizeRoutePlanLiveRecipients(append(p.LiveRecipients, recipients...))
}

func (p *RoutePlan) AddDeliveryIntents(intents ...RoutePlanDeliveryIntent) {
	if p == nil {
		return
	}
	p.DeliveryIntents = normalizeRoutePlanDeliveryIntents(append(p.DeliveryIntents, intents...))
}

func (p RoutePlan) WithDefaultDeliveryContext(deliveryContext events.DeliveryContext) RoutePlan {
	p = p.Normalized()
	deliveryContext = deliveryContext.Normalized()
	if deliveryContext.Empty() || p.ReplyContextConsumed {
		return p
	}
	for i := range p.DeliveryIntents {
		if p.DeliveryIntents[i].Context.Empty() {
			p.DeliveryIntents[i].Context = deliveryContext
		}
	}
	return p.Normalized()
}

func (p RoutePlan) RecipientIDs() []string {
	p = p.Normalized()
	out := make([]string, 0, len(p.LiveRecipients))
	for _, recipient := range p.LiveRecipients {
		out = append(out, recipient.Recipient.ID())
	}
	return uniqueStrings(out)
}

func (p RoutePlan) PersistedRecipientIDs() []string {
	p = p.Normalized()
	out := make([]string, 0, len(p.LiveRecipients))
	for _, recipient := range p.LiveRecipients {
		if !recipient.PersistAsDelivery {
			continue
		}
		out = append(out, recipient.Recipient.ID())
	}
	return uniqueStrings(out)
}

func (p RoutePlan) DeliveryTargets() map[string]events.RouteIdentity {
	p = p.Normalized()
	out := map[string]events.RouteIdentity{}
	for _, intent := range p.DeliveryIntents {
		if !intent.Recipient.IsAgent() {
			continue
		}
		if intent.TargetOwnership.Empty() {
			continue
		}
		out[intent.Recipient.ID()] = intent.TargetOwnership.Route()
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (p RoutePlan) DeliveryRoutes() []events.DeliveryRoute {
	p = p.Normalized()
	out := make([]events.DeliveryRoute, 0, len(p.DeliveryIntents))
	for _, intent := range p.DeliveryIntents {
		if !intent.Persist {
			continue
		}
		out = append(out, events.DeliveryRoute{
			Recipient:         intent.Recipient,
			AgentIdentity:     intent.AgentIdentity,
			Target:            intent.TargetOwnership,
			Context:           intent.Context,
			PayloadProjection: intent.PayloadProjection,
			ConnectClaim:      intent.ConnectClaim,
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

func (p RoutePlan) liveDispatchDeliveryRoutes() []events.DeliveryRoute {
	p = p.Normalized()
	liveAgents := make(map[agentidentity.Identity]struct{}, len(p.LiveRecipients))
	for _, recipient := range p.LiveRecipients {
		if recipient.Recipient.IsAgent() {
			liveAgents[recipient.AgentIdentity] = struct{}{}
		}
	}
	out := make([]events.DeliveryRoute, 0, len(p.DeliveryIntents))
	for _, intent := range p.DeliveryIntents {
		if !intent.Persist {
			continue
		}
		if intent.Recipient.IsAgent() && intent.PendingAgentLifecycle {
			if _, live := liveAgents[intent.AgentIdentity]; !live {
				continue
			}
		}
		out = append(out, events.DeliveryRoute{
			Recipient:         intent.Recipient,
			AgentIdentity:     intent.AgentIdentity,
			Target:            intent.TargetOwnership,
			Context:           intent.Context,
			PayloadProjection: intent.PayloadProjection,
			ConnectClaim:      intent.ConnectClaim,
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

// ValidatePersistentDeliveries proves that every admitted persistent live
// agent recipient has one durable route. A durable agent route must either
// have a live recipient or carry explicit same-plan lifecycle-creation
// authority. Node routes are durable pipeline authority and do not require a
// LiveRecipients entry.
func (p RoutePlan) ValidatePersistentDeliveries() error {
	p = p.Normalized()
	liveAgents := make(map[agentidentity.Identity]struct{})
	for index, recipient := range p.LiveRecipients {
		if !recipient.PersistAsDelivery {
			continue
		}
		if !recipient.Recipient.IsAgent() {
			return fmt.Errorf("persistent live recipient %d must identify one agent", index)
		}
		if err := recipient.AgentIdentity.Validate(); err != nil {
			return fmt.Errorf("persistent live recipient %d agent identity: %w", index, err)
		}
		if recipient.AgentIdentity.AgentID() != recipient.Recipient.ID() {
			return fmt.Errorf("persistent live recipient %d subscriber id does not match agent identity", index)
		}
		liveAgents[recipient.AgentIdentity] = struct{}{}
	}
	routeAgents := make(map[agentidentity.Identity]struct{})
	pendingAgents := make(map[agentidentity.Identity]struct{})
	for _, intent := range p.DeliveryIntents {
		if !intent.PendingAgentLifecycle {
			continue
		}
		if !intent.Persist {
			return fmt.Errorf("pending lifecycle delivery intent must be persistent")
		}
		if !intent.Recipient.IsAgent() {
			return fmt.Errorf("pending lifecycle delivery intent must identify one agent")
		}
		if err := intent.AgentIdentity.Validate(); err != nil || intent.AgentIdentity.AgentID() != intent.Recipient.ID() {
			return fmt.Errorf("pending lifecycle delivery intent has invalid agent identity")
		}
		pendingAgents[intent.AgentIdentity] = struct{}{}
	}
	routes := p.DeliveryRoutes()
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		return err
	}
	for _, route := range routes {
		if route.Recipient.IsAgent() {
			routeAgents[route.AgentIdentity] = struct{}{}
		}
	}
	for identity := range liveAgents {
		if _, ok := routeAgents[identity]; !ok {
			return fmt.Errorf("persistent live agent %q has no exact durable delivery route", identity.Description())
		}
	}
	for identity := range routeAgents {
		if _, live := liveAgents[identity]; live {
			continue
		}
		if _, pending := pendingAgents[identity]; !pending {
			return fmt.Errorf("durable agent delivery route %q has no persistent live recipient or pending lifecycle authority", identity.Description())
		}
	}
	for identity := range pendingAgents {
		if _, ok := routeAgents[identity]; !ok {
			return fmt.Errorf("pending lifecycle agent %q has no exact durable delivery route", identity.Description())
		}
	}
	return nil
}

func (p RoutePlan) HasPersistentDeliveries() bool {
	return len(p.PersistedRecipientIDs()) > 0 || len(p.DeliveryRoutes()) > 0
}

func (p RoutePlan) InternalDeliveryRoutes() []events.DeliveryRoute {
	p = p.Normalized()
	internalRecipients := p.InternalRecipientIDs()
	if len(internalRecipients) == 0 {
		return nil
	}
	known := p.DeliveryRoutes()
	out := make([]events.DeliveryRoute, 0, len(known))
	internalSet := make(map[string]struct{}, len(internalRecipients))
	for _, recipient := range internalRecipients {
		internalSet[strings.TrimSpace(recipient)] = struct{}{}
	}
	if _, carrier := internalSet[workflowRuntimeInternalCarrierID]; carrier {
		for _, route := range known {
			if route.Recipient.IsNode() && route.Recipient.ID() != workflowRuntimeInternalCarrierID {
				out = append(out, route)
			}
		}
		return events.NormalizeDeliveryRoutes(out)
	}
	for _, route := range known {
		if _, ok := internalSet[route.Recipient.ID()]; !ok {
			continue
		}
		out = append(out, route)
	}
	if len(out) == 0 {
		return nil
	}
	return events.NormalizeDeliveryRoutes(out)
}

func (p RoutePlan) InternalRecipientIDs() []string {
	p = p.Normalized()
	internal := make([]string, 0, len(p.LiveRecipients))
	for _, recipient := range p.LiveRecipients {
		if !recipient.PersistAsDelivery {
			internal = append(internal, recipient.Recipient.ID())
		}
	}
	return uniqueStrings(internal)
}

func routePlanLiveRecipientsFromManifest(manifest deliveryRecipientManifest, producer routeIntentProducer) []RoutePlanLiveRecipient {
	persisted := make(map[string]struct{}, len(manifest.PersistedRecipients))
	for _, recipient := range uniqueStrings(manifest.PersistedRecipients) {
		persisted[recipient] = struct{}{}
	}
	candidates := normalizeDeliveryRecipientCandidates(manifest.LiveRecipients)
	if len(candidates) == 0 {
		for _, recipient := range uniqueStrings(manifest.Recipients) {
			_, persist := persisted[recipient]
			if persist {
				continue
			}
			candidates = append(candidates, deliveryRecipientCandidate{
				ID:                recipient,
				PersistAsDelivery: false,
				LiveAuthority:     liveRecipientAuthorityIdentity,
			})
		}
	}
	out := make([]RoutePlanLiveRecipient, 0, len(candidates))
	for _, candidate := range candidates {
		recipient := candidate.ID
		persist := false
		if _, ok := persisted[recipient]; ok {
			persist = true
		}
		typedRecipient := events.MustNodeDeliveryRecipient(recipient)
		if persist {
			typedRecipient = events.MustAgentDeliveryRecipient(recipient)
		}
		out = append(out, RoutePlanLiveRecipient{
			Recipient:         typedRecipient,
			AgentIdentity:     candidate.AgentIdentity,
			PersistAsDelivery: persist,
			Producer:          producer,
			liveAuthority:     candidate.LiveAuthority.Normalized(),
			agentRoute:        candidate.AgentRoute,
		})
	}
	return normalizeRoutePlanLiveRecipients(out)
}

func routePlanDeliveryIntentsFromAdmittedRoutes(routes []events.DeliveryRoute, producer routeIntentProducer) []RoutePlanDeliveryIntent {
	routes = events.NormalizeDeliveryRoutes(routes)
	if len(routes) == 0 {
		return nil
	}
	out := make([]RoutePlanDeliveryIntent, 0, len(routes))
	for _, route := range routes {
		out = append(out, RoutePlanDeliveryIntent{
			Recipient:         route.Recipient,
			AgentIdentity:     route.AgentIdentity,
			TargetBlueprint:   route.Target.Route(),
			TargetOwnership:   route.Target,
			Context:           route.Context,
			PayloadProjection: route.PayloadProjection,
			ConnectClaim:      route.ConnectClaim,
			Producer:          producer,
			Persist:           true,
		})
	}
	return normalizeRoutePlanDeliveryIntents(out)
}

func routePlanDeliveryIntentsFromRoutes(routes []plannedDeliveryRoute, producer routeIntentProducer) []RoutePlanDeliveryIntent {
	routes = normalizePlannedDeliveryRoutes(routes)
	out := make([]RoutePlanDeliveryIntent, 0, len(routes))
	for _, route := range routes {
		out = append(out, RoutePlanDeliveryIntent{
			Recipient: route.Recipient, AgentIdentity: route.AgentIdentity, TargetBlueprint: route.Target,
			Handler: route.Handler, Context: route.Context, PayloadProjection: route.PayloadProjection, ConnectClaim: route.ConnectClaim,
			Producer: producer, Persist: true,
		})
	}
	return normalizeRoutePlanDeliveryIntents(out)
}

func routePlanDeliveryIntentsFromConnectRoutes(routes []runtimepinrouting.ConnectDeliveryRoute, producer routeIntentProducer, receiverEvent events.EventType) []RoutePlanDeliveryIntent {
	routes = runtimepinrouting.NormalizeConnectDeliveryRoutes(routes)
	out := make([]RoutePlanDeliveryIntent, 0, len(routes))
	for _, route := range routes {
		handler := runtimepipeline.DeliveryTargetHandler{}
		if !route.Handler.Empty() {
			handler = runtimepipeline.MustDeliveryTargetHandler(route.Handler.FlowID(), route.Handler.NodeID()).ForEvent(receiverEvent)
		}
		out = append(out, RoutePlanDeliveryIntent{
			Recipient: route.Recipient, AgentIdentity: route.AgentIdentity, TargetBlueprint: route.Target,
			Handler: handler, Context: route.Context, PayloadProjection: route.PayloadProjection, ConnectClaim: route.ConnectClaim,
			Producer: producer, Persist: true,
		})
	}
	return normalizeRoutePlanDeliveryIntents(out)
}

func deliveryRouteLiveRecipients(routes []events.DeliveryRoute) []RoutePlanLiveRecipient {
	routes = events.NormalizeDeliveryRoutes(routes)
	out := make([]RoutePlanLiveRecipient, 0, len(routes))
	for _, route := range routes {
		out = append(out, RoutePlanLiveRecipient{
			Recipient: route.Recipient, AgentIdentity: route.AgentIdentity,
			PersistAsDelivery: route.Recipient.IsAgent(), Producer: routeIntentProducerConnectRoutePlan,
		})
	}
	return normalizeRoutePlanLiveRecipients(out)
}

func routePlanFromManifest(evt events.Event, manifest deliveryRecipientManifest, producer routeIntentProducer) RoutePlan {
	plan := newRoutePlan(evt)
	plan.AddLiveRecipients(routePlanLiveRecipientsFromManifest(manifest, producer)...)
	plan.AddDeliveryIntents(routePlanDeliveryIntentsFromAdmittedRoutes(manifest.DeliveryRoutes, producer)...)
	plan.TargetFailure = manifest.TargetFailure
	if len(plan.LiveRecipients) > 0 || len(plan.DeliveryIntents) > 0 || !plan.TargetFailure.Empty() {
		plan.MarkLowerPrecedenceRouteProduction(producer)
	}
	return plan.Normalized()
}

func normalizeRoutePlanAuthorityState(state RoutePlanAuthorityState) RoutePlanAuthorityState {
	switch state {
	case RoutePlanAuthorityCanonicalMatched:
		return RoutePlanAuthorityCanonicalMatched
	case RoutePlanAuthorityCanonicalFailedClosed:
		return RoutePlanAuthorityCanonicalFailedClosed
	case RoutePlanAuthorityLowerPrecedence:
		return RoutePlanAuthorityLowerPrecedence
	default:
		return RoutePlanAuthorityNoCanonicalMatch
	}
}

func normalizeRoutePlanSource(source routePlanSource) routePlanSource {
	switch source {
	case routePlanSourceAgentPolicy, routePlanSourceDirectPolicy, routePlanSourceInternalTarget,
		routePlanSourceConcreteNodeRoute, routePlanSourceScopedNodeRoute, routePlanSourceRootNodeRoute,
		routePlanSourceRootInputFlowNode, routePlanSourceRecipientMaterializer, routePlanSourceConnectRoutePlan:
		return source
	default:
		return 0
	}
}

type liveRecipientKey struct {
	recipient     events.DeliveryRecipient
	agentIdentity agentidentity.Identity
}

func normalizeRoutePlanLiveRecipients(in []RoutePlanLiveRecipient) []RoutePlanLiveRecipient {
	if len(in) == 0 {
		return nil
	}
	out := make([]RoutePlanLiveRecipient, 0, len(in))
	indexByKey := make(map[liveRecipientKey]int, len(in))
	for _, recipient := range in {
		recipient.AgentIdentity = recipient.AgentIdentity.Normalize()
		recipient.Producer = recipient.Producer.Normalized()
		recipient.liveAuthority = recipient.liveAuthority.Normalized()
		if recipient.liveAuthority == liveRecipientAuthorityIdentity {
			recipient.agentRoute = nil
		}
		if recipient.Recipient.Empty() {
			continue
		}
		if recipient.Recipient.IsAgent() {
			if err := recipient.AgentIdentity.Validate(); err != nil || recipient.AgentIdentity.AgentID() != recipient.Recipient.ID() {
				continue
			}
		} else if !recipient.Recipient.IsNode() || !recipient.AgentIdentity.IsZero() {
			continue
		}
		key := liveRecipientKey{
			recipient:     recipient.Recipient,
			agentIdentity: recipient.AgentIdentity,
		}
		if idx, ok := indexByKey[key]; ok {
			out[idx].PersistAsDelivery = out[idx].PersistAsDelivery || recipient.PersistAsDelivery
			out[idx] = mergeRoutePlanLiveRecipientAuthority(out[idx], recipient)
			if out[idx].Producer.Empty() {
				out[idx].Producer = recipient.Producer
			}
			continue
		}
		indexByKey[key] = len(out)
		out = append(out, recipient)
	}
	return out
}

func mergeRoutePlanLiveRecipientAuthority(current, candidate RoutePlanLiveRecipient) RoutePlanLiveRecipient {
	if current.liveAuthority.Normalized() == liveRecipientAuthorityIdentity || candidate.liveAuthority.Normalized() == liveRecipientAuthorityIdentity {
		current.liveAuthority = liveRecipientAuthorityIdentity
		current.agentRoute = nil
		return current
	}
	current.liveAuthority = liveRecipientAuthorityAgentRoute
	if current.agentRoute == nil {
		current.agentRoute = candidate.agentRoute
	}
	return current
}

type deliveryIntentKey struct {
	recipient      events.DeliveryRecipient
	agentIdentity  agentidentity.Identity
	target         events.RouteIdentity
	targetOwner    events.DeliveryTargetOwnership
	handler        runtimepipeline.DeliveryTargetHandler
	replyContextID string
	projection     string
	connectClaim   events.ConnectExecutionClaim
	structural     runtimepinrouting.StructuralTargetOwnerProof
}

func normalizeRoutePlanDeliveryIntents(in []RoutePlanDeliveryIntent) []RoutePlanDeliveryIntent {
	if len(in) == 0 {
		return nil
	}
	out := make([]RoutePlanDeliveryIntent, 0, len(in))
	indexByKey := make(map[deliveryIntentKey]int, len(in))
	for _, intent := range in {
		intent.AgentIdentity = intent.AgentIdentity.Normalize()
		intent.TargetBlueprint = intent.TargetBlueprint.Normalized()
		intent.Context = intent.Context.Normalized()
		intent.PayloadProjection = intent.PayloadProjection.Normalized()
		intent.Producer = intent.Producer.Normalized()
		if intent.Recipient.Empty() {
			out = append(out, intent)
			continue
		}
		if intent.Recipient.IsAgent() {
			if err := intent.AgentIdentity.Validate(); err != nil || intent.AgentIdentity.AgentID() != intent.Recipient.ID() {
				continue
			}
		} else if !intent.Recipient.IsNode() || !intent.AgentIdentity.IsZero() {
			continue
		}
		key := deliveryIntentKey{
			recipient:      intent.Recipient,
			agentIdentity:  intent.AgentIdentity,
			target:         intent.TargetBlueprint,
			targetOwner:    intent.TargetOwnership,
			handler:        intent.Handler,
			replyContextID: intent.Context.ReplyContextID(),
			projection:     intent.PayloadProjection.Fingerprint(),
			connectClaim:   intent.ConnectClaim,
			structural:     intent.StructuralOwnerProof,
		}
		if idx, ok := indexByKey[key]; ok {
			out[idx].Persist = out[idx].Persist || intent.Persist
			out[idx].PendingAgentLifecycle = out[idx].PendingAgentLifecycle || intent.PendingAgentLifecycle
			if out[idx].Handler.Empty() {
				out[idx].Handler = intent.Handler
			}
			if out[idx].Producer.Empty() {
				out[idx].Producer = intent.Producer
			}
			continue
		}
		indexByKey[key] = len(out)
		out = append(out, intent)
	}
	return out
}

func cloneStringAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func containsString(in []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, item := range in {
		if strings.TrimSpace(item) == want {
			return true
		}
	}
	return false
}
