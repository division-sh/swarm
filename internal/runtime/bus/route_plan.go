package bus

import (
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
)

const (
	routePlanSubscriberAgent    = "agent"
	routePlanSubscriberInternal = "internal"
)

type routePlanSource string
type routePlanReason string

const (
	routePlanSourceAgentPolicy           routePlanSource = "agent_policy"
	routePlanSourceDirectPolicy          routePlanSource = "direct_policy"
	routePlanSourceInternalTarget        routePlanSource = "internal_target_route"
	routePlanSourceConcreteNodeRoute     routePlanSource = "concrete_node_route"
	routePlanSourceScopedNodeRoute       routePlanSource = "scoped_node_route"
	routePlanSourceRootNodeRoute         routePlanSource = "root_node_route"
	routePlanSourceRootInputFlowNode     routePlanSource = "root_input_flow_node_route"
	routePlanSourceRecipientMaterializer routePlanSource = "recipient_plan_materializer"
	routePlanSourceConnectRoutePlan      routePlanSource = "connect_route_plan"

	routePlanReasonMatchedAgentSubscription routePlanReason = "matched_agent_subscription"
	routePlanReasonDirectPublish            routePlanReason = "direct_publish"
	routePlanReasonInternalCarrier          routePlanReason = "internal_carrier"
	routePlanReasonRouteTableNode           routePlanReason = "route_table_node"
	routePlanReasonMaterializedRoute        routePlanReason = "materialized_route"
	routePlanReasonLoweredConnectRoutePlan  routePlanReason = "lowered_connect_route_plan"
)

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
	case routeIntentProducerRecipientMaterializer:
		return routePlanSourceRecipientMaterializer
	case routeIntentProducerConnectRoutePlan:
		return routePlanSourceConnectRoutePlan
	default:
		return ""
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
		routeIntentProducerRootInputFlowNode:
		return routePlanReasonRouteTableNode
	case routeIntentProducerRecipientMaterializer:
		return routePlanReasonMaterializedRoute
	case routeIntentProducerConnectRoutePlan:
		return routePlanReasonLoweredConnectRoutePlan
	default:
		return ""
	}
}

func (p routeIntentProducer) Empty() bool {
	return p.Normalized() == routeIntentProducerUnknown
}

func (p routeIntentProducer) String() string {
	p = p.Normalized()
	source := p.Source()
	reason := p.Reason()
	if source == "" {
		return string(reason)
	}
	if reason == "" {
		return string(source)
	}
	return string(source) + "/" + string(reason)
}

type RoutePlanAuthorityState string

const (
	RoutePlanAuthorityNoCanonicalMatch      RoutePlanAuthorityState = "no_canonical_match"
	RoutePlanAuthorityCanonicalMatched      RoutePlanAuthorityState = "canonical_matched"
	RoutePlanAuthorityCanonicalFailedClosed RoutePlanAuthorityState = "canonical_failed_closed"
	RoutePlanAuthorityLowerPrecedence       RoutePlanAuthorityState = "lower_precedence"
)

// RoutePlan is the EventBus-owned publish-time route authority. It records the
// typed delivery intents that should be persisted and the live dispatch
// recipients that remain only projections/consumers of that authority.
type RoutePlan struct {
	Event                events.Event
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
}

type RoutePlanLiveRecipient struct {
	RecipientID       string
	SubscriberType    string
	PersistAsDelivery bool
	Producer          routeIntentProducer
}

type RoutePlanDeliveryIntent struct {
	SubscriberType string
	SubscriberID   string
	Target         events.RouteIdentity
	Producer       routeIntentProducer
	Persist        bool
}

func newRoutePlan(evt events.Event) RoutePlan {
	return RoutePlan{Event: evt, AuthorityState: RoutePlanAuthorityNoCanonicalMatch}
}

func (p RoutePlan) Normalized() RoutePlan {
	p.AuthorityState = normalizeRoutePlanAuthorityState(p.AuthorityState)
	p.AuthorityOwner = normalizeRoutePlanSource(p.AuthorityOwner)
	p.LiveRecipients = normalizeRoutePlanLiveRecipients(p.LiveRecipients)
	p.DeliveryIntents = normalizeRoutePlanDeliveryIntents(p.DeliveryIntents)
	p.RoutedRecipients = append([]Subscriber(nil), p.RoutedRecipients...)
	p.SubscribedRecipients = uniqueStrings(p.SubscribedRecipients)
	p.ExtraDetail = cloneStringAnyMap(p.ExtraDetail)
	p.TargetFailure = runtimepinrouting.TargetFailure(strings.TrimSpace(string(p.TargetFailure)))
	p.ContradictionReason = strings.TrimSpace(p.ContradictionReason)
	if p.CycleEscalation != nil {
		evt := *p.CycleEscalation
		p.CycleEscalation = &evt
	}
	if p.AuthorityState == RoutePlanAuthorityCanonicalMatched && p.TargetFailure != "" {
		p.AuthorityState = RoutePlanAuthorityCanonicalFailedClosed
	}
	if p.AuthorityState == RoutePlanAuthorityCanonicalFailedClosed {
		p.LiveRecipients = nil
		p.DeliveryIntents = nil
		p.RoutedRecipients = nil
		p.SubscribedRecipients = nil
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
	p.TargetFailure = runtimepinrouting.TargetFailure(strings.TrimSpace(string(failure)))
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
	if p.AuthorityOwner == "" {
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

func (p RoutePlan) RecipientIDs() []string {
	p = p.Normalized()
	out := make([]string, 0, len(p.LiveRecipients))
	for _, recipient := range p.LiveRecipients {
		out = append(out, recipient.RecipientID)
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
		out = append(out, recipient.RecipientID)
	}
	return uniqueStrings(out)
}

func (p RoutePlan) DeliveryTargets() map[string]events.RouteIdentity {
	p = p.Normalized()
	out := map[string]events.RouteIdentity{}
	for _, intent := range p.DeliveryIntents {
		if intent.SubscriberType != routePlanSubscriberAgent {
			continue
		}
		if intent.Target.Empty() {
			continue
		}
		out[intent.SubscriberID] = intent.Target.Normalized()
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
			SubscriberType: intent.SubscriberType,
			SubscriberID:   intent.SubscriberID,
			Target:         intent.Target,
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

func (p RoutePlan) HasPersistentDeliveries() bool {
	return len(p.PersistedRecipientIDs()) > 0 || len(p.DeliveryRoutes()) > 0
}

func (p RoutePlan) InternalDeliveryRoutes() []events.DeliveryRoute {
	p = p.Normalized()
	internalRecipients := make([]string, 0, len(p.LiveRecipients))
	for _, recipient := range p.LiveRecipients {
		if recipient.PersistAsDelivery {
			continue
		}
		internalRecipients = append(internalRecipients, recipient.RecipientID)
	}
	internalRecipients = uniqueStrings(internalRecipients)
	if len(internalRecipients) == 0 {
		return nil
	}
	known := p.DeliveryRoutes()
	out := make([]events.DeliveryRoute, 0, len(known))
	internalSet := make(map[string]struct{}, len(internalRecipients))
	for _, recipient := range internalRecipients {
		internalSet[strings.TrimSpace(recipient)] = struct{}{}
	}
	for _, route := range known {
		if _, ok := internalSet[strings.TrimSpace(route.SubscriberID)]; !ok {
			continue
		}
		out = append(out, route)
	}
	if len(out) == 0 {
		for _, recipient := range internalRecipients {
			out = append(out, events.DeliveryRoute{
				SubscriberType: "node",
				SubscriberID:   recipient,
			})
		}
	}
	return events.NormalizeDeliveryRoutes(out)
}

func routePlanLiveRecipientsFromManifest(manifest deliveryRecipientManifest, producer routeIntentProducer) []RoutePlanLiveRecipient {
	persisted := make(map[string]struct{}, len(manifest.PersistedRecipients))
	for _, recipient := range uniqueStrings(manifest.PersistedRecipients) {
		persisted[recipient] = struct{}{}
	}
	recipientIDs := uniqueStrings(append(append([]string(nil), manifest.Recipients...), manifest.PersistedRecipients...))
	out := make([]RoutePlanLiveRecipient, 0, len(recipientIDs))
	for _, recipient := range recipientIDs {
		subscriberType := routePlanSubscriberInternal
		persist := false
		if _, ok := persisted[recipient]; ok {
			subscriberType = routePlanSubscriberAgent
			persist = true
		}
		out = append(out, RoutePlanLiveRecipient{
			RecipientID:       recipient,
			SubscriberType:    subscriberType,
			PersistAsDelivery: persist,
			Producer:          producer,
		})
	}
	return normalizeRoutePlanLiveRecipients(out)
}

func routePlanDeliveryIntentsFromRoutes(routes []events.DeliveryRoute, producer routeIntentProducer) []RoutePlanDeliveryIntent {
	routes = events.NormalizeDeliveryRoutes(routes)
	if len(routes) == 0 {
		return nil
	}
	out := make([]RoutePlanDeliveryIntent, 0, len(routes))
	for _, route := range routes {
		out = append(out, RoutePlanDeliveryIntent{
			SubscriberType: route.SubscriberType,
			SubscriberID:   route.SubscriberID,
			Target:         route.Target,
			Producer:       producer,
			Persist:        true,
		})
	}
	return normalizeRoutePlanDeliveryIntents(out)
}

func routePlanFromManifest(evt events.Event, manifest deliveryRecipientManifest, producer routeIntentProducer) RoutePlan {
	plan := newRoutePlan(evt)
	plan.AddLiveRecipients(routePlanLiveRecipientsFromManifest(manifest, producer)...)
	plan.AddDeliveryIntents(routePlanDeliveryIntentsFromRoutes(manifest.DeliveryRoutes, producer)...)
	plan.TargetFailure = manifest.TargetFailure
	if len(plan.LiveRecipients) > 0 || len(plan.DeliveryIntents) > 0 || plan.TargetFailure != "" {
		plan.MarkLowerPrecedenceRouteProduction(producer)
	}
	return plan.Normalized()
}

func normalizeRoutePlanAuthorityState(state RoutePlanAuthorityState) RoutePlanAuthorityState {
	switch RoutePlanAuthorityState(strings.TrimSpace(string(state))) {
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
	return routePlanSource(strings.TrimSpace(string(source)))
}

func normalizeRoutePlanLiveRecipients(in []RoutePlanLiveRecipient) []RoutePlanLiveRecipient {
	if len(in) == 0 {
		return nil
	}
	out := make([]RoutePlanLiveRecipient, 0, len(in))
	indexByKey := make(map[string]int, len(in))
	for _, recipient := range in {
		recipient.RecipientID = strings.TrimSpace(recipient.RecipientID)
		recipient.SubscriberType = strings.TrimSpace(recipient.SubscriberType)
		recipient.Producer = recipient.Producer.Normalized()
		if recipient.RecipientID == "" {
			continue
		}
		if recipient.SubscriberType == "" {
			if recipient.PersistAsDelivery {
				recipient.SubscriberType = routePlanSubscriberAgent
			} else {
				recipient.SubscriberType = routePlanSubscriberInternal
			}
		}
		key := strings.Join([]string{recipient.SubscriberType, recipient.RecipientID}, "\x00")
		if idx, ok := indexByKey[key]; ok {
			out[idx].PersistAsDelivery = out[idx].PersistAsDelivery || recipient.PersistAsDelivery
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

func normalizeRoutePlanDeliveryIntents(in []RoutePlanDeliveryIntent) []RoutePlanDeliveryIntent {
	if len(in) == 0 {
		return nil
	}
	out := make([]RoutePlanDeliveryIntent, 0, len(in))
	indexByKey := make(map[string]int, len(in))
	for _, intent := range in {
		intent.SubscriberType = strings.TrimSpace(intent.SubscriberType)
		intent.SubscriberID = strings.TrimSpace(intent.SubscriberID)
		intent.Target = intent.Target.Normalized()
		intent.Producer = intent.Producer.Normalized()
		if intent.SubscriberType == "" || intent.SubscriberID == "" {
			continue
		}
		key := strings.Join([]string{intent.SubscriberType, intent.SubscriberID, intent.Target.FlowID, intent.Target.FlowInstance, intent.Target.EntityID}, "\x00")
		if idx, ok := indexByKey[key]; ok {
			out[idx].Persist = out[idx].Persist || intent.Persist
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
