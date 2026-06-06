package bus

import (
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
)

const (
	routePlanSubscriberAgent    = "agent"
	routePlanSubscriberInternal = "internal"

	routePlanSourceAgentPolicy           = "agent_policy"
	routePlanSourceDirectPolicy          = "direct_policy"
	routePlanSourceInternalTarget        = "internal_target_route"
	routePlanSourceConcreteNodeRoute     = "concrete_node_route"
	routePlanSourceScopedNodeRoute       = "scoped_node_route"
	routePlanSourceRootNodeRoute         = "root_node_route"
	routePlanSourceRootInputFlowNode     = "root_input_flow_node_route"
	routePlanSourceRecipientMaterializer = "recipient_plan_materializer"
	routePlanSourceLegacyProjection      = "legacy_projection"

	routePlanReasonMatchedAgentSubscription = "matched_agent_subscription"
	routePlanReasonDirectPublish            = "direct_publish"
	routePlanReasonInternalCarrier          = "internal_carrier"
	routePlanReasonRouteTableNode           = "route_table_node"
	routePlanReasonMaterializedRoute        = "materialized_route"
)

// RoutePlan is the EventBus-owned publish-time route authority. It records the
// typed delivery intents that should be persisted and the live dispatch
// recipients that remain only projections/consumers of that authority.
type RoutePlan struct {
	Event                events.Event
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
	Source            string
	Reason            string
}

type RoutePlanDeliveryIntent struct {
	SubscriberType string
	SubscriberID   string
	Target         events.RouteIdentity
	Source         string
	Reason         string
	Persist        bool
}

func newRoutePlan(evt events.Event) RoutePlan {
	return RoutePlan{Event: evt}
}

func (p RoutePlan) Normalized() RoutePlan {
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
	return p
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

func (p RoutePlan) EventDeliveryPlan() eventDeliveryPlan {
	p = p.Normalized()
	return eventDeliveryPlan{
		RoutePlan:            p,
		Event:                p.Event,
		Recipients:           p.RecipientIDs(),
		PersistedRecipients:  p.PersistedRecipientIDs(),
		DeliveryTargets:      p.DeliveryTargets(),
		DeliveryRoutes:       p.DeliveryRoutes(),
		RoutedRecipients:     append([]Subscriber(nil), p.RoutedRecipients...),
		SubscribedRecipients: uniqueStrings(p.SubscribedRecipients),
		ExtraDetail:          cloneStringAnyMap(p.ExtraDetail),
		TargetFailure:        p.TargetFailure,
		ContradictionReason:  p.ContradictionReason,
		BlockedByCycle:       p.BlockedByCycle,
		CycleEscalation:      p.CycleEscalation,
	}
}

func (p eventDeliveryPlan) CanonicalRoutePlan() RoutePlan {
	if len(p.RoutePlan.LiveRecipients) > 0 || len(p.RoutePlan.DeliveryIntents) > 0 || p.RoutePlan.Event.Type() != "" || p.RoutePlan.Event.ID() != "" {
		plan := p.RoutePlan.Normalized()
		if plan.Event.Type() == "" && plan.Event.ID() == "" {
			plan.Event = p.Event
		}
		return plan
	}
	return routePlanFromLegacyEventDeliveryPlan(p)
}

func (p eventDeliveryPlan) WithCanonicalRoutePlan(routePlan RoutePlan) eventDeliveryPlan {
	routePlan = routePlan.Normalized()
	if strings.TrimSpace(p.ContradictionReason) != "" {
		routePlan.ContradictionReason = p.ContradictionReason
	}
	if p.BlockedByCycle {
		routePlan.BlockedByCycle = true
	}
	if p.CycleEscalation != nil {
		evt := *p.CycleEscalation
		routePlan.CycleEscalation = &evt
	}
	return routePlan.EventDeliveryPlan()
}

func routePlanFromLegacyEventDeliveryPlan(plan eventDeliveryPlan) RoutePlan {
	out := newRoutePlan(plan.Event)
	persisted := make(map[string]struct{}, len(plan.PersistedRecipients))
	for _, recipient := range uniqueStrings(plan.PersistedRecipients) {
		persisted[recipient] = struct{}{}
	}
	for _, recipient := range uniqueStrings(plan.Recipients) {
		subscriberType := routePlanSubscriberInternal
		persist := false
		if _, ok := persisted[recipient]; ok {
			subscriberType = routePlanSubscriberAgent
			persist = true
		}
		out.AddLiveRecipients(RoutePlanLiveRecipient{
			RecipientID:       recipient,
			SubscriberType:    subscriberType,
			PersistAsDelivery: persist,
			Source:            routePlanSourceLegacyProjection,
			Reason:            routePlanSourceLegacyProjection,
		})
	}
	for recipient := range persisted {
		if containsString(out.RecipientIDs(), recipient) {
			continue
		}
		out.AddLiveRecipients(RoutePlanLiveRecipient{
			RecipientID:       recipient,
			SubscriberType:    routePlanSubscriberAgent,
			PersistAsDelivery: true,
			Source:            routePlanSourceLegacyProjection,
			Reason:            routePlanSourceLegacyProjection,
		})
	}
	out.AddDeliveryIntents(routePlanDeliveryIntentsFromRoutes(plan.DeliveryRoutes, routePlanSourceLegacyProjection, routePlanSourceLegacyProjection)...)
	out.RoutedRecipients = append([]Subscriber(nil), plan.RoutedRecipients...)
	out.SubscribedRecipients = uniqueStrings(plan.SubscribedRecipients)
	out.ExtraDetail = cloneStringAnyMap(plan.ExtraDetail)
	out.TargetFailure = plan.TargetFailure
	out.ContradictionReason = plan.ContradictionReason
	out.BlockedByCycle = plan.BlockedByCycle
	out.CycleEscalation = plan.CycleEscalation
	return out.Normalized()
}

func routePlanLiveRecipientsFromManifest(manifest deliveryRecipientManifest, source, reason string) []RoutePlanLiveRecipient {
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
			Source:            source,
			Reason:            reason,
		})
	}
	return normalizeRoutePlanLiveRecipients(out)
}

func routePlanDeliveryIntentsFromRoutes(routes []events.DeliveryRoute, source, reason string) []RoutePlanDeliveryIntent {
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
			Source:         source,
			Reason:         reason,
			Persist:        true,
		})
	}
	return normalizeRoutePlanDeliveryIntents(out)
}

func routePlanFromManifest(evt events.Event, manifest deliveryRecipientManifest, source, reason string) RoutePlan {
	plan := newRoutePlan(evt)
	plan.AddLiveRecipients(routePlanLiveRecipientsFromManifest(manifest, source, reason)...)
	plan.AddDeliveryIntents(routePlanDeliveryIntentsFromRoutes(manifest.DeliveryRoutes, source, reason)...)
	plan.TargetFailure = manifest.TargetFailure
	return plan.Normalized()
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
		recipient.Source = strings.TrimSpace(recipient.Source)
		recipient.Reason = strings.TrimSpace(recipient.Reason)
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
			if out[idx].Source == "" {
				out[idx].Source = recipient.Source
			}
			if out[idx].Reason == "" {
				out[idx].Reason = recipient.Reason
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
		intent.Source = strings.TrimSpace(intent.Source)
		intent.Reason = strings.TrimSpace(intent.Reason)
		if intent.SubscriberType == "" || intent.SubscriberID == "" {
			continue
		}
		key := strings.Join([]string{intent.SubscriberType, intent.SubscriberID, intent.Target.FlowID, intent.Target.FlowInstance, intent.Target.EntityID}, "\x00")
		if idx, ok := indexByKey[key]; ok {
			out[idx].Persist = out[idx].Persist || intent.Persist
			if out[idx].Source == "" {
				out[idx].Source = intent.Source
			}
			if out[idx].Reason == "" {
				out[idx].Reason = intent.Reason
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
