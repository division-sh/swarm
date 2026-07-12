package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

var errAuthoritativeDeliveryIncomplete = errors.New("authoritative delivery incomplete")

func (eb *EventBus) activeAgentDescriptors(ctx context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
	ephemeral := eb.runtimeActiveAgentDescriptors()
	lister, ok := eb.store.(ActiveAgentDescriptorLister)
	if !ok {
		if len(ephemeral) > 0 {
			return ephemeral, true, nil
		}
		return nil, false, nil
	}
	descriptors, err := lister.ListActiveAgentDescriptors(ctx)
	if err != nil {
		return nil, true, err
	}
	set := make(map[string]ActiveAgentDescriptor, len(descriptors)+len(ephemeral))
	for _, descriptor := range descriptors {
		descriptor = descriptor.Normalized()
		if descriptor.AgentID == "" {
			continue
		}
		set[descriptor.AgentID] = descriptor
	}
	for id, descriptor := range ephemeral {
		set[id] = descriptor
	}
	return set, true, nil
}

func (eb *EventBus) PinRoutingDescriptors(ctx context.Context) ([]runtimepinrouting.Descriptor, error) {
	descriptors, _, err := eb.activeTargetDescriptors(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]runtimepinrouting.Descriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		descriptor = descriptor.Normalized()
		if descriptor.FlowInstance == "" && descriptor.EntityID == "" {
			continue
		}
		out = append(out, runtimepinrouting.Descriptor{
			ID:            descriptor.ID,
			EntityID:      descriptor.EntityID,
			FlowInstance:  descriptor.FlowInstance,
			AddressFields: normalizeDescriptorAddressFields(descriptor.AddressFields),
		})
	}
	return out, nil
}

func (eb *EventBus) activeTargetDescriptors(ctx context.Context) ([]ActiveTargetDescriptor, bool, error) {
	agentDescriptors, agentsOK, err := eb.activeAgentDescriptors(ctx)
	if err != nil {
		return nil, true, err
	}
	out := activeTargetDescriptorsFromAgents(agentDescriptors)
	lister, flowOK := eb.store.(ActiveFlowInstanceDescriptorLister)
	if !flowOK {
		return out, agentsOK || len(out) > 0, nil
	}
	flowDescriptors, err := lister.ListActiveFlowInstanceDescriptors(ctx)
	if err != nil {
		return nil, true, err
	}
	out = appendActiveFlowInstanceTargetDescriptors(out, flowDescriptors)
	return out, true, nil
}

func activeTargetDescriptorsFromAgents(descriptors map[string]ActiveAgentDescriptor) []ActiveTargetDescriptor {
	if len(descriptors) == 0 {
		return nil
	}
	out := make([]ActiveTargetDescriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		out = appendActiveTargetDescriptor(out, descriptor.TargetDescriptor())
	}
	return out
}

func appendActiveFlowInstanceTargetDescriptors(out []ActiveTargetDescriptor, descriptors []ActiveFlowInstanceDescriptor) []ActiveTargetDescriptor {
	if len(descriptors) == 0 {
		return out
	}
	for _, descriptor := range descriptors {
		out = appendActiveTargetDescriptor(out, descriptor.TargetDescriptor())
	}
	return out
}

func appendActiveTargetDescriptor(out []ActiveTargetDescriptor, descriptor ActiveTargetDescriptor) []ActiveTargetDescriptor {
	descriptor = descriptor.Normalized()
	if descriptor.FlowInstance == "" && descriptor.EntityID == "" {
		return out
	}
	for _, existing := range out {
		existing = existing.Normalized()
		if existing.ID == descriptor.ID && existing.EntityID == descriptor.EntityID && existing.FlowInstance == descriptor.FlowInstance {
			return out
		}
	}
	return append(out, descriptor)
}

// RegisterRuntimeActiveAgentDescriptor adds in-memory active-agent metadata for
// handlers that are intentionally not persisted as ordinary current-runtime
// agents. Delivery planning still uses the normal authoritative recipient
// policy; this only supplies runtime-local descriptor evidence to that policy.
func (eb *EventBus) RegisterRuntimeActiveAgentDescriptor(descriptor ActiveAgentDescriptor) {
	if eb == nil {
		return
	}
	descriptor = descriptor.Normalized()
	if descriptor.AgentID == "" {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if eb.runtimeAgentDescriptors == nil {
		eb.runtimeAgentDescriptors = make(map[string]ActiveAgentDescriptor)
	}
	eb.runtimeAgentDescriptors[descriptor.AgentID] = descriptor
}

func (eb *EventBus) runtimeActiveAgentDescriptors() map[string]ActiveAgentDescriptor {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	if len(eb.runtimeAgentDescriptors) == 0 {
		return nil
	}
	out := make(map[string]ActiveAgentDescriptor, len(eb.runtimeAgentDescriptors))
	for id, descriptor := range eb.runtimeAgentDescriptors {
		out[id] = descriptor
	}
	return out
}

func filterRecipientsForExplicitAgentScope(evt events.Event, recipients []string, descriptors map[string]ActiveAgentDescriptor) []string {
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return nil
	}
	if len(descriptors) == 0 {
		return nil
	}
	eventEntityID := strings.TrimSpace(evt.EntityID())
	out := make([]string, 0, len(recipients))
	for _, r := range recipients {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		descriptor, ok := descriptors[r]
		if !ok {
			continue
		}
		if descriptor.EntityID != "" {
			if eventEntityID == "" || descriptor.EntityID != eventEntityID {
				continue
			}
		}
		out = append(out, r)
	}
	return out
}

func (eb *EventBus) resolveRoutedSubscribers(eventType string) []Subscriber {
	return eb.resolveRoutedSubscribersForEvent(events.NewRouteProbeEvent(events.EventType(eventType)))
}

func (eb *EventBus) resolveRoutedSubscribersForEvent(evt events.Event) []Subscriber {
	if eb == nil {
		return nil
	}
	eventKeys := routedEventKeysForPlan(evt)
	if len(eventKeys) == 0 {
		return nil
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	out := make([]Subscriber, 0, 8)
	if table != nil {
		for _, eventType := range eventKeys {
			out = append(out, table.Resolve(eventType)...)
		}
	}
	return dedupeSubscribers(out)
}

func (eb *EventBus) resolveRoutedRecipients(eventType string) []string {
	return eb.resolveRoutedRecipientsForEvent(events.NewRouteProbeEvent(events.EventType(eventType)))
}

func (eb *EventBus) resolveRoutedRecipientsForEvent(evt events.Event) []string {
	subscribers := eb.resolveRoutedSubscribersForEvent(evt)
	if len(subscribers) == 0 {
		return nil
	}
	out := make([]string, 0, len(subscribers))
	seen := make(map[string]struct{}, len(subscribers))
	for _, subscriber := range subscribers {
		subscriberID := strings.TrimSpace(subscriber.ID)
		if subscriberID == "" {
			continue
		}
		if _, exists := seen[subscriberID]; exists {
			continue
		}
		seen[subscriberID] = struct{}{}
		out = append(out, subscriberID)
	}
	return out
}

func (eb *EventBus) deliverToAgents(ctx context.Context, evt events.Event, agentIDs []string) error {
	return eb.deliverToAgentsWithTargets(ctx, evt, agentIDs, nil)
}

func (eb *EventBus) deliverToAgentsWithTargets(ctx context.Context, evt events.Event, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	return eb.deliverToRecipientsWithRoutes(ctx, evt, agentIDs, deliveryRoutesFromTargetMap(agentIDs, "agent", deliveryTargets))
}

func (eb *EventBus) deliverToRecipientsWithRoutes(ctx context.Context, evt events.Event, recipientIDs []string, deliveryRoutes []events.DeliveryRoute) error {
	expected := agentChannelDeliveryRecipients(recipientIDs, deliveryRoutes)
	dispatchRecipients := uniqueStrings(append(append([]string(nil), recipientIDs...), expected...))
	if len(dispatchRecipients) == 0 {
		return nil
	}
	expectedSet := make(map[string]struct{}, len(expected))
	for _, recipient := range expected {
		expectedSet[recipient] = struct{}{}
	}
	routesByRecipient := deliveryRoutesBySubscriber(deliveryRoutes)
	recipients := eb.snapshotRecipientChans(dispatchRecipients)
	delivered := make([]string, 0, len(recipients))
	seen := make(map[string]struct{}, len(recipients))
	for _, recipient := range recipients {
		seen[recipient.agentID] = struct{}{}
	}
	missing := make([]string, 0, len(expected))
	for _, recipient := range expected {
		if _, ok := seen[recipient]; !ok {
			missing = append(missing, recipient)
		}
	}
	timedOut := make([]string, 0, len(recipients))
	for _, recipient := range recipients {
		routes := routesByRecipient[recipient.deliveryRouteTargetKey()]
		if recipient.isWorkflowRuntimeInternalCarrier() {
			// The workflow-runtime subscription is an in-memory carrier for the
			// concrete node delivery routes. Its placeholder route must never
			// hide the target or route-scoped context owned by those routes.
			if nodeRoutes := workflowRuntimeInternalCarrierRoutes(deliveryRoutes); len(nodeRoutes) > 0 {
				routes = nodeRoutes
			}
		}
		if len(routes) == 0 {
			routes = []events.DeliveryRoute{{}}
		}
		for _, route := range routes {
			target := route.Target.Normalized()
			deliverEvent := evt.WithDeliveryContext(route.Context)
			if !target.Empty() {
				deliverEvent = events.NewProjectionEvent(
					evt.ID(),
					evt.Type(),
					evt.SourceAgent(),
					evt.TaskID(),
					evt.Payload(),
					evt.ChainDepth(),
					evt.RunID(),
					evt.ParentEventID(),
					events.EnvelopeForTargetRoute(evt.NormalizedEnvelope(), target),
					evt.CreatedAt(),
				).WithDeliveryContext(route.Context)
			}
			select {
			case recipient.ch <- deliverEvent:
				delivered = append(delivered, recipient.agentID)
			case <-ctx.Done():
				remaining := make([]string, 0, len(recipients)-len(delivered))
				for _, candidate := range recipients {
					if _, ok := seen[candidate.agentID]; ok {
						delete(seen, candidate.agentID)
					}
				}
				for _, recipient := range expected {
					found := false
					for _, sent := range delivered {
						if sent == recipient {
							found = true
							break
						}
					}
					if !found {
						remaining = append(remaining, recipient)
					}
				}
				return eb.logAuthoritativeDeliveryIncomplete(ctx, evt, expected, delivered, missing, remaining, ctx.Err())
			case <-time.After(deliverySendTimeout):
				if _, required := expectedSet[recipient.agentID]; required {
					timedOut = append(timedOut, recipient.agentID)
				}
				eb.logRuntime(ctx, "warn", "Event delivery to a recipient timed out", "eventbus", "delivery_timeout", evt.ID(), string(evt.Type()), recipient.agentID, evt.EntityID(), "", nil, map[string]any{
					"timeout_ms": int(deliverySendTimeout / time.Millisecond),
				}, targetDeliveryTimeoutFailure(evt, recipient.agentID, deliverySendTimeout), 0)
			}
		}
	}
	if len(missing) > 0 || len(timedOut) > 0 {
		return eb.logAuthoritativeDeliveryIncomplete(ctx, evt, expected, delivered, missing, timedOut, nil)
	}
	return nil
}

func deliveryRoutesBySubscriber(deliveryRoutes []events.DeliveryRoute) map[deliveryRouteTargetKey][]events.DeliveryRoute {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 {
		return nil
	}
	out := make(map[deliveryRouteTargetKey][]events.DeliveryRoute, len(deliveryRoutes))
	for _, route := range deliveryRoutes {
		route = route.Normalized()
		if route.SubscriberType == "" || route.SubscriberID == "" {
			continue
		}
		key := deliveryRouteTargetKey{subscriberType: route.SubscriberType, subscriberID: route.SubscriberID}
		out[key] = append(out[key], route)
	}
	return out
}

func agentChannelDeliveryRecipients(recipientIDs []string, deliveryRoutes []events.DeliveryRoute) []string {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 {
		return uniqueStrings(recipientIDs)
	}
	return deliveryRouteRecipientIDsByType(deliveryRoutes, "agent")
}

func deliveryRoutesFromTargetMap(recipients []string, subscriberType string, deliveryTargets map[string]events.RouteIdentity) []events.DeliveryRoute {
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(recipients))
	for _, recipient := range recipients {
		out = append(out, events.DeliveryRoute{
			SubscriberType: subscriberType,
			SubscriberID:   recipient,
			Target:         deliveryTargets[strings.TrimSpace(recipient)],
		})
	}
	return events.NormalizeDeliveryRoutes(out)
}

type deliveryRouteTargetKey struct {
	subscriberType string
	subscriberID   string
}

func deliveryRouteTargetsBySubscriber(deliveryRoutes []events.DeliveryRoute) map[deliveryRouteTargetKey][]events.RouteIdentity {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 {
		return nil
	}
	out := make(map[deliveryRouteTargetKey][]events.RouteIdentity, len(deliveryRoutes))
	for _, route := range deliveryRoutes {
		subscriberType := strings.TrimSpace(route.SubscriberType)
		recipient := strings.TrimSpace(route.SubscriberID)
		if subscriberType == "" || recipient == "" {
			continue
		}
		if route.Target.Empty() {
			continue
		}
		key := deliveryRouteTargetKey{
			subscriberType: subscriberType,
			subscriberID:   recipient,
		}
		out[key] = append(out[key], route.Target.Normalized())
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

type agentRecipient struct {
	agentID string
	ch      chan events.Event
	kind    inMemorySubscriberKind
}

const workflowRuntimeInternalCarrierID = "workflow-runtime"

func (r agentRecipient) deliveryRouteTargetKey() deliveryRouteTargetKey {
	subscriberType := "agent"
	if r.kind == inMemorySubscriberInternal {
		subscriberType = "node"
	}
	return deliveryRouteTargetKey{
		subscriberType: subscriberType,
		subscriberID:   strings.TrimSpace(r.agentID),
	}
}

func (r agentRecipient) isWorkflowRuntimeInternalCarrier() bool {
	return r.kind == inMemorySubscriberInternal && strings.TrimSpace(r.agentID) == workflowRuntimeInternalCarrierID
}

func workflowRuntimeInternalCarrierTargets(deliveryRoutes []events.DeliveryRoute) []events.RouteIdentity {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 {
		return nil
	}
	out := make([]events.RouteIdentity, 0, len(deliveryRoutes))
	seen := map[string]struct{}{}
	for _, route := range deliveryRoutes {
		if strings.TrimSpace(route.SubscriberType) != "node" {
			continue
		}
		target := route.Target.Normalized()
		if target.Empty() {
			continue
		}
		key := strings.Join([]string{target.FlowID, target.FlowInstance, target.EntityID}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, target)
	}
	return out
}

func workflowRuntimeInternalCarrierRoutes(deliveryRoutes []events.DeliveryRoute) []events.DeliveryRoute {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(deliveryRoutes))
	for _, route := range deliveryRoutes {
		if strings.TrimSpace(route.SubscriberType) != "node" {
			continue
		}
		if strings.TrimSpace(route.SubscriberID) == workflowRuntimeInternalCarrierID {
			continue
		}
		out = append(out, route)
	}
	return events.NormalizeDeliveryRoutes(out)
}

func (eb *EventBus) snapshotRecipientChans(agentIDs []string) []agentRecipient {
	if eb == nil || len(agentIDs) == 0 {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	out := make([]agentRecipient, 0, len(agentIDs))
	for _, id := range agentIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		ch, ok := eb.agentChans[id]
		if !ok {
			continue
		}
		kind := eb.subscriptionKinds[id]
		if kind == "" {
			kind = inMemorySubscriberAgent
		}
		out = append(out, agentRecipient{agentID: id, ch: ch, kind: kind})
	}
	return out
}

func (eb *EventBus) deliverByType(evt events.Event) {
	recipients := uniqueStrings(append(
		eb.resolveRoutedRecipientsForEvent(evt),
		eb.resolveSubscribedRecipients(string(evt.Type()))...,
	))
	_ = eb.deliverToAgents(context.Background(), evt, recipients)
}

func (eb *EventBus) resolveSubscribedRecipients(eventType string) []string {
	return deliveryRecipientIDs(eb.resolveSubscribedRecipientsForPlanning(eventType))
}

func (eb *EventBus) resolveSubscribedRecipientsForPlanning(eventType string) []deliveryRecipientCandidate {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	recipients := make([]deliveryRecipientCandidate, 0, len(eb.subscriptions))
	for agentID, pats := range eb.subscriptions {
		for _, pat := range pats {
			if routeMatches(string(pat), eventType) {
				recipients = append(recipients, deliveryRecipientCandidate{
					ID:                agentID,
					PersistAsDelivery: eb.subscriptionKinds[agentID] != inMemorySubscriberInternal,
				})
				break
			}
		}
	}
	return normalizeDeliveryRecipientCandidates(recipients)
}

func (eb *EventBus) resolveInternalRecipientsForRoutedNodePlanning(evt events.Event, routed []Subscriber) []deliveryRecipientCandidate {
	if eb == nil {
		return nil
	}
	aliases := routedNodeInternalSubscriptionAliases(evt, routed)
	if len(aliases) == 0 {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	recipients := make([]deliveryRecipientCandidate, 0, len(eb.subscriptions))
	for subscriberID, pats := range eb.subscriptions {
		if eb.subscriptionKinds[subscriberID] != inMemorySubscriberInternal {
			continue
		}
		for _, pat := range pats {
			matched := false
			for _, alias := range aliases {
				if routeMatches(string(pat), alias) {
					matched = true
					break
				}
			}
			if matched {
				recipients = append(recipients, deliveryRecipientCandidate{
					ID:                subscriberID,
					PersistAsDelivery: false,
				})
				break
			}
		}
	}
	return normalizeDeliveryRecipientCandidates(recipients)
}

func (eb *EventBus) ResolveSubscribedRecipients(eventType string) []string {
	return eb.resolveSubscribedRecipients(eventType)
}

func routeMatches(pattern, eventType string) bool {
	return RouteMatches(pattern, eventType)
}

func isValidEventTypeName(raw string) bool {
	return IsValidEventTypeName(raw)
}

func uniqueStrings(in []string) []string {
	return UniqueStrings(in)
}

func dedupeSubscribers(in []Subscriber) []Subscriber {
	if len(in) == 0 {
		return nil
	}
	out := make([]Subscriber, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, subscriber := range in {
		key := strings.TrimSpace(subscriber.ID) + "|" + strings.TrimSpace(subscriber.Type) + "|" + strings.TrimSpace(subscriber.Path)
		if strings.TrimSpace(subscriber.ID) == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, subscriber)
	}
	return out
}

func ensurePublishEpoch(ctx context.Context) error {
	epoch, ok := RuntimeEpochFromContext(ctx)
	if !ok || epoch <= 0 {
		return nil
	}
	if !IsCurrentRuntimeEpoch(epoch) {
		return ErrStaleRuntimeEpoch
	}
	return nil
}

func filterOutAgentIDs(in []string, disallow []string) []string {
	return FilterOutAgentIDs(in, disallow)
}

func (eb *EventBus) emitContradiction(ctx context.Context, source events.Event, reason string) error {
	eb.logRuntime(ctx, "warn", "Event routing contradiction was detected", "eventbus", "contradiction", strings.TrimSpace(source.ID()), strings.TrimSpace(string(source.Type())), "", strings.TrimSpace(source.EntityID()), "", nil, map[string]any{
		"reason": strings.TrimSpace(reason),
	}, nil, 0)
	return nil
}

func (eb *EventBus) markPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	if eb == nil || eb.store == nil {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	recorder, ok := eb.store.(PipelineReceiptPersistence)
	if !ok {
		return errors.New("event bus store does not support pipeline receipt persistence")
	}
	if err := recorder.UpsertPipelineReceipt(ctx, eventID, status, failure); err != nil {
		canonical := eventBusDependencyFailure(err, "pipeline_receipt_persist_failed", "persist_pipeline_receipt")
		eb.logRuntime(ctx, "error", "Persisting the pipeline receipt failed", "eventbus", "pipeline_receipt_persist_failed", eventID, "", "", "", "", nil, map[string]any{
			"status": status,
		}, canonical, 0)
		return runtimefailures.FromEnvelope(*canonical)
	}
	if err := eb.ConvergeNormalRunCompletionForEvent(ctx, eventID); err != nil {
		canonical := eventBusDependencyFailure(err, "normal_run_completion_failed", "converge_run_completion")
		eb.logRuntime(ctx, "error", "Persisting normal run completion failed", "eventbus", "normal_run_completion_failed", eventID, "", "", "", "", nil, nil, canonical, 0)
		return runtimefailures.FromEnvelope(*canonical)
	}
	return nil
}

// SettleRecoveredPipelineEvent keeps startup and periodic recovery on the same
// receipt, run-convergence, and route-obligation completion owner.
func (eb *EventBus) SettleRecoveredPipelineEvent(ctx context.Context, evt events.Event) error {
	if err := eb.markPipelineReceipt(ctx, evt.ID(), "processed", nil); err != nil {
		return err
	}
	if evt.Type() == events.EventType("mailbox.card_decided") {
		return eb.completeDecisionRouteObligation(ctx, evt.ID())
	}
	return nil
}

// QuarantineRecoveredPipelineEvent atomically records a terminal pipeline
// receipt and removes a non-retryable decision route from the due queue.
func (eb *EventBus) QuarantineRecoveredPipelineEvent(ctx context.Context, evt events.Event, cause error) error {
	failure := runtimefailures.Normalize(cause, "eventbus", "quarantine_decision_route")
	obligations, ok := eb.store.(runtimepipeline.DecisionRouteObligationStore)
	if !ok || obligations == nil {
		return errors.New("decision route obligation store is required for quarantine")
	}
	if err := obligations.QuarantineDecisionRouteObligation(ctx, evt.ID(), time.Now().UTC(), &failure); err != nil {
		return err
	}
	eb.logRuntime(ctx, "error", "Decision route was quarantined after a non-retryable failure", "eventbus", "decision_route_quarantined", evt.ID(), string(evt.Type()), "", evt.EntityID(), "", nil, nil, &failure, 0)
	return nil
}

func (eb *EventBus) completeDecisionRouteObligation(ctx context.Context, eventID string) error {
	obligations, ok := eb.store.(runtimepipeline.DecisionRouteObligationStore)
	if !ok || obligations == nil {
		return nil
	}
	if err := obligations.CompleteDecisionRouteObligation(ctx, eventID, time.Now().UTC()); err != nil {
		canonical := eventBusDependencyFailure(err, "decision_route_obligation_complete_failed", "complete_decision_route_obligation")
		eb.logRuntime(ctx, "error", "Completing the decision route obligation failed", "eventbus", "decision_route_obligation_complete_failed", eventID, "", "", "", "", nil, nil, canonical, 0)
		return runtimefailures.FromEnvelope(*canonical)
	}
	return nil
}

func (eb *EventBus) deferDecisionRouteObligation(ctx context.Context, eventID string, cause error) error {
	obligations, ok := eb.store.(runtimepipeline.DecisionRouteObligationStore)
	if !ok || obligations == nil {
		return nil
	}
	failure := runtimefailures.Normalize(cause, "eventbus", "defer_decision_route")
	return obligations.DeferDecisionRouteObligation(ctx, eventID, time.Now().UTC().Add(runtimepipeline.DecisionRouteRetryDelay), &failure)
}

func (eb *EventBus) logAuthoritativeDeliveryIncomplete(ctx context.Context, evt events.Event, expected, delivered, missing, timedOut []string, cause error) error {
	detail := map[string]any{
		"expected_recipients":  expected,
		"delivered_recipients": delivered,
	}
	if len(missing) > 0 {
		detail["missing_recipients"] = missing
	}
	if len(timedOut) > 0 {
		detail["timed_out_recipients"] = timedOut
	}
	parts := make([]string, 0, 3)
	if len(missing) > 0 {
		parts = append(parts, "missing="+strings.Join(missing, ","))
	}
	if len(timedOut) > 0 {
		parts = append(parts, "timed_out="+strings.Join(timedOut, ","))
	}
	if cause != nil {
		parts = append(parts, cause.Error())
	}
	if len(parts) == 0 {
		parts = append(parts, "incomplete")
	}
	baseErr := fmt.Errorf("%w: %s", errAuthoritativeDeliveryIncomplete, strings.Join(parts, "; "))
	failureErr := runtimefailures.Wrap(runtimefailures.ClassTargetUnreachable, "authoritative_delivery_incomplete", "eventbus", "deliver_authoritative_recipients", detail, baseErr)
	failure := runtimefailures.Normalize(failureErr, "eventbus", "deliver_authoritative_recipients")
	eb.logRuntime(ctx, "warn", "Authoritative delivery fan-out was incomplete", "eventbus", "delivery_incomplete", evt.ID(), string(evt.Type()), "", evt.EntityID(), "", nil, detail, &failure, 0)
	return failureErr
}

func targetDeliveryTimeoutFailure(evt events.Event, recipient string, timeout time.Duration) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassTimeout, "delivery_timeout", "eventbus", "deliver_recipient", map[string]any{
		"event_id": evt.ID(), "event_type": string(evt.Type()), "recipient": strings.TrimSpace(recipient), "timeout_ms": int(timeout / time.Millisecond),
	}), "eventbus", "deliver_recipient")
	return &failure
}

func (eb *EventBus) logRuntime(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	logger := eb.logger
	eb.mu.RUnlock()
	if logger == nil {
		return nil
	}
	if failure != nil {
		if err := runtimefailures.ValidateEnvelope(*failure); err != nil {
			return fmt.Errorf("validate runtime log failure: %w", err)
		}
	}
	ctx = runtimecorrelation.WithRuntimeDiagnosticLineage(ctx, eventID, eventType)
	ctx = eb.withBundleFingerprint(ctx)
	if err := logger.Log(ctx, level, message, component, action, eventID, eventType, agentID, entityID, sessionID, correlation, detail, runtimefailures.CloneEnvelope(failure), durationUS); err != nil {
		diaglog.ProcessLog("error", "diagnostics", "runtime log persistence failed",
			"component", strings.TrimSpace(component),
			"action", strings.TrimSpace(action),
			"error", err.Error(),
		)
		return err
	}
	return nil
}

func (eb *EventBus) LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	logger := eb.logger
	eb.mu.RUnlock()
	if logger == nil {
		return nil
	}
	if entry.Failure != nil {
		if err := runtimefailures.ValidateEnvelope(*entry.Failure); err != nil {
			return fmt.Errorf("validate runtime log failure: %w", err)
		}
	}
	ctx = runtimecorrelation.WithRuntimeDiagnosticLineage(ctx, entry.EventID, entry.EventType)
	ctx = eb.withBundleFingerprint(ctx)
	return logger.Log(ctx, entry.Level, entry.Message, entry.Component, entry.Action, entry.EventID, entry.EventType, entry.AgentID, entry.EffectiveEntityID(), entry.SessionID, entry.Correlation, entry.Detail, runtimefailures.CloneEnvelope(entry.Failure), entry.DurationUS)
}
