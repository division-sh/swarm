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
	descriptors, _, err := eb.activeAgentDescriptors(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]runtimepinrouting.Descriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		descriptor = descriptor.Normalized()
		if descriptor.AgentID == "" {
			continue
		}
		out = append(out, runtimepinrouting.Descriptor{
			ID:           descriptor.AgentID,
			EntityID:     descriptor.EntityID,
			FlowInstance: descriptor.FlowInstance,
		})
	}
	return out, nil
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
	if eb == nil {
		return nil
	}
	eventType = strings.Trim(strings.TrimSpace(eventType), "/")
	if eventType == "" {
		return nil
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	out := make([]Subscriber, 0, 8)
	if table != nil {
		out = append(out, table.Resolve(eventType)...)
	}
	return dedupeSubscribers(out)
}

func (eb *EventBus) resolveRoutedRecipients(eventType string) []string {
	subscribers := eb.resolveRoutedSubscribers(eventType)
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
	targetsByRecipient := deliveryRouteTargetsByRecipient(deliveryRoutes)
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
		targets := targetsByRecipient[strings.TrimSpace(recipient.agentID)]
		if len(targets) == 0 {
			targets = []events.RouteIdentity{{}}
		}
		for _, target := range targets {
			deliverEvent := evt
			if !target.Empty() {
				deliverEvent = evt.WithDeliveryTarget(target)
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
				eb.logRuntime(ctx, "warn", "Event delivery to a recipient timed out", "eventbus", "delivery_timeout", evt.ID, string(evt.Type), recipient.agentID, evt.EntityID(), "", nil, map[string]any{
					"timeout_ms": int(deliverySendTimeout / time.Millisecond),
				}, "", 0)
			}
		}
	}
	if len(missing) > 0 || len(timedOut) > 0 {
		return eb.logAuthoritativeDeliveryIncomplete(ctx, evt, expected, delivered, missing, timedOut, nil)
	}
	return nil
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

func deliveryRouteTargetsByRecipient(deliveryRoutes []events.DeliveryRoute) map[string][]events.RouteIdentity {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 {
		return nil
	}
	out := make(map[string][]events.RouteIdentity, len(deliveryRoutes))
	for _, route := range deliveryRoutes {
		recipient := strings.TrimSpace(route.SubscriberID)
		if recipient == "" {
			continue
		}
		if route.Target.Empty() {
			continue
		}
		out[recipient] = append(out[recipient], route.Target.Normalized())
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

type agentRecipient struct {
	agentID string
	ch      chan events.Event
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
		out = append(out, agentRecipient{agentID: id, ch: ch})
	}
	return out
}

func (eb *EventBus) deliverByType(evt events.Event) {
	recipients := uniqueStrings(append(
		eb.resolveRoutedRecipients(string(evt.Type)),
		eb.resolveSubscribedRecipients(string(evt.Type))...,
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
	eb.logRuntime(ctx, "warn", "Event routing contradiction was detected", "eventbus", "contradiction", strings.TrimSpace(source.ID), strings.TrimSpace(string(source.Type)), "", strings.TrimSpace(source.EntityID()), "", nil, map[string]any{
		"reason": strings.TrimSpace(reason),
	}, "", 0)
	return nil
}

func (eb *EventBus) markPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
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
	if err := recorder.UpsertPipelineReceipt(ctx, eventID, status, errText); err != nil {
		eb.logRuntime(ctx, "error", "Persisting the pipeline receipt failed", "eventbus", "pipeline_receipt_persist_failed", eventID, "", "", "", "", nil, map[string]any{
			"status": status,
		}, err.Error(), 0)
		return err
	}
	if err := eb.ConvergeNormalRunCompletionForEvent(ctx, eventID); err != nil {
		eb.logRuntime(ctx, "error", "Persisting normal run completion failed", "eventbus", "normal_run_completion_failed", eventID, "", "", "", "", nil, nil, err.Error(), 0)
		return err
	}
	return nil
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
	errText := ""
	if cause != nil {
		errText = cause.Error()
		detail["cause"] = errText
	}
	eb.logRuntime(ctx, "warn", "Authoritative delivery fan-out was incomplete", "eventbus", "delivery_incomplete", evt.ID, string(evt.Type), "", evt.EntityID(), "", nil, detail, errText, 0)
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
	return fmt.Errorf("%w: %s", errAuthoritativeDeliveryIncomplete, strings.Join(parts, "; "))
}

func (eb *EventBus) logRuntime(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, errText string, durationUS int) error {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	logger := eb.logger
	eb.mu.RUnlock()
	if logger == nil {
		return nil
	}
	ctx = runtimecorrelation.WithRuntimeDiagnosticLineage(ctx, eventID, eventType)
	ctx = eb.withBundleFingerprint(ctx)
	if err := logger.Log(ctx, level, message, component, action, eventID, eventType, agentID, entityID, sessionID, correlation, detail, errText, durationUS); err != nil {
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
	return eb.logRuntime(ctx, entry.Level, entry.Message, entry.Component, entry.Action, entry.EventID, entry.EventType, entry.AgentID, entry.EffectiveEntityID(), entry.SessionID, entry.Correlation, entry.Detail, entry.Error, entry.DurationUS)
}
