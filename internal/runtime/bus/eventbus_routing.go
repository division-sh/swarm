package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"swarm/internal/events"
	"swarm/internal/runtime/diaglog"
	runtimepipeline "swarm/internal/runtime/pipeline"
)

var errAuthoritativeDeliveryIncomplete = errors.New("authoritative delivery incomplete")

func (eb *EventBus) activeAgentDescriptors(ctx context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
	lister, ok := eb.store.(ActiveAgentDescriptorLister)
	if !ok {
		return nil, false, nil
	}
	descriptors, err := lister.ListActiveAgentDescriptors(ctx)
	if err != nil {
		return nil, true, err
	}
	set := make(map[string]ActiveAgentDescriptor, len(descriptors))
	for _, descriptor := range descriptors {
		descriptor = descriptor.Normalized()
		if descriptor.AgentID == "" {
			continue
		}
		set[descriptor.AgentID] = descriptor
	}
	return set, true, nil
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
	expected := uniqueStrings(agentIDs)
	if len(expected) == 0 {
		return nil
	}
	recipients := eb.snapshotRecipientChans(expected)
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
		select {
		case recipient.ch <- evt:
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
			timedOut = append(timedOut, recipient.agentID)
			eb.logRuntime(ctx, "warn", "Event delivery to a recipient timed out", "eventbus", "delivery_timeout", evt.ID, string(evt.Type), recipient.agentID, evt.EntityID(), "", nil, map[string]any{
				"timeout_ms": int(deliverySendTimeout / time.Millisecond),
			}, "", 0)
		}
	}
	if len(missing) > 0 || len(timedOut) > 0 {
		return eb.logAuthoritativeDeliveryIncomplete(ctx, evt, expected, delivered, missing, timedOut, nil)
	}
	return nil
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

func (eb *EventBus) markPipelineReceipt(ctx context.Context, eventID, status, errText string) {
	if eb == nil || eb.store == nil {
		return
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return
	}
	recorder, ok := eb.store.(PipelineReceiptPersistence)
	if !ok {
		return
	}
	if err := recorder.UpsertPipelineReceipt(ctx, eventID, status, errText); err != nil {
		eb.logRuntime(ctx, "error", "Persisting the pipeline receipt failed", "eventbus", "pipeline_receipt_persist_failed", eventID, "", "", "", "", nil, map[string]any{
			"status": status,
		}, err.Error(), 0)
	}
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
