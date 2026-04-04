package bus

import (
	"context"
	"strings"
	"time"

	"swarm/internal/events"
	runtimepipeline "swarm/internal/runtime/pipeline"
)

func (eb *EventBus) persistableRecipients(ctx context.Context, recipients []string) []string {
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return nil
	}
	lister, ok := eb.store.(ActiveAgentLister)
	if !ok {
		return recipients
	}
	ids, err := lister.ListActiveAgentIDs(ctx)
	if err != nil {
		return recipients
	}
	if len(ids) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			set[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(recipients))
	for _, r := range recipients {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if _, ok := set[r]; ok {
			out = append(out, r)
		}
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

func (eb *EventBus) deliverToAgents(ctx context.Context, evt events.Event, agentIDs []string) {
	recipients := eb.snapshotRecipientChans(agentIDs)
	for _, recipient := range recipients {
		select {
		case recipient.ch <- evt:
		case <-ctx.Done():
			return
		case <-time.After(deliverySendTimeout):
			eb.logRuntime(ctx, "warn", "Event delivery to a recipient timed out", "eventbus", "delivery_timeout", evt.ID, string(evt.Type), recipient.agentID, evt.EntityID(), "", nil, map[string]any{
				"timeout_ms": int(deliverySendTimeout / time.Millisecond),
			}, "", 0)
		}
	}
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
	eb.deliverToAgents(context.Background(), evt, recipients)
}

func (eb *EventBus) resolveSubscribedRecipients(eventType string) []string {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	recipients := make([]string, 0, len(eb.subscriptions))
	for agentID, pats := range eb.subscriptions {
		for _, pat := range pats {
			if routeMatches(string(pat), eventType) {
				recipients = append(recipients, agentID)
				break
			}
		}
	}
	return recipients
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

func filterOutEntityScopedAgentIDs(in []string, entityID string) []string {
	entityID = strings.TrimSpace(entityID)
	if len(in) == 0 || entityID == "" {
		return in
	}
	suffix := "-" + entityID
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if strings.HasSuffix(v, suffix) {
			continue
		}
		out = append(out, v)
	}
	return out
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

func (eb *EventBus) logRuntime(ctx context.Context, level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, errText string, durationUS int) {
	if eb == nil {
		return
	}
	eb.mu.RLock()
	logger := eb.logger
	eb.mu.RUnlock()
	if logger == nil {
		return
	}
	logger.Log(ctx, level, message, component, action, eventID, eventType, agentID, entityID, sessionID, correlation, detail, errText, durationUS)
}

func (eb *EventBus) LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) {
	eb.logRuntime(ctx, entry.Level, entry.Message, entry.Component, entry.Action, entry.EventID, entry.EventType, entry.AgentID, entry.EffectiveEntityID(), entry.SessionID, entry.Correlation, entry.Detail, entry.Error, entry.DurationUS)
}
