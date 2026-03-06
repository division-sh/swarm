package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	"github.com/google/uuid"
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

func (eb *EventBus) isFactoryEvent(eventType events.EventType) bool {
	name := strings.TrimSpace(string(eventType))
	if name == "" {
		return false
	}
	for _, prefix := range factoryEventPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func (eb *EventBus) resolveOpCoRecipients(evt events.Event) []string {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	table := eb.routingTable[evt.VerticalID]
	if table == nil {
		return nil
	}

	set := make(map[string]struct{})
	eventName := string(evt.Type)
	for _, r := range table.Routes {
		if r.Status != "" && r.Status != "active" {
			continue
		}
		if routeMatches(r.EventPattern, eventName) {
			set[r.SubscriberID] = struct{}{}
		}
	}

	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
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
			eb.logRuntime(ctx, RuntimeLogEntry{
				Level:      "warn",
				Component:  "eventbus",
				Action:     "delivery_timeout",
				EventID:    evt.ID,
				EventType:  string(evt.Type),
				AgentID:    recipient.agentID,
				VerticalID: evt.VerticalID,
				Detail: map[string]any{
					"timeout_ms": int(deliverySendTimeout / time.Millisecond),
				},
			})
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
	recipients := eb.resolveSubscribedRecipients(string(evt.Type))
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

func routeMatches(pattern, eventType string) bool {
	return runtimebus.RouteMatches(pattern, eventType)
}

func appendUniqueEventType(in []events.EventType, v events.EventType) []events.EventType {
	return runtimebus.AppendUniqueEventType(in, v)
}

func isValidEventTypeName(raw string) bool {
	return runtimebus.IsValidEventTypeName(raw)
}

func (eb *EventBus) resolveHumanTaskRecipients(evt events.Event) []string {
	if len(evt.Payload) == 0 {
		return nil
	}
	var payload struct {
		RequestingAgent string `json:"requesting_agent"`
	}
	_ = json.Unmarshal(evt.Payload, &payload)
	agentID := strings.TrimSpace(payload.RequestingAgent)
	if agentID == "" {
		return nil
	}
	return []string{agentID}
}

func uniqueStrings(in []string) []string {
	return runtimebus.UniqueStrings(in)
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
	return runtimebus.FilterOutAgentIDs(in, disallow)
}

func filterOutVerticalScopedAgentIDs(in []string, verticalID string) []string {
	verticalID = strings.TrimSpace(verticalID)
	if len(in) == 0 || verticalID == "" {
		return in
	}
	suffix := "-" + verticalID
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
	payload := []byte(fmt.Sprintf(`{"event_id":"%s","reason":"%s","source_type":"%s"}`,
		source.ID, reason, source.Type))
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.contradiction_detected"),
		SourceAgent: "runtime",
		TaskID:      source.TaskID,
		VerticalID:  source.VerticalID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	if err := eb.store.AppendEvent(ctx, evt); err != nil {
		return fmt.Errorf("persist contradiction event: %w", err)
	}
	eb.logRuntime(ctx, RuntimeLogEntry{
		Level:      "warn",
		Component:  "guardrails",
		Action:     "violation",
		EventID:    source.ID,
		EventType:  string(source.Type),
		VerticalID: source.VerticalID,
		Detail: map[string]any{
			"reason": reason,
		},
	})
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
	_ = recorder.UpsertPipelineReceipt(ctx, eventID, status, errText)
}

func (eb *EventBus) logRuntime(ctx context.Context, entry RuntimeLogEntry) {
	if eb == nil {
		return
	}
	eb.mu.RLock()
	logger := eb.logger
	eb.mu.RUnlock()
	if logger == nil {
		return
	}
	logger.Log(ctx, entry)
}
