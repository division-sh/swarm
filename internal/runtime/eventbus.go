package runtime

import (
	"context"
	"strings"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	runtimepipeline "empireai/internal/runtime/pipeline"
)

type EventInterceptor = runtimebus.EventInterceptor

var ErrStaleRuntimeEpoch = runtimebus.ErrStaleRuntimeEpoch

type EventBus struct {
	*runtimebus.EventBus
}

func NewEventBus(store runtimebus.EventStore) *EventBus {
	return &EventBus{EventBus: runtimebus.NewEventBus(store)}
}

func NewEventBusWithOptions(store runtimebus.EventStore, logger *RuntimeLogger, interceptorProvider func() []runtimebus.EventInterceptor) *EventBus {
	var hook runtimebus.LoggerHook
	if logger != nil {
		hook = runtimeLoggerHook{logger: logger}
	}
	return &EventBus{EventBus: runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Logger:              hook,
		InterceptorProvider: interceptorProvider,
	})}
}

func (eb *EventBus) SetRuntimeLogger(logger *RuntimeLogger) {
	if eb == nil || eb.EventBus == nil {
		return
	}
	if logger == nil {
		eb.EventBus.SetLoggerHook(nil)
		return
	}
	eb.EventBus.SetLoggerHook(runtimeLoggerHook{logger: logger})
}

func (eb *EventBus) Store() runtimebus.EventStore {
	if eb == nil || eb.EventBus == nil {
		return nil
	}
	return eb.EventBus.Store()
}

func (eb *EventBus) logRuntime(ctx context.Context, entry RuntimeLogEntry) {
	if eb == nil || eb.EventBus == nil {
		return
	}
	eb.EventBus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
		Level:      entry.Level,
		Component:  entry.Component,
		Action:     entry.Action,
		EventID:    entry.EventID,
		EventType:  entry.EventType,
		AgentID:    entry.AgentID,
		EntityID:   entry.EffectiveEntityID(),
		CampaignID: entry.CampaignID,
		ScanID:     entry.ScanID,
		SessionID:  entry.SessionID,
		Detail:     entry.Detail,
		Error:      entry.Error,
		DurationUS: entry.DurationUS,
	})
}

func (eb *EventBus) deliverByType(evt events.Event) {
	if eb == nil || eb.EventBus == nil {
		return
	}
	recipients := eb.ResolveSubscribedRecipients(string(evt.Type))
	eb.PublishDirect(context.Background(), evt, recipients)
}

func isValidEventTypeName(raw string) bool {
	return runtimebus.IsValidEventTypeName(raw)
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
		if v == "" || strings.HasSuffix(v, suffix) {
			continue
		}
		out = append(out, v)
	}
	return out
}

type runtimeLoggerHook struct {
	logger *RuntimeLogger
}

func (h runtimeLoggerHook) Log(ctx context.Context, level, component, action, eventID, eventType, agentID, verticalID, campaignID, scanID, sessionID string, detail any, errText string, durationUS int) {
	if h.logger == nil {
		return
	}
	h.logger.Log(ctx, RuntimeLogEntry{
		Level:      level,
		Component:  component,
		Action:     action,
		EventID:    eventID,
		EventType:  eventType,
		AgentID:    agentID,
		EntityID:   strings.TrimSpace(verticalID),
		CampaignID: campaignID,
		ScanID:     scanID,
		SessionID:  sessionID,
		Detail:     detail,
		Error:      errText,
		DurationUS: durationUS,
	})
}
