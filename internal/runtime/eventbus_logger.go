package runtime

import (
	"context"
	"strings"

	runtimebus "empireai/internal/runtime/bus"
)

func newRuntimeEventBus(store runtimebus.EventStore, logger *RuntimeLogger, interceptorProvider func() []runtimebus.EventInterceptor) (*runtimebus.EventBus, error) {
	var hook runtimebus.LoggerHook
	if logger != nil {
		hook = runtimeLoggerHook{logger: logger}
	}
	return runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Logger:              hook,
		InterceptorProvider: interceptorProvider,
	})
}

type runtimeLoggerHook struct {
	logger *RuntimeLogger
}

func (h runtimeLoggerHook) Log(ctx context.Context, level, component, action, eventID, eventType, agentID, entityID, campaignID, scanID, sessionID string, detail any, errText string, durationUS int) {
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
		EntityID:   strings.TrimSpace(entityID),
		CampaignID: campaignID,
		ScanID:     scanID,
		SessionID:  sessionID,
		Detail:     detail,
		Error:      errText,
		DurationUS: durationUS,
	})
}
