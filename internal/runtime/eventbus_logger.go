package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	runtimebus "empireai/internal/runtime/bus"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimetools "empireai/internal/runtime/tools"
)

func newRuntimeEventBus(store runtimebus.EventStore, logger *RuntimeLogger, interceptorProvider func() []runtimebus.EventInterceptor, payloadValidator runtimebus.PayloadValidator) (*runtimebus.EventBus, error) {
	var hook runtimebus.LoggerHook
	if logger != nil {
		hook = runtimeLoggerHook{logger: logger}
	}
	return runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Logger:              hook,
		InterceptorProvider: interceptorProvider,
		PayloadValidator:    payloadValidator,
	})
}

func newRuntimePayloadValidator(strict bool) runtimebus.PayloadValidator {
	return func(eventType string, payload []byte) error {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			return nil
		}
		schemas := runtimecontracts.EventSchemaRegistry()
		schema, ok := schemas[eventType]
		if !ok {
			return nil
		}
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		decoded := map[string]any{}
		if err := json.Unmarshal(payload, &decoded); err != nil {
			if strict {
				return err
			}
			slog.Warn("event payload validation skipped: invalid event payload JSON",
				"event_type", eventType,
				"error", err.Error(),
			)
			return nil
		}
		if err := runtimetools.ValidatePayloadAgainstSchema(schema.Schema, decoded); err != nil {
			if strict {
				return err
			}
			slog.Warn("event payload validation warning",
				"event_type", eventType,
				"error", err.Error(),
			)
		}
		return nil
	}
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
