package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/diaglog"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
)

func newRuntimeEventBus(store runtimebus.EventStore, logger *RuntimeLogger, source semanticview.Source, interceptorProvider func() []runtimebus.EventInterceptor, payloadValidator runtimebus.PayloadValidator) (*runtimebus.EventBus, error) {
	var hook runtimebus.LoggerHook
	if logger != nil {
		hook = runtimeLoggerHook{logger: logger}
	}
	return runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Logger:              hook,
		InterceptorProvider: interceptorProvider,
		ContractBundle:      source,
		PayloadValidator:    payloadValidator,
	})
}

func newRuntimePayloadValidator(strict bool, logger *RuntimeLogger, schemas map[string]runtimecontracts.EventSchema) runtimebus.PayloadValidator {
	return func(eventType string, payload []byte) error {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			return nil
		}
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
			if logger != nil {
				handleRuntimeLogPersistenceError("event-bus", "payload_validation_json_invalid", logger.Warn(context.Background(), "event-bus", "payload_validation_json_invalid", map[string]any{
					"event_type": eventType,
				}, err))
			}
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
			if logger != nil {
				handleRuntimeLogPersistenceError("event-bus", "payload_validation_warning", logger.Warn(context.Background(), "event-bus", "payload_validation_warning", map[string]any{
					"event_type": eventType,
				}, err))
			}
		}
		return nil
	}
}

type runtimeLoggerHook struct {
	logger *RuntimeLogger
}

func (h runtimeLoggerHook) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, errText string, durationUS int) error {
	if h.logger == nil {
		return nil
	}
	return h.logger.Log(ctx, RuntimeLogEntry{
		Level:       level,
		Message:     message,
		Component:   component,
		Action:      action,
		EventID:     eventID,
		EventType:   eventType,
		AgentID:     agentID,
		EntityID:    strings.TrimSpace(entityID),
		SessionID:   sessionID,
		Correlation: correlation,
		Detail:      detail,
		Error:       errText,
		DurationUS:  durationUS,
	})
}
