package runtime

import (
	"context"
	"encoding/json"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/diaglog"
	runtimeeventpayload "swarm/internal/runtime/eventpayload"
	"swarm/internal/runtime/semanticview"
	runtimesharedjson "swarm/internal/runtime/sharedjson"
	runtimetools "swarm/internal/runtime/tools"
)

func newRuntimeEventBus(store runtimebus.EventStore, logger *RuntimeLogger, source semanticview.Source, bundleFingerprint string, interceptorProvider func() []runtimebus.EventInterceptor, payloadValidator runtimebus.PayloadValidator) (*runtimebus.EventBus, error) {
	var hook runtimebus.LoggerHook
	if logger != nil {
		hook = runtimeLoggerHook{logger: logger}
	}
	return runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Logger:              hook,
		InterceptorProvider: interceptorProvider,
		ContractBundle:      source,
		PayloadValidator:    payloadValidator,
		BundleFingerprint:   bundleFingerprint,
	})
}

// newRuntimePayloadValidator owns canonical event-store admission validation.
// Supported emit surfaces validate producer-authored payloads before publish;
// this validator is the final pre-persistence guard for every event and the
// primary guard for ingress/direct/store paths without a producer-surface owner.
func newRuntimePayloadValidator(logger *RuntimeLogger, schemas map[string]runtimecontracts.EventSchema) runtimebus.PayloadValidator {
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
			if logger != nil {
				handleRuntimeLogPersistenceError("event-bus", "payload_validation_json_invalid", logger.Warn(context.Background(), "event-bus", "payload_validation_json_invalid", map[string]any{
					"event_type": eventType,
				}, err))
			}
			return err
		}
		if err := runtimetools.ValidatePayloadAgainstSchema(schema.Schema, payloadForCanonicalEventValidation(decoded, schema.Schema)); err != nil {
			if logger != nil {
				handleRuntimeLogPersistenceError("event-bus", "payload_validation_rejected", logger.Warn(context.Background(), "event-bus", "payload_validation_rejected", map[string]any{
					"event_type": eventType,
				}, err))
			}
			return err
		}
		return nil
	}
}

// payloadForCanonicalEventValidation validates only the event payload contract.
// Runtime-owned canonical context is envelope/admission metadata unless the
// target event schema explicitly declares the same field as payload.
func payloadForCanonicalEventValidation(payload map[string]any, schema map[string]any) map[string]any {
	if len(payload) == 0 || schema == nil {
		return payload
	}
	return runtimeeventpayload.StripUndeclaredRuntimeOwnedCanonicalContext(payload, schemaPropertyNames(schema))
}

func schemaPropertyNames(schema map[string]any) map[string]struct{} {
	props := runtimesharedjson.SchemaProperties(schema["properties"])
	out := make(map[string]struct{}, len(props))
	for key := range props {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = struct{}{}
	}
	return out
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
