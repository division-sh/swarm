package runtime

import (
	"context"
	"encoding/json"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/diaglog"
	"swarm/internal/runtime/semanticview"
	runtimesharedjson "swarm/internal/runtime/sharedjson"
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

func newRuntimePayloadValidator(logger *RuntimeLogger, source semanticview.Source, schemas map[string]runtimecontracts.EventSchema) runtimebus.PayloadValidator {
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
		validationSchema := schemaForCanonicalEventValidation(schema.Schema, decoded, source, schemas)
		if err := runtimetools.ValidatePayloadAgainstSchema(validationSchema, payloadForCanonicalEventValidation(decoded, validationSchema)); err != nil {
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

func payloadForCanonicalEventValidation(payload map[string]any, schema map[string]any) map[string]any {
	if len(payload) == 0 || schema == nil {
		return payload
	}
	props := schemaPropertyNames(schema)
	projected := make(map[string]any, len(payload))
	for key, value := range payload {
		if _, declared := props[key]; !declared && isRuntimeOwnedCanonicalContextField(key, payload) {
			continue
		}
		projected[key] = value
	}
	return projected
}

func isRuntimeOwnedCanonicalContextField(key string, payload map[string]any) bool {
	switch strings.TrimSpace(key) {
	case "entity_id", "flow_instance", "trigger_event_type", "current_state", "task_id", "timer_handle":
		return true
	case "target":
		return strings.TrimSpace(asString(payload["trigger_event_type"])) != ""
	default:
		return false
	}
}

func schemaForCanonicalEventValidation(schema map[string]any, payload map[string]any, source semanticview.Source, schemas map[string]runtimecontracts.EventSchema) map[string]any {
	validationSchema := cloneValidationSchema(schema)
	allowed := schemaPropertiesForValidation(validationSchema)
	triggerEventType := strings.TrimSpace(asString(payload["trigger_event_type"]))
	if triggerEventType != "" {
		if triggerSchema, ok := schemas[triggerEventType]; ok {
			for key, prop := range runtimesharedjson.SchemaProperties(triggerSchema.Schema["properties"]) {
				if _, exists := allowed[key]; exists {
					continue
				}
				allowed[key] = cloneValidationSchema(prop)
			}
		}
	}
	if source != nil {
		for _, group := range source.WorkflowEntitySchema().Groups {
			for _, field := range group.Fields {
				name := strings.TrimSpace(field.Name)
				if name == "" {
					continue
				}
				if _, exists := allowed[name]; exists {
					continue
				}
				allowed[name] = entityFieldValidationSchema(field.Type)
			}
		}
	}
	validationSchema["properties"] = allowed
	return validationSchema
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

func schemaPropertiesForValidation(schema map[string]any) map[string]any {
	props := runtimesharedjson.SchemaProperties(schema["properties"])
	out := make(map[string]any, len(props))
	for key, prop := range props {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = cloneValidationSchema(prop)
	}
	return out
}

func cloneValidationSchema(schema map[string]any) map[string]any {
	if len(schema) == 0 {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(schema))
	for key, value := range schema {
		switch typed := value.(type) {
		case map[string]any:
			cloned[key] = cloneValidationSchema(typed)
		case []any:
			items := make([]any, len(typed))
			for i := range typed {
				items[i] = typed[i]
			}
			cloned[key] = items
		default:
			cloned[key] = value
		}
	}
	return cloned
}

func entityFieldValidationSchema(fieldType string) map[string]any {
	fieldType = strings.TrimSpace(fieldType)
	schema := map[string]any{}
	for _, base := range []string{"string", "integer", "number", "boolean", "object", "array"} {
		if fieldType == base || strings.HasPrefix(fieldType, base+" ") || strings.HasPrefix(fieldType, base+"(") {
			schema["type"] = base
			return schema
		}
	}
	return schema
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
