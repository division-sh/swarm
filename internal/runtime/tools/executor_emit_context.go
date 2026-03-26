package tools

import (
	"strings"

	"swarm/internal/events"
	models "swarm/internal/runtime/core/actors"
)

func (e *Executor) enrichEmitPayloadContext(actor models.AgentConfig, inbound events.Event, eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	out := make(map[string]any, len(payload)+2)
	for k, v := range payload {
		out[k] = v
	}
	if emitSchemaAllowsProperty(eventType, "task_id") && strings.TrimSpace(asString(out["task_id"])) == "" {
		out["task_id"] = strings.TrimSpace(inbound.TaskID)
	}
	entityID := actor.EffectiveEntityID()
	if entityID == "" {
		entityID = strings.TrimSpace(inbound.EntityID())
	}
	if emitSchemaAllowsProperty(eventType, "entity_id") && strings.TrimSpace(asString(out["entity_id"])) == "" {
		out["entity_id"] = entityID
	}
	return out
}

func emitSchemaAllowsProperty(eventType, property string) bool {
	eventType = strings.TrimSpace(eventType)
	property = strings.TrimSpace(property)
	if eventType == "" || property == "" {
		return false
	}
	schema := schemaForEventType(eventType).Schema
	props, ok := schema["properties"].(map[string]any)
	if !ok || len(props) == 0 {
		return false
	}
	_, ok = props[property]
	return ok
}

func trimEmitPayloadToSchema(eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return payload
	}
	schema := schemaForEventType(eventType).Schema
	if schemaAdditionalProps(schema["additionalProperties"]) {
		return payload
	}
	props := schemaProperties(schema["properties"])
	if len(props) == 0 {
		return payload
	}
	out := make(map[string]any, len(payload))
	for k, v := range payload {
		if _, ok := props[strings.TrimSpace(k)]; !ok {
			continue
		}
		out[k] = v
	}
	return out
}
