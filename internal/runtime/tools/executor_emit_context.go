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
	if e.emitSchemaAllowsProperty(eventType, "task_id") && strings.TrimSpace(asString(out["task_id"])) == "" {
		out["task_id"] = strings.TrimSpace(inbound.TaskID)
	}
	entityID := actor.EffectiveEntityID()
	if entityID == "" {
		entityID = strings.TrimSpace(inbound.EntityID())
	}
	if e.emitSchemaAllowsProperty(eventType, "entity_id") && strings.TrimSpace(asString(out["entity_id"])) == "" {
		out["entity_id"] = entityID
	}
	return out
}

func (e *Executor) emitSchemaAllowsProperty(eventType, property string) bool {
	eventType = strings.TrimSpace(eventType)
	property = strings.TrimSpace(property)
	if eventType == "" || property == "" {
		return false
	}
	if e == nil || e.emitRegistry == nil {
		return false
	}
	schema := e.emitRegistry.SchemaForEventType(eventType).Schema
	props, ok := schema["properties"].(map[string]any)
	if !ok || len(props) == 0 {
		return false
	}
	_, ok = props[property]
	return ok
}
