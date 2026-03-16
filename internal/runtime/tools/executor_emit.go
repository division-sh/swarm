package tools

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"empireai/internal/events"
	models "empireai/internal/runtime/core/actors"
	"github.com/google/uuid"
)

func (e *Executor) handleEmitTool(ctx context.Context, actor models.AgentConfig, toolName string, input any) (any, error) {
	eventType, ok := eventTypeFromEmitToolName(toolName)
	if !ok {
		return nil, NewRuntimeError(
			"invalid_emit_tool_name",
			"tool-executor",
			"handle_emit_tool.resolve_event_type",
			false,
			"invalid emit tool name: %s",
			toolName,
		)
	}
	if !IsEmitToolAllowedForRole(actor.Role, toolName) {
		return nil, NewRuntimeError(
			"emit_tool_not_allowed",
			"tool-executor",
			"handle_emit_tool.authorize",
			false,
			"event type %q is not allowed for role %q",
			eventType,
			canonicalRuntimeRole(actor.Role),
		)
	}
	if e.bus == nil {
		return nil, NewRuntimeError(
			"dependency_unavailable",
			"tool-executor",
			"handle_emit_tool.publish",
			true,
			"event bus is not configured",
		)
	}

	payloadMap := map[string]any{}
	if err := decodeToolInput(input, &payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"invalid_tool_input",
			"tool-executor",
			"handle_emit_tool.decode_input",
			false,
			err,
			"invalid emit tool input",
		)
	}
	if payloadMap == nil {
		payloadMap = map[string]any{}
	}
	eventType = e.resolveAgentScopedEmitEventType(actor, eventType)

	inbound, _ := InboundEventFromContext(ctx)
	payloadMap = e.enrichEmitPayloadContext(actor, inbound, eventType, payloadMap)
	payloadMap = trimEmitPayloadToSchema(eventType, payloadMap)
	if err := ValidateEventPayloadAgainstSchema(eventType, payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"schema_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_schema",
			false,
			err,
			"emit payload schema validation failed",
		)
	}

	emitted := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: actor.ID,
		TaskID:      strings.TrimSpace(asString(payloadMap["task_id"])),
		Payload:     mustJSON(payloadMap),
		CreatedAt:   time.Now(),
	}).WithEntityID(strings.TrimSpace(asString(payloadMap["entity_id"])))
	if emitted.EntityID() == "" {
		emitted = emitted.WithEntityID(strings.TrimSpace(actor.EffectiveEntityID()))
	}
	if emitted.TaskID == "" {
		emitted.TaskID = strings.TrimSpace(inbound.TaskID)
	}
	if emitted.EntityID() == "" {
		emitted = emitted.WithEntityID(strings.TrimSpace(inbound.EntityID()))
	}
	if emitted.EntityID() != "" {
		if strings.TrimSpace(asString(payloadMap["entity_id"])) == "" {
			payloadMap["entity_id"] = emitted.EntityID()
		}
		emitted.Payload = mustJSON(payloadMap)
	}
	if err := e.bus.Publish(ctx, emitted); err != nil {
		return nil, WrapRuntimeError(
			"event_publish_failed",
			"tool-executor",
			"handle_emit_tool.publish",
			true,
			err,
			"failed to publish emitted event type=%s event_id=%s",
			eventType,
			emitted.ID,
		)
	}

	if rec, ok := EmittedEventsRecorderFromContext(ctx); ok && rec != nil {
		rec.Append(emitted)
	}
	return map[string]any{
		"status":     "published",
		"event_id":   emitted.ID,
		"event_type": eventType,
	}, nil
}

func (e *Executor) resolveAgentScopedEmitEventType(actor models.AgentConfig, eventType string) string {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" || strings.Contains(eventType, "/") {
		return eventType
	}
	flowID := strings.TrimSpace(actor.Mode)
	if flowID == "" {
		return eventType
	}
	flowPath := strings.TrimSpace(configString(actor.Config, "flow_path"))
	flowPath = strings.Trim(flowPath, "/")
	if flowPath == "" {
		return eventType
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	if source == nil {
		return eventType
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return eventType
	}
	for _, candidate := range append(append([]string{}, scope.OutputEvents...), scope.InputEvents...) {
		if strings.TrimSpace(candidate) == eventType {
			return strings.Trim(flowPath+"/"+eventType, "/")
		}
	}
	if _, ok := scope.Events[eventType]; ok {
		return strings.Trim(flowPath+"/"+eventType, "/")
	}
	return eventType
}

func configString(raw json.RawMessage, key string) string {
	key = strings.TrimSpace(key)
	if key == "" || len(raw) == 0 || !json.Valid(raw) {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(asString(payload[key]))
}
