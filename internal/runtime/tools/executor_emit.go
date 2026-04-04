package tools

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/eventidentity"
	runtimepipeline "swarm/internal/runtime/pipeline"
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
	schemaEventType := eventType
	eventType = e.resolveAgentScopedEmitEventType(actor, eventType)

	inbound, _ := InboundEventFromContext(ctx)
	payloadMap = e.enrichEmitPayloadContext(actor, inbound, schemaEventType, payloadMap)
	payloadMap = trimEmitPayloadToSchema(schemaEventType, payloadMap)
	if err := ValidateEventPayloadAgainstSchema(schemaEventType, payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"schema_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_schema",
			false,
			err,
			"emit payload schema validation failed",
		)
	}

	entityIDSource := "payload"
	if strings.TrimSpace(asString(payloadMap["entity_id"])) == "" {
		switch {
		case strings.TrimSpace(actor.EffectiveEntityID()) != "":
			entityIDSource = "actor"
		case strings.TrimSpace(inbound.EntityID()) != "":
			entityIDSource = "inbound_event"
		default:
			entityIDSource = "none"
		}
	}
	taskIDSource := "payload"
	if strings.TrimSpace(asString(payloadMap["task_id"])) == "" {
		if strings.TrimSpace(inbound.TaskID) != "" {
			taskIDSource = "inbound_event"
		} else {
			taskIDSource = "none"
		}
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
	e.logEmitTargetResolution(ctx, actor, toolName, schemaEventType, eventType, emitted, entityIDSource, taskIDSource)
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

func (e *Executor) logEmitTargetResolution(ctx context.Context, actor models.AgentConfig, toolName, requestedEventType, resolvedEventType string, emitted events.Event, entityIDSource, taskIDSource string) {
	if e == nil || e.bus == nil {
		return
	}
	logger, ok := e.bus.(runtimeToolLogSink)
	if !ok || logger == nil {
		return
	}
	logger.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
		Level:     "info",
		Message:   "Emit tool target was resolved",
		Component: "tool-executor",
		Action:    "emit_target_resolved",
		AgentID:   strings.TrimSpace(actor.ID),
		EntityID:  strings.TrimSpace(actor.EffectiveEntityID()),
		Detail: map[string]any{
			"tool_name":             strings.TrimSpace(toolName),
			"requested_event_type":  strings.TrimSpace(requestedEventType),
			"resolved_event_type":   strings.TrimSpace(resolvedEventType),
			"emitted_event_id":      strings.TrimSpace(emitted.ID),
			"emitted_event_type":    strings.TrimSpace(string(emitted.Type)),
			"emitted_entity_id":     strings.TrimSpace(emitted.EntityID()),
			"emitted_task_id":       strings.TrimSpace(emitted.TaskID),
			"entity_id_source":      strings.TrimSpace(entityIDSource),
			"task_id_source":        strings.TrimSpace(taskIDSource),
			"actor_entity_id":       strings.TrimSpace(actor.EffectiveEntityID()),
			"inbound_parent_event":  strings.TrimSpace(emitted.ParentEventID),
			"context_entity_target": strings.TrimSpace(emitted.EntityID()),
		},
	})
}

func (e *Executor) resolveAgentScopedEmitEventType(actor models.AgentConfig, eventType string) string {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" || strings.Contains(eventType, "/") {
		return eventType
	}
	configured := configuredEmitEvents(actor.Config)
	for _, candidate := range configured {
		if strings.Contains(candidate, "/") && eventidentity.LeafName(candidate) == eventType {
			return strings.TrimSpace(candidate)
		}
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
	localEvents := make([]string, 0, len(scope.OutputEvents)+len(scope.Events))
	localEvents = append(localEvents, scope.OutputEvents...)
	for candidate := range scope.Events {
		localEvents = append(localEvents, candidate)
	}
	return eventidentity.ExternalizeForFlow(flowPath, localEvents, eventType)
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
