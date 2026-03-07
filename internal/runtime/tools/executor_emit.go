package tools

import (
	"context"
	"strings"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
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

	inbound, _ := InboundEventFromContext(ctx)
	if err := e.trackTransitionPrerequisites(actor, inbound); err != nil {
		return nil, WrapRuntimeError(
			"emit_transition_prerequisite_failed",
			"tool-executor",
			"handle_emit_tool.track_prerequisites",
			false,
			err,
			"emit transition prerequisites failed",
		)
	}

	payloadMap = e.preNormalizeEmitPayload(actor, inbound, eventType, payloadMap)
	payloadMap = e.enrichEmitPayloadContext(actor, inbound, eventType, payloadMap)
	payloadMap = trimEmitPayloadToSchema(eventType, payloadMap)
	if err := ValidateEventPayloadAgainstSchema(eventType, payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"schema_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_schema_pre_normalize",
			false,
			err,
			"emit payload schema validation failed",
		)
	}

	payloadMap = e.normalizeEmitPayload(actor, inbound, eventType, payloadMap)
	payloadMap = trimEmitPayloadToSchema(eventType, payloadMap)
	if err := ValidateEventPayloadAgainstSchema(eventType, payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"schema_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_schema_post_normalize",
			false,
			err,
			"emit payload schema validation failed",
		)
	}
	if err := e.enforceMigrationGuardrail(ctx, actor, eventType, payloadMap); err != nil {
		return nil, err
	}

	emitted := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: actor.ID,
		TaskID:      strings.TrimSpace(asString(payloadMap["task_id"])),
		VerticalID:  strings.TrimSpace(asString(payloadMap["vertical_id"])),
		Payload:     mustJSON(payloadMap),
		CreatedAt:   time.Now(),
	}
	if emitted.VerticalID == "" {
		emitted.VerticalID = strings.TrimSpace(actor.VerticalID)
	}
	if emitted.TaskID == "" {
		emitted.TaskID = strings.TrimSpace(inbound.TaskID)
	}
	if emitted.VerticalID == "" {
		emitted.VerticalID = strings.TrimSpace(inbound.VerticalID)
	}

	if err := e.validateEmitTransition(actor, inbound, emitted); err != nil {
		return nil, WrapRuntimeError(
			"emit_transition_guardrail_violation",
			"tool-executor",
			"handle_emit_tool.validate_transition",
			false,
			err,
			"emit transition rejected by guardrail",
		)
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
