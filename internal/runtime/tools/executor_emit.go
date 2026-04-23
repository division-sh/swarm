package tools

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/eventidentity"
)

func (e *Executor) handleEmitTool(ctx context.Context, actor models.AgentConfig, toolName string, input any) (any, error) {
	eventType, eventSchema, ok := e.emitRegistry.EventSchemaForActorTool(actor, toolName)
	if !ok {
		err := NewRuntimeError(
			"invalid_emit_tool_name",
			"tool-executor",
			"handle_emit_tool.resolve_event_type",
			false,
			"invalid emit tool name: %s",
			toolName,
		)
		e.logEmitToolOutcome(ctx, actor, toolName, "", "", nil, nil, events.Event{}, "invalid_emit_tool_name", "payload_shape", "resolve_event_type", err)
		return nil, err
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
		wrapped := WrapRuntimeError(
			"invalid_tool_input",
			"tool-executor",
			"handle_emit_tool.decode_input",
			false,
			err,
			"invalid emit tool input",
		)
		e.logEmitToolOutcome(ctx, actor, toolName, eventType, eventType, diagnosticPayloadMap(input), nil, events.Event{}, "payload_shape_failed", "payload_shape", "decode_input", wrapped)
		return nil, wrapped
	}
	if payloadMap == nil {
		payloadMap = map[string]any{}
	}
	preValidationPayload := diagnosticPayloadMap(payloadMap)
	schemaEventType := eventType
	eventType = e.resolveAgentScopedEmitEventType(actor, eventType)

	inbound, _ := runtimebus.InboundEventFromContext(ctx)
	payloadMap = e.enrichEmitPayloadContext(actor, inbound, schemaEventType, payloadMap)
	if err := rejectEmitEnvelopeFields(payloadMap); err != nil {
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, diagnosticPayloadMap(payloadMap), events.Event{}, "payload_shape_failed", "payload_shape", "envelope_field", err)
		return nil, err
	}
	postEnrichmentPayload := diagnosticPayloadMap(payloadMap)
	if err := ValidatePayloadAgainstSchema(eventSchema.Schema, payloadMap); err != nil {
		wrapped := WrapRuntimeError(
			"schema_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_schema",
			false,
			err,
			"emit payload schema validation failed",
		)
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.Event{}, "schema_validation_failed", "validation", "validate_schema", wrapped)
		return nil, wrapped
	}

	emitted := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: actor.ID,
		Payload:     mustJSON(payloadMap),
		CreatedAt:   time.Now(),
	})
	entityID := strings.TrimSpace(actor.EffectiveEntityID())
	if entityID == "" {
		entityID = strings.TrimSpace(inbound.EntityID())
	}
	if entityID != "" {
		emitted = emitted.WithEntityID(entityID)
	}
	flowInstance := strings.Trim(strings.TrimSpace(actor.CanonicalFlowPath()), "/")
	if flowInstance == "" {
		flowInstance = strings.Trim(strings.TrimSpace(inbound.FlowInstance()), "/")
	}
	if flowInstance != "" {
		emitted = emitted.WithFlowInstance(flowInstance)
	}
	emitted.TaskID = strings.TrimSpace(inbound.TaskID)
	if emitted.TaskID == "" {
		emitted.TaskID = strings.TrimSpace(asString(payloadMap["task_id"]))
	}
	if err := e.bus.Publish(ctx, emitted); err != nil {
		wrapped := WrapRuntimeError(
			"event_publish_failed",
			"tool-executor",
			"handle_emit_tool.publish",
			true,
			err,
			"failed to publish emitted event type=%s event_id=%s",
			eventType,
			emitted.ID,
		)
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, emitted, "event_publish_failed", "publish", "publish", wrapped)
		return nil, wrapped
	}
	e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, emitted, "published", "", "", nil)

	if rec, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && rec != nil {
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
	configured := UniqueNonEmpty(actor.EmitEvents)
	for _, candidate := range configured {
		if strings.Contains(candidate, "/") && eventidentity.LeafName(candidate) == eventType {
			return strings.TrimSpace(candidate)
		}
	}
	flowID := strings.TrimSpace(actor.Mode)
	if flowID == "" {
		return eventType
	}
	flowPath := actor.CanonicalFlowPath()
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
