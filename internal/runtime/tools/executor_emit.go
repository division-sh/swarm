package tools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type publishRecipientPlanner interface {
	CheckPublishRecipientPlan(context.Context, events.Event) (runtimebus.PublishRecipientPlan, error)
}

func (e *Executor) handleEmitTool(ctx context.Context, actor models.AgentConfig, toolName string, input any, outputIdentity ...llm.ToolOutputEventIdentity) (any, error) {
	if len(outputIdentity) > 1 {
		return nil, failures.New(failures.ClassSchemaInvalid, "tool_output_event_identity_ambiguous", "tool-executor", "handle_emit_tool.identity", nil)
	}
	if len(outputIdentity) == 1 {
		if err := outputIdentity[0].Validate(); err != nil {
			return nil, failures.Wrap(failures.ClassSchemaInvalid, "tool_output_event_identity_invalid", "tool-executor", "handle_emit_tool.identity", nil, err)
		}
	}
	eventType, eventSchema, ok := e.emitRegistry.EventSchemaForActorTool(actor, toolName)
	if !ok {
		err := failures.NewDetail(
			"invalid_emit_tool_name",
			"tool-executor",
			"handle_emit_tool.resolve_event_type",
			map[string]any{"tool": strings.TrimSpace(toolName)},
		)
		e.logEmitToolOutcome(ctx, actor, toolName, "", "", nil, nil, events.NoEvent(), "invalid_emit_tool_name", "payload_shape", "resolve_event_type", err)
		return nil, err
	}
	if e.bus == nil {
		return nil, failures.NewDetail(
			"dependency_unavailable",
			"tool-executor",
			"handle_emit_tool.publish",
			map[string]any{"dependency": "event_bus"},
		)
	}

	payloadMap := map[string]any{}
	if err := decodeToolInput(input, &payloadMap); err != nil {
		wrapped := failures.WrapDetail(
			"invalid_tool_input",
			"tool-executor",
			"handle_emit_tool.decode_input",
			nil,
			err,
		)
		e.logEmitToolOutcome(ctx, actor, toolName, eventType, eventType, diagnosticPayloadMap(input), nil, events.NoEvent(), "payload_shape_failed", "payload_shape", "decode_input", wrapped)
		return nil, wrapped
	}
	if payloadMap == nil {
		payloadMap = map[string]any{}
	}
	preValidationPayload := diagnosticPayloadMap(payloadMap)
	schemaEventType := eventType

	inbound, _ := runtimebus.InboundEventFromContext(ctx)
	executionMode := actor.ExecutionMode
	if !executionMode.Valid() {
		err := fmt.Errorf("emit tool requires typed execution mode for agent %s", strings.TrimSpace(actor.ID))
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, nil, events.NoEvent(), "execution_mode_missing", "execution_mode", "resolve_execution_mode", err)
		return nil, err
	}
	if activeMode, ok := runtimeeffects.ExecutionModeFromContext(ctx); ok && activeMode != executionMode {
		err := fmt.Errorf("emit tool execution mode %q conflicts with agent %s mode %q", activeMode, strings.TrimSpace(actor.ID), executionMode)
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, nil, events.NoEvent(), "execution_mode_conflict", "execution_mode", "resolve_execution_mode", err)
		return nil, err
	}
	emitLineage := events.LineageFromEvent(inbound)
	emitLineage.ExecutionMode = executionMode
	payloadMap = e.enrichEmitPayloadContext(actor, inbound, schemaEventType, payloadMap)
	if err := rejectEmitEnvelopeFields(payloadMap); err != nil {
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, diagnosticPayloadMap(payloadMap), events.NoEvent(), "payload_shape_failed", "payload_shape", "envelope_field", err)
		return nil, err
	}
	postEnrichmentPayload := diagnosticPayloadMap(payloadMap)
	if err := ValidatePayloadAgainstSchema(eventSchema.Schema, payloadMap); err != nil {
		wrapped := failures.WrapDetail(
			"schema_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_schema",
			map[string]any{"event": schemaEventType},
			err,
		)
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.NoEvent(), "schema_validation_failed", "validation", "validate_schema", wrapped)
		return nil, wrapped
	}
	if err := e.validateEmitCriteriaCitations(actor, schemaEventType, eventSchema, payloadMap); err != nil {
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.NoEvent(), "criteria_citation_validation_failed", "validation", "validate_criteria_citations", err)
		return nil, err
	}

	entityID := strings.TrimSpace(actor.EffectiveEntityID())
	projection, ok := semanticview.ResolveAgentContractProjection(e.workflowSource, actor)
	if !ok {
		return nil, fmt.Errorf("emit tool requires exact agent declaration")
	}
	flowID := projection.OwnerFlowID
	routingSource, err := e.agentExecutionRoutingSource(ctx, actor)
	if err != nil {
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.NoEvent(), "routing_source_invalid", "routing_source", "construct", err)
		return nil, err
	}
	flowInstance := routingSource.Route().FlowInstance
	publication, err := runtimepinrouting.AdmitPublicationIdentity(flowID, schemaEventType, routingSource)
	if err != nil {
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.NoEvent(), "publication_identity_invalid", "routing_source", "construct", err)
		return nil, err
	}
	eventType = string(publication)
	envelope := events.EventEnvelope{
		EntityID:     entityID,
		FlowInstance: flowInstance,
	}
	if !routingSource.Empty() {
		envelope = events.EnvelopeForSourceRoute(envelope, routingSource.Route())
	}
	taskID := asString(payloadMap["task_id"])
	var eventID string
	var createdAt time.Time
	if len(outputIdentity) == 1 {
		eventID = outputIdentity[0].EventID()
		createdAt = outputIdentity[0].CreatedAt()
	}
	if strings.TrimSpace(eventID) == "" {
		eventID = uuid.NewString()
	}
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	emitted, err := events.NewChildEvent(events.ChildEventInput{
		Facts: events.EventFacts{
			ID: eventID, Type: events.EventType(eventType), Producer: events.ProducerClaim{Type: events.EventProducerAgent, ID: actor.ID},
			TaskID: taskID, Payload: mustJSON(payloadMap), Envelope: envelope, RoutingSource: routingSource,
			CreatedAt: createdAt, ExecutionMode: executionMode,
		},
		Lineage: emitLineage,
	})
	if err != nil {
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.NoEvent(), "event_construction_failed", "construction", "construct", err)
		return nil, err
	}
	if runtimepinrouting.PinDeclaredOutput(e.workflowSource, flowID, eventType) {
		resolution := runtimepinrouting.ResolveEnvelope(runtimepinrouting.ResolutionInput{
			Source: e.workflowSource, FlowID: flowID, EventType: eventType, RoutingSource: routingSource,
		}, envelope)
		if !resolution.Failure.Empty() {
			wrapped := failures.NewTarget(resolution.Failure.Code(), "tool-executor", "handle_emit_tool.pin_target_resolution",
				map[string]any{"tool": strings.TrimSpace(toolName), "event": eventType})
			e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.SomeEvent(emitted), "pin_target_resolution_failed", "publish", "pin_target_resolution", wrapped)
			return nil, wrapped
		}
		emitted, err = events.ResolveEnvelope(emitted, resolution.Envelope)
		if err != nil {
			return nil, err
		}
		if planner, ok := e.bus.(publishRecipientPlanner); ok && planner != nil {
			plan, err := planner.CheckPublishRecipientPlan(ctx, emitted)
			if err != nil {
				wrapped := failures.WrapDetail(
					"route_plan_preflight_failed",
					"tool-executor",
					"handle_emit_tool.route_plan_preflight",
					map[string]any{"event": eventType},
					err,
				)
				e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.SomeEvent(emitted), "route_plan_preflight_failed", "publish", "route_plan_preflight", wrapped)
				return nil, wrapped
			}
			if plan.UsesCanonicalRouteAuthority() {
				if plan.TargetFailure != "" {
					wrapped := failures.NewTarget(
						plan.TargetFailure,
						"tool-executor",
						"handle_emit_tool.route_plan_preflight",
						map[string]any{"tool": strings.TrimSpace(toolName), "event": eventType},
					)
					e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.SomeEvent(emitted), "route_plan_preflight_failed", "publish", "route_plan_preflight", wrapped)
					return nil, wrapped
				}
			}
		}
	}
	agentIdentity, err := actor.ConcreteIdentity()
	if err != nil {
		return nil, err
	}
	instance := flowidentity.RunScopedFlowInstance{RunID: agentIdentity.RunID, Route: flowidentity.Route{
		ScopeKey: agentIdentity.Route.ScopeKey, InstanceID: agentIdentity.Route.InstanceID, InstancePath: agentIdentity.Route.InstancePath,
	}}
	if agentIdentity.Route.Presence == agentidentity.RouteRoot {
		instance.Route = flowidentity.Route{ScopeKey: ".", InstanceID: instance.RunID, InstancePath: instance.RunID}
	}
	emitResult, publishErr := e.bus.PublishEmit(ctx, emitted, pipeline.WorkflowPublicationStageRequest{Instance: instance, EntityID: entityID})
	if err := emitResult.Validate(); err != nil {
		return nil, err
	}
	if publishErr == nil && (!emitResult.Accepted || !emitResult.FeedbackCommitted) {
		return nil, fmt.Errorf("emit did not acknowledge its publication and feedback")
	}
	output := map[string]any{"status": "published", "event_id": emitted.ID(), "event_type": eventType,
		"accepted": emitResult.Accepted, "feedback_committed": emitResult.FeedbackCommitted, "replayed_result": emitResult.Replay}
	if emitResult.Accepted {
		output["retry_write"] = false
		if rec, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && rec != nil {
			rec.Append(emitted)
		}
	}
	if emitResult.FeedbackCommitted {
		if err := emitResult.Feedback.Validate(); err != nil {
			return output, err
		}
		stage := emitResult.Feedback.Receipt.Stage()
		if emitResult.Feedback.Receipt.EventID() != emitted.ID() || stage.Instance != instance || entityID != "" && stage.EntityID != entityID {
			return output, fmt.Errorf("emit feedback contradicts its exact author occurrence")
		}
		output["stage"], output["stage_defined"], output["revision"] = stage.Stage, stage.StageDefined, stage.Revision
		output["stage_origin"], output["dispatch"] = emitResult.Feedback.StageOrigin, emitResult.Feedback.Dispatch
	}
	if publishErr != nil {
		output["status"] = "publish_failed"
		if emitResult.Accepted {
			output["status"] = "accepted"
		}
		wrapped := failures.WrapDetail(
			"event_publish_failed",
			"tool-executor",
			"handle_emit_tool.publish",
			output,
			publishErr,
		)
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.SomeEvent(emitted), "event_publish_failed", "publish", "publish", wrapped)
		return output, wrapped
	}
	e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.SomeEvent(emitted), "published", "", "", nil)

	return output, nil
}
