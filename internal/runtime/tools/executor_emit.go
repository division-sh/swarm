package tools

import (
	"context"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type publishRecipientPlanner interface {
	CheckPublishRecipientPlan(context.Context, events.Event) (runtimebus.PublishRecipientPlan, error)
}

func (e *Executor) handleEmitTool(ctx context.Context, actor models.AgentConfig, toolName string, input any) (any, error) {
	eventType, eventSchema, ok := e.emitRegistry.EventSchemaForActorTool(actor, toolName)
	if !ok {
		err := failures.NewDetail(
			"invalid_emit_tool_name",
			"tool-executor",
			"handle_emit_tool.resolve_event_type",
			map[string]any{"tool": strings.TrimSpace(toolName)},
		)
		e.logEmitToolOutcome(ctx, actor, toolName, "", "", nil, nil, events.EmptyEvent(), "invalid_emit_tool_name", "payload_shape", "resolve_event_type", err)
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
		e.logEmitToolOutcome(ctx, actor, toolName, eventType, eventType, diagnosticPayloadMap(input), nil, events.EmptyEvent(), "payload_shape_failed", "payload_shape", "decode_input", wrapped)
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
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, diagnosticPayloadMap(payloadMap), events.EmptyEvent(), "payload_shape_failed", "payload_shape", "envelope_field", err)
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
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.EmptyEvent(), "schema_validation_failed", "validation", "validate_schema", wrapped)
		return nil, wrapped
	}
	if err := e.validateEmitCriteriaCitations(actor, schemaEventType, eventSchema, payloadMap); err != nil {
		e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, events.EmptyEvent(), "criteria_citation_validation_failed", "validation", "validate_criteria_citations", err)
		return nil, err
	}

	entityID := strings.TrimSpace(actor.EffectiveEntityID())
	if entityID == "" {
		entityID = strings.TrimSpace(inbound.EntityID())
	}
	flowInstance := emitFlowInstanceForActorEvent(actor, inbound)
	flowID := emitActorFlowID(e.workflowSource, actor, flowInstance)
	sourceRoute := events.RouteIdentity{
		FlowID:       flowID,
		FlowInstance: flowInstance,
		EntityID:     entityID,
	}.Normalized()
	envelope := events.EventEnvelope{
		EntityID:     entityID,
		FlowInstance: flowInstance,
	}
	if !sourceRoute.Empty() {
		envelope = events.EnvelopeForSourceRoute(envelope, sourceRoute)
	}
	taskID := asString(payloadMap["task_id"])
	emitted := events.NewChildEvent(
		uuid.NewString(),
		events.EventType(eventType),
		actor.ID,
		taskID,
		mustJSON(payloadMap),
		0,
		inbound,
		envelope,
		time.Now(),
	)
	if runtimepinrouting.PinDeclaredOutput(e.workflowSource, flowID, eventType) {
		usePublishAuthority := false
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
				e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, emitted, "route_plan_preflight_failed", "publish", "route_plan_preflight", wrapped)
				return nil, wrapped
			}
			if plan.UsesCanonicalRouteAuthority() {
				if plan.TargetFailure != "" {
					wrapped := failures.NewTarget(
						string(plan.TargetFailure),
						"tool-executor",
						"handle_emit_tool.route_plan_preflight",
						map[string]any{"tool": strings.TrimSpace(toolName), "event": eventType},
					)
					e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, emitted, "route_plan_preflight_failed", "publish", "route_plan_preflight", wrapped)
					return nil, wrapped
				}
				usePublishAuthority = true
			}
		}
		if !usePublishAuthority {
			spec := runtimecontracts.EmitSpec{Event: eventType}
			parentRoute, allowEntityOnlyParentRoute, err := e.emitParentRouteForActor(ctx, actor, flowID, flowInstance, inbound)
			if err != nil {
				wrapped := failures.WrapDetail(
					"parent_route_lookup_failed",
					"tool-executor",
					"handle_emit_tool.parent_route",
					map[string]any{"event": eventType},
					err,
				)
				e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, emitted, "parent_route_lookup_failed", "publish", "parent_route", wrapped)
				return nil, wrapped
			}
			resolution := runtimepinrouting.ResolveEnvelope(runtimepinrouting.ResolutionInput{
				Source:                     e.workflowSource,
				FlowID:                     flowID,
				EventType:                  eventType,
				Emit:                       spec,
				SourceRoute:                sourceRoute,
				Inbound:                    inbound,
				ParentRoute:                parentRoute,
				AllowEntityOnlyParentRoute: allowEntityOnlyParentRoute,
			}, envelope)
			if resolution.Failure != "" {
				wrapped := failures.NewTarget(
					string(resolution.Failure),
					"tool-executor",
					"handle_emit_tool.pin_target_resolution",
					map[string]any{"tool": strings.TrimSpace(toolName), "event": eventType},
				)
				e.logEmitToolOutcome(ctx, actor, toolName, schemaEventType, eventType, preValidationPayload, postEnrichmentPayload, emitted, "pin_target_resolution_failed", "publish", "pin_target_resolution", wrapped)
				return nil, wrapped
			}
			emitted = events.NewChildEvent(
				emitted.ID(),
				emitted.Type(),
				emitted.SourceAgent(),
				emitted.TaskID(),
				emitted.Payload(),
				emitted.ChainDepth(),
				inbound,
				resolution.Envelope,
				emitted.CreatedAt(),
			)
		}
	}
	if err := e.bus.Publish(ctx, emitted); err != nil {
		wrapped := failures.WrapDetail(
			"event_publish_failed",
			"tool-executor",
			"handle_emit_tool.publish",
			map[string]any{"event": eventType, "event_id": emitted.ID()},
			err,
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
		"event_id":   emitted.ID(),
		"event_type": eventType,
	}, nil
}

func (e *Executor) emitParentRouteForActor(ctx context.Context, actor models.AgentConfig, flowID, flowInstance string, inbound events.Event) (events.RouteIdentity, bool, error) {
	if e == nil {
		return events.RouteIdentity{}, false, nil
	}
	if e.workflowInstances != nil && e.workflowInstances.Enabled() {
		for _, ref := range emitWorkflowInstanceRefs(actor, flowInstance) {
			instance, ok, err := e.workflowInstances.Load(ctx, ref)
			if err != nil {
				return events.RouteIdentity{}, false, err
			}
			if !ok {
				continue
			}
			parent := runtimeflowidentity.ParentRouteFromMetadata(instance.Metadata).Normalized()
			return events.RouteIdentity{
				FlowID:       parent.FlowID,
				FlowInstance: parent.FlowInstance,
				EntityID:     parent.EntityID,
			}.Normalized(), false, nil
		}
	}
	if route, ok := e.staticFlowEntityParentRoute(flowID, inbound); ok {
		return route, true, nil
	}
	return events.RouteIdentity{}, false, nil
}

func emitWorkflowInstanceRefs(actor models.AgentConfig, flowInstance string) []string {
	candidates := []string{
		actor.EffectiveEntityID(),
		strings.Trim(strings.TrimSpace(flowInstance), "/"),
		actor.CanonicalFlowPath(),
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	return out
}

func (e *Executor) staticFlowEntityParentRoute(flowID string, inbound events.Event) (events.RouteIdentity, bool) {
	flowID = strings.TrimSpace(flowID)
	if e == nil || e.workflowSource == nil || flowID == "" {
		return events.RouteIdentity{}, false
	}
	scope, ok := e.workflowSource.FlowScopeByID(flowID)
	if !ok || strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
		return events.RouteIdentity{}, false
	}
	path := strings.Trim(strings.TrimSpace(e.workflowSource.FlowPath(flowID)), "/")
	if !strings.Contains(path, "/") {
		return events.RouteIdentity{}, false
	}
	entityID := strings.TrimSpace(inbound.EntityID())
	if entityID == "" {
		return events.RouteIdentity{}, false
	}
	return events.RouteIdentity{EntityID: entityID}.Normalized(), true
}

func emitFlowInstanceForActorEvent(actor models.AgentConfig, inbound events.Event) string {
	actorFlow := strings.Trim(strings.TrimSpace(actor.CanonicalFlowPath()), "/")
	inboundFlow := strings.Trim(strings.TrimSpace(inbound.FlowInstance()), "/")
	if inboundFlow != "" && flowWithinActorScope(actorFlow, inboundFlow) {
		return inboundFlow
	}
	return actorFlow
}

func emitActorFlowID(source semanticview.Source, actor models.AgentConfig, flowInstance string) string {
	if source == nil {
		return ""
	}
	if agentSource, ok := source.AgentContractSource(actor.ID); ok {
		if flowID := strings.TrimSpace(agentSource.FlowID); flowID != "" {
			return flowID
		}
	}
	actorFlow := strings.Trim(strings.TrimSpace(actor.CanonicalFlowPath()), "/")
	if actorFlow == "" {
		actorFlow = strings.Trim(strings.TrimSpace(flowInstance), "/")
	}
	for _, scope := range source.FlowScopes() {
		path := strings.Trim(strings.TrimSpace(scope.Path), "/")
		if path == "" {
			continue
		}
		if actorFlow == path || strings.HasPrefix(actorFlow, path+"/") {
			return strings.TrimSpace(scope.ID)
		}
	}
	return ""
}

func flowWithinActorScope(actorFlow, inboundFlow string) bool {
	actorFlow = strings.Trim(strings.TrimSpace(actorFlow), "/")
	inboundFlow = strings.Trim(strings.TrimSpace(inboundFlow), "/")
	if actorFlow == "" || inboundFlow == "" {
		return false
	}
	return inboundFlow == actorFlow || strings.HasPrefix(inboundFlow, actorFlow+"/")
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
	flowID := strings.TrimSpace(actor.FlowID)
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
