package pipeline

import (
	"context"
	"fmt"
	"strings"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	"swarm/internal/runtime/core/paths"
	"swarm/internal/runtime/semanticview"
)

type FlowInstanceActivationRequest struct {
	ContractBundle semanticview.Source
	Instance       runtimeflowidentity.Instance
	InitialState   string
	Config         map[string]any
	TriggerEvent   events.Event
}

type FlowInstanceActivator func(context.Context, FlowInstanceActivationRequest) error

type FlowInstanceDeactivationRequest struct {
	ContractBundle semanticview.Source
	Instance       runtimeflowidentity.Instance
	FinalState     string
}

type FlowInstanceDeactivator func(context.Context, FlowInstanceDeactivationRequest) error

func (pc *PipelineCoordinator) createFlowInstance(ctx context.Context, triggerCtx workflowTriggerContext, plan handlerExecutionPlan) error {
	if pc == nil || pc.instanceActivator == nil {
		return fmt.Errorf("flow instance activator is not configured")
	}
	templateID := strings.TrimSpace(plan.Template)
	if templateID == "" {
		return fmt.Errorf("flow instance template is required")
	}
	if source := pc.SemanticSource(); source != nil {
		schema, ok := source.FlowSchemaByID(templateID)
		if !ok || !strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
			return fmt.Errorf("flow template %s is not a template flow", templateID)
		}
	}
	entityID := workflowEventEntityID(triggerCtx.Event)
	payload := parsePayloadMap(triggerCtx.Event.Payload)
	entity := map[string]any{
		"entity_id": entityID,
	}
	if !hasRequiredCreateFlowInstanceSiblings(plan) {
		return fmt.Errorf("create_flow_instance requires non-empty instance_id_from and config_from")
	}
	instanceID := strings.TrimSpace(resolveFlowInstanceID(plan.InstanceIDPath, plan.InstanceIDFrom, payload, entity))
	if instanceID == "" {
		return fmt.Errorf("create_flow_instance instance_id_from resolved empty")
	}
	sourceEntityID := strings.TrimSpace(entityID)
	instance := runtimeflowidentity.Derive(pc.SemanticSource(), templateID, instanceID)
	instance.ParentEntityID = sourceEntityID
	req := FlowInstanceActivationRequest{
		ContractBundle: pc.SemanticSource(),
		Instance:       instance,
		InitialState:   strings.TrimSpace(plan.AdvancesTo),
		Config:         map[string]any{},
		TriggerEvent:   triggerCtx.Event,
	}
	req.Config = resolveFlowInstanceConfig(plan.ConfigFrom, payload, entity)
	if len(req.Config) == 0 {
		return fmt.Errorf("create_flow_instance config_from resolved empty")
	}
	if err := pc.instanceActivator(ctx, req); err != nil {
		return err
	}
	return nil
}

func hasRequiredCreateFlowInstanceSiblings(plan handlerExecutionPlan) bool {
	if strings.TrimSpace(plan.InstanceIDFrom) == "" && !plan.InstanceIDPath.HasExplicitRoot() {
		return false
	}
	if plan.ConfigFrom == nil {
		return false
	}
	return len(plan.ConfigFrom.ConfigEntries()) > 0
}

func resolveFlowInstanceConfig(spec *runtimecontracts.ConfigFromSpec, payload, entity map[string]any) map[string]any {
	if spec == nil {
		return map[string]any{}
	}
	out := map[string]any{}
	for _, entry := range spec.ConfigEntries() {
		key := strings.TrimSpace(entry.Key)
		if key == "" {
			continue
		}
		if value, ok := resolveFlowInstanceValue(entry.RefPath, entry.Ref, payload, entity); ok {
			out[key] = value
		}
	}
	return out
}

func resolveFlowInstanceID(pathSpec paths.Path, expr string, payload, entity map[string]any) string {
	if value, ok := resolveFlowInstanceValue(pathSpec, expr, payload, entity); ok {
		return strings.TrimSpace(asString(value))
	}
	return ""
}

func resolveFlowInstanceValue(pathSpec paths.Path, expr string, payload, entity map[string]any) (any, bool) {
	if pathSpec.HasExplicitRoot() {
		if value, ok := resolveFlowInstancePath(pathSpec, payload, entity); ok {
			return value, true
		}
	}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	segments := strings.Split(expr, ".")
	if len(segments) == 1 {
		if value, ok := payload[segments[0]]; ok {
			return value, true
		}
		if value, ok := entity[segments[0]]; ok {
			return value, true
		}
		return expr, true
	}
	switch strings.TrimSpace(segments[0]) {
	case "payload":
		return resolveFlowInstanceSegments(payload, segments[1:])
	case "entity":
		return resolveFlowInstanceSegments(entity, segments[1:])
	default:
		return nil, false
	}
}

func resolveFlowInstancePath(pathSpec paths.Path, payload, entity map[string]any) (any, bool) {
	switch pathSpec.Root {
	case paths.RootPayload:
		return resolveFlowInstanceSegments(payload, pathSpec.Segments)
	case paths.RootEntity:
		return resolveFlowInstanceSegments(entity, pathSpec.Segments)
	default:
		return nil, false
	}
}

func resolveFlowInstanceSegments(root map[string]any, segments []string) (any, bool) {
	current := any(root)
	for _, segment := range segments {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[strings.TrimSpace(segment)]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func DeriveFlowInstancePath(source semanticview.Source, templateID, instanceID string) string {
	return runtimeflowidentity.InstancePath(source, templateID, instanceID)
}

func (pc *PipelineCoordinator) handlerEmitEnvelope(ctx context.Context, triggerCtx workflowTriggerContext, eventType string) map[string]any {
	payload := parsePayloadMap(triggerCtx.Event.Payload)
	out := map[string]any{}
	entityID := resolveEmittedEntityID(
		pc.SemanticSource(),
		pipelineFlowScope(ctx),
		eventType,
		triggerCtx.State,
		triggerCtx.Event,
		triggerCtx.State.EntityID,
		workflowEventEntityIDWithPayload(triggerCtx.Event, payload),
	)
	if entityID != "" {
		out["entity_id"] = entityID
	}
	if strings.TrimSpace(eventType) != "" {
		out["trigger_event_type"] = strings.TrimSpace(string(triggerCtx.Event.Type))
	}
	if state := strings.TrimSpace(string(triggerCtx.State.Stage)); state != "" {
		out["current_state"] = state
	}
	return out
}

func workflowEntityMetadataPayload(source semanticview.Source, flowID string, metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	allowed := workflowEntitySchemaFields(source, flowID)
	if len(allowed) == 0 {
		return nil
	}
	materialized := workflowMaterializeEntityMetadata(source, flowID, metadata)
	out := make(map[string]any, len(allowed))
	for key := range allowed {
		if value, ok := materialized[key]; ok {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
