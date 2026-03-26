package pipeline

import (
	"context"
	"strings"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/paths"
	"swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type FlowInstanceActivationRequest struct {
	ContractBundle semanticview.Source
	TemplateID     string
	InstanceID     string
	EntityID       string
	FlowPath       string
	InitialState   string
	Config         map[string]any
	TriggerEvent   events.Event
}

type FlowInstanceActivator func(context.Context, FlowInstanceActivationRequest) error

type FlowInstanceDeactivationRequest struct {
	ContractBundle semanticview.Source
	TemplateID     string
	InstanceID     string
	EntityID       string
	FlowPath       string
	FinalState     string
}

type FlowInstanceDeactivator func(context.Context, FlowInstanceDeactivationRequest) error

func (pc *FactoryPipelineCoordinator) createFlowInstance(ctx context.Context, triggerCtx workflowTriggerContext, plan handlerExecutionPlan) bool {
	if pc == nil || pc.instanceActivator == nil {
		return false
	}
	templateID := strings.TrimSpace(plan.Template)
	if templateID == "" {
		return false
	}
	entityID := workflowEventEntityID(triggerCtx.Event)
	payload := parsePayloadMap(triggerCtx.Event.Payload)
	entity := map[string]any{
		"entity_id": entityID,
	}
	instanceID := strings.TrimSpace(firstNonEmptyString(
		asString(payload["instance_id"]),
		resolveFlowInstanceID(plan.InstanceIDPath, plan.InstanceIDFrom, payload, entity),
	))
	if instanceID == "" {
		instanceID = uuid.NewString()
	}
	req := FlowInstanceActivationRequest{
		ContractBundle: pc.SemanticSource(),
		TemplateID:     templateID,
		InstanceID:     instanceID,
		EntityID:       entityID,
		FlowPath:       DeriveFlowInstancePath(pc.SemanticSource(), templateID, instanceID),
		InitialState:   strings.TrimSpace(plan.AdvancesTo),
		Config:         map[string]any{},
		TriggerEvent:   triggerCtx.Event,
	}
	if plan.ConfigFrom != nil {
		req.Config = resolveFlowInstanceConfig(plan.ConfigFrom, payload, entity)
	}
	return pc.instanceActivator(ctx, req) == nil
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
	basePath := strings.Trim(strings.TrimSpace(templateID), "/")
	if source != nil {
		if resolved := strings.Trim(strings.TrimSpace(source.FlowPath(templateID)), "/"); resolved != "" {
			basePath = resolved
		}
	}
	instanceID = strings.Trim(strings.TrimSpace(instanceID), "/")
	switch {
	case basePath == "":
		return instanceID
	case instanceID == "":
		return basePath
	default:
		return basePath + "/" + instanceID
	}
}

func (pc *FactoryPipelineCoordinator) handlerEmitPayload(_ context.Context, triggerCtx workflowTriggerContext, eventType string) map[string]any {
	payload := parsePayloadMap(triggerCtx.Event.Payload)
	// Only carry contract-visible entity fields into emitted payloads; internal
	// workflow metadata such as gates, evidence, and runtime bookkeeping should
	// not leak onto the bus.
	out := workflowEntityMetadataPayload(pc.SemanticSource(), triggerCtx.State.Metadata)
	if out == nil {
		out = map[string]any{}
	}
	for key, value := range payload {
		out[key] = value
	}
	entityID := workflowEventEntityIDWithPayload(triggerCtx.Event, payload)
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

func workflowEntityMetadataPayload(source semanticview.Source, metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	allowed := workflowEntitySchemaFields(source)
	if len(allowed) == 0 {
		return nil
	}
	out := make(map[string]any, len(allowed))
	for key, value := range metadata {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
