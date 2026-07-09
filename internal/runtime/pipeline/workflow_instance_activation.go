package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type FlowInstanceActivationRequest struct {
	ContractBundle semanticview.Source
	Instance       runtimeflowidentity.Instance
	InitialState   string
	Config         map[string]any
	Metadata       map[string]any
	TriggerEvent   events.Event
}

type FlowInstanceActivator func(context.Context, FlowInstanceActivationRequest) error

type FlowInstanceDeactivationRequest struct {
	ContractBundle semanticview.Source
	Instance       runtimeflowidentity.Instance
	FinalState     string
}

type FlowInstanceDeactivator func(context.Context, FlowInstanceDeactivationRequest) error

type flowInstanceConfigRefError struct {
	Key    string
	Ref    string
	Reason string
}

func (e flowInstanceConfigRefError) Error() string {
	return fmt.Sprintf("create_flow_instance config_from %q ref %q %s", e.Key, e.Ref, e.Reason)
}

func (pc *PipelineCoordinator) createFlowInstance(ctx context.Context, triggerCtx workflowTriggerContext, plan handlerExecutionPlan, handlerContext values.Context) error {
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
	payload := parsePayloadMap(triggerCtx.Event.Payload())
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
	instance.ParentRoute = runtimeflowidentity.ParentRoute{
		FlowID:       strings.TrimSpace(pipelineFlowScope(ctx)),
		FlowInstance: strings.Trim(strings.TrimSpace(triggerCtx.Event.FlowInstance()), "/"),
		EntityID:     sourceEntityID,
	}
	req := FlowInstanceActivationRequest{
		ContractBundle: pc.SemanticSource(),
		Instance:       instance,
		InitialState:   strings.TrimSpace(plan.AdvancesTo),
		Config:         map[string]any{},
		TriggerEvent:   triggerCtx.Event,
	}
	config, err := resolveFlowInstanceConfig(plan.ConfigFrom, handlerContext)
	if err != nil {
		return err
	}
	req.Config = config
	if len(req.Config) == 0 {
		return fmt.Errorf("create_flow_instance config_from resolved empty")
	}
	if err := pc.instanceActivator(ctx, req); err != nil {
		return err
	}
	if err := pc.armWorkflowCurrentStageTimers(ctx, instance.EntityID, ""); err != nil {
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

func createFlowInstanceHandlerContext(triggerCtx workflowTriggerContext, payload, entity map[string]any) values.Context {
	handlerContext := values.NewContext()
	handlerContext.Event = values.Wrap(triggerCtx.Event.ContextMap(string(triggerCtx.State.Stage)))
	handlerContext.Payload = values.Wrap(payload)
	handlerContext.Entity = values.Wrap(entity)
	return handlerContext
}

func resolveFlowInstanceConfig(spec *runtimecontracts.ConfigFromSpec, handlerContext values.Context) (map[string]any, error) {
	if spec == nil {
		return map[string]any{}, nil
	}
	out := map[string]any{}
	for _, entry := range spec.ConfigEntries() {
		key := strings.TrimSpace(entry.Key)
		if key == "" {
			continue
		}
		value, err := resolveFlowInstanceConfigValue(entry, handlerContext)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func resolveFlowInstanceConfigValue(entry runtimecontracts.ConfigBinding, handlerContext values.Context) (any, error) {
	key := strings.TrimSpace(entry.Key)
	ref := strings.TrimSpace(entry.Ref)
	if ref == "" {
		return nil, flowInstanceConfigRefError{Key: key, Ref: entry.Ref, Reason: "is empty"}
	}
	if entry.RefPath.HasExplicitRoot() {
		switch entry.RefPath.Root {
		case paths.RootPayload, paths.RootEntity, paths.RootPlatformEntity, paths.RootEvent:
			if entry.RefPath.Root == paths.RootEvent {
				if err := events.ValidateEventContextReference(strings.Join(entry.RefPath.Segments, ".")); err != nil {
					return nil, flowInstanceConfigRefError{Key: key, Ref: ref, Reason: err.Error()}
				}
			}
			value, ok := lookupFlowInstanceConfigPath(handlerContext, entry.RefPath)
			if !ok {
				return nil, flowInstanceConfigRefError{Key: key, Ref: ref, Reason: "resolved empty"}
			}
			return value, nil
		default:
			root := entry.RefPath.Root.String()
			if root == "" {
				root = strings.Split(ref, ".")[0]
			}
			return nil, flowInstanceConfigRefError{Key: key, Ref: ref, Reason: fmt.Sprintf("uses unsupported root %q", root)}
		}
	}
	segments := strings.Split(ref, ".")
	if len(segments) == 1 {
		if value, ok := lookupFlowInstanceConfigPath(handlerContext, paths.Path{Root: paths.RootPayload, Segments: segments, Raw: ref}); ok {
			return value, nil
		}
		if value, ok := lookupFlowInstanceConfigPath(handlerContext, paths.Path{Root: paths.RootEntity, Segments: segments, Raw: ref}); ok {
			return value, nil
		}
		return ref, nil
	}
	return nil, flowInstanceConfigRefError{Key: key, Ref: ref, Reason: "requires supported root payload, entity, _entity, or event"}
}

func lookupFlowInstanceConfigPath(handlerContext values.Context, path paths.Path) (any, bool) {
	if path.IsZero() || !path.HasExplicitRoot() {
		return nil, false
	}
	current := any(handlerContext.Bucket(path.Root).Raw())
	for _, segment := range path.Segments {
		object, ok := flowInstanceConfigObject(current)
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

func flowInstanceConfigObject(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case values.Bucket:
		return typed.Raw(), true
	default:
		return nil, false
	}
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
	payload := parsePayloadMap(triggerCtx.Event.Payload())
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
		out["trigger_event_type"] = strings.TrimSpace(string(triggerCtx.Event.Type()))
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
