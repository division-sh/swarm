package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
	"empireai/internal/runtime/semanticview"
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
	instanceID := strings.TrimSpace(firstNonEmptyString(
		asString(parsePayloadMap(triggerCtx.Event.Payload)["instance_id"]),
		plan.InstanceIDFrom,
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
		req.Config = cloneMap(parsePayloadMap(triggerCtx.Event.Payload))
	}
	return pc.instanceActivator(ctx, req) == nil
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
	out := cloneMap(payload)
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
