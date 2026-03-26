package tools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"swarm/internal/events"
	models "swarm/internal/runtime/core/actors"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type createFlowInstanceInput struct {
	Template   string         `json:"template"`
	InstanceID string         `json:"instance_id"`
	Config     map[string]any `json:"config"`
}

func (e *Executor) execCreateFlowInstance(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	var in createFlowInstanceInput
	if err := decodeToolInput(input, &in); err != nil {
		return nil, fmt.Errorf("create_flow_instance: %w", err)
	}
	templateID := strings.TrimSpace(in.Template)
	if templateID == "" {
		return nil, fmt.Errorf("create_flow_instance: template is required")
	}
	instanceID := strings.TrimSpace(in.InstanceID)
	if instanceID == "" {
		instanceID = uuid.NewString()
	}

	e.mu.RLock()
	activator := e.flowActivator
	source := e.workflowSource
	e.mu.RUnlock()
	if activator == nil {
		return nil, fmt.Errorf("create_flow_instance: flow activation not available")
	}

	entityID := strings.TrimSpace(actor.EffectiveEntityID())
	if entityID == "" {
		return nil, fmt.Errorf("create_flow_instance: actor entity scope is required")
	}

	triggerPayload := map[string]any{
		"template":    templateID,
		"instance_id": instanceID,
		"entity_id":   entityID,
	}
	if len(in.Config) > 0 {
		triggerPayload["config"] = in.Config
	}
	req := runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source,
		TemplateID:     templateID,
		InstanceID:     instanceID,
		EntityID:       entityID,
		FlowPath:       runtimepipeline.DeriveFlowInstancePath(source, templateID, instanceID),
		Config:         cloneToolMap(in.Config),
		TriggerEvent: (events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("tool.create_flow_instance"),
			SourceAgent: actor.ID,
			Payload:     mustJSON(triggerPayload),
			CreatedAt:   time.Now(),
		}).WithEntityID(entityID),
	}
	if err := activator(ctx, req); err != nil {
		return nil, fmt.Errorf("create_flow_instance: %w", err)
	}
	return map[string]any{
		"status":      "created",
		"template":    templateID,
		"instance_id": instanceID,
		"entity_id":   entityID,
	}, nil
}

func cloneToolMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
