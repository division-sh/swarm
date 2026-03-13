package pipeline

import "strings"

type workflowExpressionContextInput struct {
	State           WorkflowState
	ValidationState *validationPipelineState
	Payload         map[string]any
	Policy          map[string]any
	Accumulated     map[string]any
	FanOut          map[string]any
	ExtraVars       map[string]any
}

func buildWorkflowExpressionContext(input workflowExpressionContextInput) workflowExpressionContext {
	entity := workflowExpressionEntityContext(input.State, input.ValidationState)
	vars := cloneStringAnyMap(input.ExtraVars)
	if vars == nil {
		vars = map[string]any{}
	}
	vars["metadata"] = map[string]any{
		"revision_count": asInt(entity["revision_count"]),
	}
	if len(input.Accumulated) > 0 {
		vars["accumulated"] = cloneStringAnyMap(input.Accumulated)
	}
	if len(input.FanOut) > 0 {
		vars["fan_out"] = cloneStringAnyMap(input.FanOut)
	}
	return workflowExpressionContext{
		Entity:  entity,
		Payload: cloneStringAnyMap(input.Payload),
		Policy:  cloneStringAnyMap(input.Policy),
		Vars:    vars,
	}
}

func workflowExpressionEntityContext(state WorkflowState, validationState *validationPipelineState) map[string]any {
	entity := cloneStringAnyMap(state.Metadata)
	if entity == nil {
		entity = map[string]any{}
	}
	if _, ok := entity["revision_count"]; !ok && validationState != nil {
		entity["revision_count"] = validationState.RevisionCount
	}
	gates := workflowExpressionGateContext(entity, validationState)
	if len(gates) > 0 {
		entity["gates"] = gates
	}
	if stage := strings.TrimSpace(string(state.Stage)); stage != "" {
		entity["current_state"] = stage
		entity["state"] = stage
		entity["stage"] = stage
	}
	if status := strings.TrimSpace(state.Status); status != "" {
		entity["status"] = status
	}
	return entity
}

func workflowExpressionGateContext(entity map[string]any, validationState *validationPipelineState) map[string]any {
	gates := map[string]any{}
	if validationState != nil {
		for key, value := range validationState.gateContext() {
			gates[key] = value
		}
	}
	for key, value := range entity {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if strings.HasPrefix(key, "g") {
			gates[key] = value
		}
	}
	return gates
}
