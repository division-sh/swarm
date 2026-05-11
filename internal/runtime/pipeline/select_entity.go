package pipeline

import (
	"context"
	"fmt"
	"strings"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/values"
	"swarm/internal/runtime/entityruntime"
)

type selectedHandlerEntity struct {
	EntityID string
	State    WorkflowState
	Event    events.Event
}

func (pc *PipelineCoordinator) selectHandlerEntityForFlow(ctx context.Context, flowID, nodeID string, handler runtimecontracts.SystemNodeEventHandler, evt events.Event) (selectedHandlerEntity, error) {
	if handler.SelectEntity == nil || handler.SelectEntity.Empty() {
		return selectedHandlerEntity{}, nil
	}
	flowID = strings.TrimSpace(flowID)
	nodeID = strings.TrimSpace(nodeID)
	if handler.CreateEntity {
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_invalid: node %s flow %s declares both create_entity and select_entity", nodeID, flowID)
	}
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_unavailable: workflow instance store is required for node %s flow %s", nodeID, flowID)
	}
	expected, err := selectEntityExpectedValues(handler.SelectEntity, evt)
	if err != nil {
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_invalid: node %s flow %s: %w", nodeID, flowID, err)
	}
	candidates, err := pc.workflowStore.List(ctx)
	if err != nil {
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_lookup_failed: node %s flow %s: %w", nodeID, flowID, err)
	}
	matches := make([]WorkflowInstance, 0, 1)
	for _, candidate := range candidates {
		if !workflowInstanceOwnedByFlow(pc.SemanticSource(), candidate, flowID) {
			continue
		}
		if !selectEntityCandidateMatches(candidate, expected) {
			continue
		}
		matches = append(matches, candidate)
	}
	switch len(matches) {
	case 0:
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_no_match: node %s flow %s found no %s entity matching declared key", nodeID, flowID, flowID)
	case 1:
	default:
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_ambiguous: node %s flow %s found %d entities matching declared key", nodeID, flowID, len(matches))
	}
	selected := matches[0]
	entityID := strings.TrimSpace(FlowInstanceEntityID(selected.StorageRef))
	if entityID == "" {
		entityID = strings.TrimSpace(selected.InstanceID)
	}
	if entityID == "" {
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_no_match: node %s flow %s selected entity has empty entity_id", nodeID, flowID)
	}
	state := pc.currentWorkflowState(ctx, entityID)
	if strings.TrimSpace(state.EntityID) == "" {
		state.EntityID = entityID
	}
	selectedEvent := evt.WithEntityID(entityID)
	if storageRef := strings.TrimSpace(selected.StorageRef); storageRef != "" {
		selectedEvent = selectedEvent.WithFlowInstance(storageRef)
	}
	return selectedHandlerEntity{
		EntityID: entityID,
		State:    state,
		Event:    selectedEvent,
	}, nil
}

func selectEntityExpectedValues(spec *runtimecontracts.SelectEntitySpec, evt events.Event) (map[string]any, error) {
	payload := values.NewContext().WithPayload(parsePayloadMap(evt.Payload))
	out := make(map[string]any, len(spec.Bindings))
	for _, binding := range spec.Bindings {
		field := strings.TrimSpace(binding.Field)
		ref := strings.TrimSpace(binding.Ref)
		if field == "" || ref == "" {
			return nil, fmt.Errorf("empty select_entity binding")
		}
		value, ok := payload.Lookup(binding.RefPath)
		if !ok {
			return nil, fmt.Errorf("missing required payload ref %q for field %s", ref, field)
		}
		out[field] = value
	}
	return out, nil
}

func selectEntityCandidateMatches(candidate WorkflowInstance, expected map[string]any) bool {
	for field, value := range expected {
		actual, ok := entityruntime.PathValue(candidate.Metadata, field)
		if !ok {
			return false
		}
		if !workflowJSONValuesEqual(actual, value) {
			return false
		}
	}
	return true
}
