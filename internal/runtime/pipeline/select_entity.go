package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

var selectOrCreateEntityNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm-select-or-create-entity"))

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
	if handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty() {
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_invalid: node %s flow %s declares both select_entity and select_or_create_entity", nodeID, flowID)
	}
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_unavailable: workflow instance store is required for node %s flow %s", nodeID, flowID)
	}
	expected, err := selectEntityExpectedValues(handler.SelectEntity, evt)
	if err != nil {
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_invalid: node %s flow %s: %w", nodeID, flowID, err)
	}
	matches, err := pc.matchHandlerEntitiesForFlow(ctx, flowID, expected)
	if err != nil {
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_lookup_failed: node %s flow %s: %w", nodeID, flowID, err)
	}
	switch len(matches) {
	case 0:
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_no_match: node %s flow %s found no %s entity matching declared key", nodeID, flowID, flowID)
	case 1:
		return pc.selectedHandlerEntityFromInstance(ctx, flowID, nodeID, evt, matches[0], "select_entity")
	default:
		return selectedHandlerEntity{}, fmt.Errorf("select_entity_ambiguous: node %s flow %s found %d entities matching declared key", nodeID, flowID, len(matches))
	}
}

func (pc *PipelineCoordinator) selectOrCreateHandlerEntityForFlow(ctx context.Context, flowID, nodeID string, handler runtimecontracts.SystemNodeEventHandler, evt events.Event) (selectedHandlerEntity, error) {
	if handler.SelectOrCreateEntity == nil || handler.SelectOrCreateEntity.Empty() {
		return selectedHandlerEntity{}, nil
	}
	flowID = strings.TrimSpace(flowID)
	nodeID = strings.TrimSpace(nodeID)
	if handler.CreateEntity {
		return selectedHandlerEntity{}, fmt.Errorf("select_or_create_entity_invalid: node %s flow %s declares both create_entity and select_or_create_entity", nodeID, flowID)
	}
	if handler.SelectEntity != nil && !handler.SelectEntity.Empty() {
		return selectedHandlerEntity{}, fmt.Errorf("select_or_create_entity_invalid: node %s flow %s declares both select_entity and select_or_create_entity", nodeID, flowID)
	}
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return selectedHandlerEntity{}, fmt.Errorf("select_or_create_entity_unavailable: workflow instance store is required for node %s flow %s", nodeID, flowID)
	}
	expected, err := selectOrCreateEntityExpectedValues(handler.SelectOrCreateEntity, evt)
	if err != nil {
		return selectedHandlerEntity{}, fmt.Errorf("select_or_create_entity_invalid: node %s flow %s: %w", nodeID, flowID, err)
	}
	matches, err := pc.matchHandlerEntitiesForFlow(ctx, flowID, expected)
	if err != nil {
		return selectedHandlerEntity{}, fmt.Errorf("select_or_create_entity_lookup_failed: node %s flow %s: %w", nodeID, flowID, err)
	}
	switch len(matches) {
	case 0:
		return pc.createdHandlerEntityForDeclaredKey(ctx, flowID, nodeID, evt, expected)
	case 1:
		return pc.selectedHandlerEntityFromInstance(ctx, flowID, nodeID, evt, matches[0], "select_or_create_entity")
	default:
		return selectedHandlerEntity{}, fmt.Errorf("select_or_create_entity_ambiguous: node %s flow %s found %d entities matching declared key", nodeID, flowID, len(matches))
	}
}

func (pc *PipelineCoordinator) matchHandlerEntitiesForFlow(ctx context.Context, flowID string, expected map[string]any) ([]WorkflowInstance, error) {
	candidates, err := pc.workflowStore.selectActiveByFields(
		ctx,
		runtimeflowidentity.ScopeKey(pc.SemanticSource(), flowID),
		selectEntityFieldSelectors(expected),
		selectEntityTerminalStates(pc, flowID),
	)
	if err != nil {
		return nil, err
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
	return matches, nil
}

func (pc *PipelineCoordinator) selectedHandlerEntityFromInstance(ctx context.Context, flowID, nodeID string, evt events.Event, selected WorkflowInstance, label string) (selectedHandlerEntity, error) {
	entityID := strings.TrimSpace(FlowInstanceEntityID(selected.StorageRef))
	if entityID == "" {
		entityID = strings.TrimSpace(selected.InstanceID)
	}
	if entityID == "" {
		return selectedHandlerEntity{}, fmt.Errorf("%s_no_match: node %s flow %s selected entity has empty entity_id", label, nodeID, flowID)
	}
	state := pc.currentWorkflowState(ctx, entityID)
	if strings.TrimSpace(state.EntityID) == "" {
		state.EntityID = entityID
	}
	envelope := events.EnvelopeForEntityID(evt.NormalizedEnvelope(), entityID)
	if storageRef := strings.TrimSpace(selected.StorageRef); storageRef != "" {
		envelope = events.EnvelopeForFlowInstance(envelope, storageRef)
	}
	selectedEvent := events.Project(evt, events.ProjectEnvelope(envelope))
	return selectedHandlerEntity{
		EntityID: entityID,
		State:    state,
		Event:    selectedEvent,
	}, nil
}

func selectEntityExpectedValues(spec *runtimecontracts.SelectEntitySpec, evt events.Event) (map[string]any, error) {
	if spec == nil {
		return nil, fmt.Errorf("missing select_entity spec")
	}
	return entityAcquisitionExpectedValues(spec.Bindings, evt)
}

func selectOrCreateEntityExpectedValues(spec *runtimecontracts.SelectOrCreateEntitySpec, evt events.Event) (map[string]any, error) {
	if spec == nil {
		return nil, fmt.Errorf("missing select_or_create_entity spec")
	}
	return entityAcquisitionExpectedValues(spec.Bindings, evt)
}

func entityAcquisitionExpectedValues(bindings []runtimecontracts.SelectEntityKeyBinding, evt events.Event) (map[string]any, error) {
	payload := values.NewContext().WithPayload(parsePayloadMap(evt.Payload()))
	out := make(map[string]any, len(bindings))
	for _, binding := range bindings {
		field := strings.TrimSpace(binding.Field)
		ref := strings.TrimSpace(binding.Ref)
		if field == "" || ref == "" {
			return nil, fmt.Errorf("empty entity acquisition binding")
		}
		value, ok := payload.Lookup(binding.RefPath)
		if !ok {
			return nil, fmt.Errorf("missing required payload ref %q for field %s", ref, field)
		}
		out[field] = value
	}
	return out, nil
}

func (pc *PipelineCoordinator) createdHandlerEntityForDeclaredKey(ctx context.Context, flowID, nodeID string, evt events.Event, expected map[string]any) (selectedHandlerEntity, error) {
	source := pc.SemanticSource()
	instanceID, err := selectOrCreateEntityInstanceID(source, flowID, expected)
	if err != nil {
		return selectedHandlerEntity{}, fmt.Errorf("select_or_create_entity_invalid: node %s flow %s: %w", nodeID, flowID, err)
	}
	instance := deriveFlowInstanceIdentity(source, flowID, instanceID)
	entityID := strings.TrimSpace(instance.EntityID)
	if entityID == "" {
		return selectedHandlerEntity{}, fmt.Errorf("select_or_create_entity_invalid: node %s flow %s derived empty entity_id", nodeID, flowID)
	}
	if existing, ok, err := pc.workflowStore.Load(ctx, entityID); err != nil {
		return selectedHandlerEntity{}, fmt.Errorf("select_or_create_entity_lookup_failed: node %s flow %s: %w", nodeID, flowID, err)
	} else if ok {
		if !workflowInstanceOwnedByFlow(source, existing, flowID) || !selectEntityCandidateMatches(existing, expected) || workflowInstanceInTerminalState(pc, flowID, existing) {
			return selectedHandlerEntity{}, fmt.Errorf("select_or_create_entity_conflict: node %s flow %s deterministic entity %s exists but does not match declared active key", nodeID, flowID, entityID)
		}
		return pc.selectedHandlerEntityFromInstance(ctx, flowID, nodeID, evt, existing, "select_or_create_entity")
	}
	state := WorkflowState{
		EntityID: entityID,
		Stage:    NormalizeWorkflowStateID(workflowInitialStateForFlow(source, flowID)),
		Metadata: workflowCreateEntityMetadata(source, flowID, instance),
	}
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	for field, value := range expected {
		values.Wrap(state.Metadata).SetPath(paths.Parse(field), value)
	}
	envelope := events.EnvelopeForEntityID(evt.NormalizedEnvelope(), entityID)
	envelope = events.EnvelopeForFlowInstance(envelope, instance.InstancePath)
	selectedEvent := events.Project(evt, events.ProjectEnvelope(envelope))
	return selectedHandlerEntity{
		EntityID: entityID,
		State:    state,
		Event:    selectedEvent,
	}, nil
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

func selectEntityFieldSelectors(expected map[string]any) []workflowInstanceFieldSelector {
	out := make([]workflowInstanceFieldSelector, 0, len(expected))
	for field, value := range expected {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		out = append(out, workflowInstanceFieldSelector{Field: field, Value: value})
	}
	return out
}

func selectOrCreateEntityInstanceID(source semanticview.Source, flowID string, expected map[string]any) (string, error) {
	scopeKey := runtimeflowidentity.ScopeKey(source, flowID)
	canonical, err := canonicalEntityAcquisitionKey(scopeKey, expected)
	if err != nil {
		return "", err
	}
	return uuid.NewSHA1(selectOrCreateEntityNamespace, []byte(canonical)).String(), nil
}

func canonicalEntityAcquisitionKey(scopeKey string, expected map[string]any) (string, error) {
	scopeKey = strings.TrimSpace(scopeKey)
	if scopeKey == "" {
		return "", fmt.Errorf("empty target flow scope")
	}
	fields := make([]string, 0, len(expected))
	for field := range expected {
		field = strings.TrimSpace(field)
		if field != "" {
			fields = append(fields, field)
		}
	}
	if len(fields) == 0 {
		return "", fmt.Errorf("empty declared key")
	}
	sort.Strings(fields)
	entries := make([]map[string]any, 0, len(fields))
	for _, field := range fields {
		entries = append(entries, map[string]any{
			"field": field,
			"value": expected[field],
		})
	}
	raw, err := json.Marshal(map[string]any{
		"scope":  scopeKey,
		"fields": entries,
	})
	if err != nil {
		return "", fmt.Errorf("marshal declared key: %w", err)
	}
	return string(raw), nil
}

func workflowInstanceInTerminalState(pc *PipelineCoordinator, flowID string, instance WorkflowInstance) bool {
	if strings.TrimSpace(instance.Status) == "terminated" || !instance.TerminatedAt.IsZero() {
		return true
	}
	state := strings.TrimSpace(instance.CurrentState)
	if state == "" {
		return false
	}
	for _, terminal := range selectEntityTerminalStates(pc, flowID) {
		if strings.EqualFold(strings.TrimSpace(terminal), state) {
			return true
		}
	}
	return false
}

func selectEntityTerminalStates(pc *PipelineCoordinator, flowID string) []string {
	if pc == nil {
		return nil
	}
	source := pc.SemanticSource()
	if source == nil {
		return nil
	}
	return source.FlowTerminalStages(flowID)
}
