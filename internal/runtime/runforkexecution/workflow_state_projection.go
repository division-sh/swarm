package runforkexecution

import (
	"fmt"
	"sort"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func selectedContractWorkflowStateProjection(
	plan runfork.RunForkPlan,
	source semanticview.Source,
	planning runfork.RunForkSelectedContractRecipientPlanning,
) ([]runfork.RunForkSelectedContractWorkflowState, error) {
	if source == nil {
		return nil, fmt.Errorf("selected-contract workflow state projection requires semantic source")
	}
	entityByEvent := make(map[string]string)
	for _, pending := range plan.PendingWork {
		eventID := strings.TrimSpace(pending.EventID)
		entityID := strings.TrimSpace(pending.RoutingSource.Route().EntityID)
		if eventID == "" || entityID == "" {
			continue
		}
		if existing := entityByEvent[eventID]; existing != "" && existing != entityID {
			return nil, fmt.Errorf("selected-contract frontier event %s has conflicting routing-source entities", eventID)
		}
		entityByEvent[eventID] = entityID
	}

	byEntity := make(map[string]runfork.RunForkSelectedContractWorkflowState)
	for _, event := range planning.RecipientPlanEvents {
		eventID := strings.TrimSpace(event.SourceEventID)
		entityID := entityByEvent[eventID]
		if entityID == "" {
			continue
		}
		for _, recipient := range event.Recipients {
			if !recipient.Recipient.IsNode() {
				continue
			}
			state, err := selectedContractNodeWorkflowState(source, eventID, entityID, recipient.Recipient.ID(), recipient.Path)
			if err != nil {
				return nil, err
			}
			if existing, ok := byEntity[entityID]; ok {
				if !selectedContractWorkflowStatesEqual(existing, state) {
					return nil, fmt.Errorf("selected-contract entity %s resolves to multiple workflow state routes", entityID)
				}
				continue
			}
			byEntity[entityID] = state
		}
	}

	out := make([]runfork.RunForkSelectedContractWorkflowState, 0, len(byEntity))
	for _, state := range byEntity {
		out = append(out, state)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EntityID < out[j].EntityID })
	return out, nil
}

func selectedContractNodeWorkflowState(
	source semanticview.Source,
	eventID, entityID, nodeID, recipientPath string,
) (runfork.RunForkSelectedContractWorkflowState, error) {
	contractSource, ok := source.NodeContractSource(strings.TrimSpace(nodeID))
	if !ok {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract node %s has no semantic owner", nodeID)
	}
	flowID := strings.TrimSpace(contractSource.FlowID)
	if flowID == "" && strings.TrimSpace(contractSource.Layer) == "project" {
		flowID = strings.TrimSpace(source.WorkflowName())
	}
	if flowID == "" {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract node %s has no workflow identity", nodeID)
	}
	state := runfork.RunForkSelectedContractWorkflowState{
		SourceEventID: eventID, EntityID: entityID, FlowID: flowID,
		WorkflowVersion: strings.TrimSpace(source.WorkflowVersion()), Mode: "static",
	}
	if flowID == strings.TrimSpace(source.WorkflowName()) {
		state.AddressKind = runfork.RunForkSelectedContractWorkflowStateRunScope
		return state, nil
	}

	scopeKey := runtimeflowidentity.ScopeKey(source, flowID)
	if scopeKey == "" {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract flow %s has no semantic scope", flowID)
	}
	instancePath := scopeKey
	if schema, exists := source.FlowSchemaByID(flowID); exists && strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
		state.Mode = "template"
		instancePath = strings.Trim(strings.TrimSpace(recipientPath), "/")
		if instancePath == "" {
			return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract template flow %s requires exact recipient path", flowID)
		}
	}
	state.AddressKind = runfork.RunForkSelectedContractWorkflowStateExact
	state.Route = runtimeflowidentity.StoredRoute(scopeKey, runtimeflowidentity.LogicalInstanceID(instancePath), instancePath)
	if !state.Route.Valid() || (state.Route.InstancePath != scopeKey && !strings.HasPrefix(state.Route.InstancePath, scopeKey+"/")) {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract workflow route %q is outside flow scope %q", state.Route.InstancePath, scopeKey)
	}
	return state, nil
}

func selectedContractWorkflowStatesEqual(left, right runfork.RunForkSelectedContractWorkflowState) bool {
	return left.EntityID == right.EntityID && left.FlowID == right.FlowID &&
		left.WorkflowVersion == right.WorkflowVersion && left.Mode == right.Mode &&
		left.AddressKind == right.AddressKind && left.Route == right.Route
}
