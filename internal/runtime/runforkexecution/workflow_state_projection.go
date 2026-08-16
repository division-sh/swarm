package runforkexecution

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
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
	recordState := func(state runfork.RunForkSelectedContractWorkflowState) error {
		if existing, ok := byEntity[state.EntityID]; ok {
			if !selectedContractWorkflowStatesEqual(existing, state) {
				return fmt.Errorf("selected-contract entity %s resolves to multiple workflow state routes", state.EntityID)
			}
			return nil
		}
		byEntity[state.EntityID] = state
		return nil
	}
	platformActivityFrontier := map[string]struct{}{}
	for _, event := range planning.RecipientPlanEvents {
		if strings.TrimSpace(event.EventName) == runfork.RunForkSelectedContractPlatformActivityEvent {
			platformActivityFrontier[strings.TrimSpace(event.SourceEventID)] = struct{}{}
		}
	}
	for _, pending := range plan.PendingWork {
		if _, selected := platformActivityFrontier[strings.TrimSpace(pending.EventID)]; !selected {
			continue
		}
		state, err := selectedContractPlatformActivityWorkflowState(source, pending)
		if err != nil {
			return nil, err
		}
		if err := recordState(state); err != nil {
			return nil, err
		}
	}
	for _, event := range planning.RecipientPlanEvents {
		eventID := strings.TrimSpace(event.SourceEventID)
		entityID := entityByEvent[eventID]
		if entityID == "" {
			continue
		}
		for _, recipient := range event.Recipients {
			node, exact := recipient.Recipient.Node()
			if !exact {
				continue
			}
			state, err := selectedContractNodeWorkflowState(source, eventID, entityID, node, recipient.Path)
			if err != nil {
				return nil, err
			}
			if err := recordState(state); err != nil {
				return nil, err
			}
		}
	}

	out := make([]runfork.RunForkSelectedContractWorkflowState, 0, len(byEntity))
	for _, state := range byEntity {
		out = append(out, state)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EntityID < out[j].EntityID })
	return out, nil
}

func selectedContractPlatformActivityWorkflowState(
	source semanticview.Source,
	pending runfork.RunForkPendingWork,
) (runfork.RunForkSelectedContractWorkflowState, error) {
	eventID := strings.TrimSpace(pending.EventID)
	routingSource := pending.RoutingSource
	route := routingSource.Route()
	entityID := strings.TrimSpace(route.EntityID)
	if eventID == "" || entityID == "" {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract platform activity requires exact event and entity identity")
	}
	state := runfork.RunForkSelectedContractWorkflowState{
		SourceEventID:   eventID,
		EntityID:        entityID,
		WorkflowVersion: strings.TrimSpace(source.WorkflowVersion()),
		Mode:            "static",
	}
	if routingSource.Kind() == events.RoutingSourceRoot {
		state.FlowID = strings.TrimSpace(source.WorkflowName())
		if state.FlowID == "" {
			return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract root platform activity has no workflow identity")
		}
		state.AddressKind = runfork.RunForkSelectedContractWorkflowStateRunScope
		return state, nil
	}
	if routingSource.Kind() != events.RoutingSourceStaticFlow &&
		routingSource.Kind() != events.RoutingSourceConcreteTemplateInstance &&
		routingSource.Kind() != events.RoutingSourceFlowOwnedControl {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract platform activity has unsupported routing source %q", routingSource.Kind().StorageCode())
	}
	flowID := strings.TrimSpace(route.FlowID)
	instancePath := strings.Trim(strings.TrimSpace(route.FlowInstance), "/")
	if flowID == "" || instancePath == "" {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract platform activity requires exact flow identity")
	}
	scopeKey := runtimeflowidentity.ScopeKey(source, flowID)
	if scopeKey == "" {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract flow %s has no semantic scope", flowID)
	}
	schema, exists := source.FlowSchemaByID(flowID)
	if !exists {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract platform activity flow %s has no semantic owner", flowID)
	}
	state.FlowID = flowID
	if strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
		state.Mode = "template"
	}
	if state.Mode == "template" && routingSource.Kind() == events.RoutingSourceStaticFlow {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract template flow %s rejects static routing source", flowID)
	}
	if state.Mode != "template" && routingSource.Kind() == events.RoutingSourceConcreteTemplateInstance {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract static flow %s rejects template routing source", flowID)
	}
	state.AddressKind = runfork.RunForkSelectedContractWorkflowStateExact
	state.Route = runtimeflowidentity.StoredRoute(scopeKey, runtimeflowidentity.LogicalInstanceID(instancePath), instancePath)
	if !state.Route.Valid() || (state.Route.InstancePath != scopeKey && !strings.HasPrefix(state.Route.InstancePath, scopeKey+"/")) {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract workflow route %q is outside flow scope %q", state.Route.InstancePath, scopeKey)
	}
	if state.Mode != "template" && state.Route.InstancePath != scopeKey {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract static workflow route %q must equal flow scope %q", state.Route.InstancePath, scopeKey)
	}
	return state, nil
}

func selectedContractNodeWorkflowState(
	source semanticview.Source,
	eventID, entityID string,
	node runtimeidentity.ExecutableNode,
	recipientPath string,
) (runfork.RunForkSelectedContractWorkflowState, error) {
	if !node.Valid() {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract recipient has no exact executable node identity")
	}
	if _, ok := source.ExecutableNode(node); !ok {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract node %s has no semantic owner", node.Key())
	}
	flowID := node.FlowID()
	if flowID == "" {
		flowID = semanticview.RootExecutionFlowID(source)
	}
	if flowID == "" {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract node %s has no workflow identity", node.Key())
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
