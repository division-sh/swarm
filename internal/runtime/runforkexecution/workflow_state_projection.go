package runforkexecution

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func selectedContractWorkflowStateProjection(
	plan runfork.RunForkPlan,
	source semanticview.Source,
	planning runfork.RunForkSelectedContractRecipientPlanning,
) ([]runfork.RunForkSelectedContractWorkflowState, error) {
	sourceModes := make(map[string]executionmode.Mode, len(planning.RecipientPlanEvents))
	for _, event := range planning.RecipientPlanEvents {
		sourceModes[strings.TrimSpace(event.SourceEventID)] = executionmode.Mock
	}
	return selectedContractWorkflowStateProjectionWithReadiness(plan, source, planning, sourceModes, nil)
}

func selectedContractWorkflowStateProjectionWithReadiness(
	plan runfork.RunForkPlan,
	source semanticview.Source,
	planning runfork.RunForkSelectedContractRecipientPlanning,
	sourceModes map[string]executionmode.Mode,
	agentBlueprints []runtimemanager.AgentMaterializationBlueprint,
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
		eventID := strings.TrimSpace(state.SourceEventID)
		mode, ok := sourceModes[eventID]
		if !ok || !mode.Valid() {
			return fmt.Errorf("selected-contract workflow state %s has no typed source execution mode", state.EntityID)
		}
		state.ExecutionMode = mode
		if state.Mode == "template" {
			config, err := selectedContractTemplateWorkflowConfig(state, agentBlueprints)
			if err != nil {
				return err
			}
			state.Config = config
			for _, blueprint := range agentBlueprints {
				plan := blueprint.Identity.Normalize()
				if plan.FlowInstance() != state.Route.InstancePath {
					continue
				}
				revision, err := runtimemanager.AgentConfigPlanRevision(blueprint.Config, plan)
				if err != nil {
					return fmt.Errorf("selected-contract workflow agent %s revision: %w", plan.Description(), err)
				}
				state.Agents = append(state.Agents, runfork.RunForkSelectedContractAgentExpectation{
					Plan: plan, ConfigRevision: revision,
				})
			}
			sort.Slice(state.Agents, func(i, j int) bool {
				return runtimeagentidentity.LessPlan(state.Agents[i].Plan, state.Agents[j].Plan)
			})
		}
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
		for _, recipient := range event.Recipients {
			if recipient.Recipient.IsAgent() {
				state, required, err := selectedContractTemplateAgentWorkflowState(source, plan, eventID, recipient)
				if err != nil {
					return nil, err
				}
				if required {
					if err := recordState(state); err != nil {
						return nil, err
					}
				}
				continue
			}
			if entityID == "" {
				continue
			}
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

func selectedContractTemplateWorkflowConfig(
	state runfork.RunForkSelectedContractWorkflowState,
	blueprints []runtimemanager.AgentMaterializationBlueprint,
) (map[string]any, error) {
	var selected map[string]any
	var selectedCanonical string
	for _, blueprint := range blueprints {
		if blueprint.Identity.Normalize().FlowInstance() != state.Route.InstancePath {
			continue
		}
		candidate := map[string]any{}
		if len(blueprint.Config.Config) > 0 {
			if err := json.Unmarshal(blueprint.Config.Config, &candidate); err != nil {
				return nil, fmt.Errorf("decode selected-contract workflow config %s: %w", state.Route.InstancePath, err)
			}
		}
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return nil, fmt.Errorf("encode selected-contract workflow config %s: %w", state.Route.InstancePath, err)
		}
		if selected == nil {
			selected = candidate
			selectedCanonical = string(encoded)
			continue
		}
		if selectedCanonical != string(encoded) {
			return nil, fmt.Errorf("selected-contract workflow agents disagree on activation config for %s", state.Route.InstancePath)
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("selected-contract template workflow %s has no declaration blueprint config owner", state.Route.InstancePath)
	}
	return selected, nil
}

func selectedContractTemplateAgentWorkflowState(
	source semanticview.Source,
	plan runfork.RunForkPlan,
	eventID string,
	recipient runfork.RunForkContractFrontierRecipient,
) (runfork.RunForkSelectedContractWorkflowState, bool, error) {
	path := strings.Trim(strings.TrimSpace(recipient.Path), "/")
	flowID, template := selectedContractTemplateFlowForPath(source, path)
	if !template {
		return runfork.RunForkSelectedContractWorkflowState{}, false, nil
	}
	agentPlan := recipient.AgentPlan.Normalize()
	if err := agentPlan.Validate(); err != nil || agentPlan.FlowInstance() != path {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract template agent recipient requires an exact declaration plan for %s", path)
	}
	var matched *runfork.RunForkPendingWork
	for i := range plan.PendingWork {
		pending := &plan.PendingWork[i]
		if strings.TrimSpace(pending.EventID) != eventID || !pending.DeliveryRoute.Recipient.IsAgent() {
			continue
		}
		identity := pending.DeliveryRoute.AgentIdentity.Normalize()
		pendingPlan, err := identity.Plan()
		if err != nil || pendingPlan.Normalize() != agentPlan || identity.RunID != strings.TrimSpace(plan.SourceRunID) {
			continue
		}
		if matched != nil {
			return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract template agent %s has multiple fixed-revision delivery owners", identity.Description())
		}
		matched = pending
	}
	if matched == nil {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract template agent %s has no fixed-revision delivery owner", agentPlan.Description())
	}
	target := matched.DeliveryRoute.Target.Route()
	if strings.TrimSpace(target.FlowID) != flowID || strings.Trim(strings.TrimSpace(target.FlowInstance), "/") != path || strings.TrimSpace(target.EntityID) == "" {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract template agent %s has incomplete target ownership", agentPlan.Description())
	}
	route := runtimeflowidentity.StoredRoute(agentPlan.Route.ScopeKey, agentPlan.Route.InstanceID, agentPlan.Route.InstancePath)
	if !route.Valid() || route.InstancePath != path {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract template agent %s has invalid flow route", agentPlan.Description())
	}
	return runfork.RunForkSelectedContractWorkflowState{
		SourceEventID:   eventID,
		EntityID:        strings.TrimSpace(target.EntityID),
		FlowID:          flowID,
		WorkflowVersion: strings.TrimSpace(source.WorkflowVersion()),
		Mode:            "template",
		AddressKind:     runfork.RunForkSelectedContractWorkflowStateExact,
		Route:           route,
	}, true, nil
}

func selectedContractTemplateFlowForPath(source semanticview.Source, path string) (string, bool) {
	path = strings.Trim(strings.TrimSpace(path), "/")
	flowID := ""
	for _, scope := range source.FlowScopes() {
		if !strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
			continue
		}
		scopePath := strings.Trim(strings.TrimSpace(scope.Path), "/")
		if scopePath == "" || !strings.HasPrefix(path, scopePath+"/") {
			continue
		}
		if flowID != "" {
			return "", false
		}
		flowID = strings.TrimSpace(scope.ID)
	}
	return flowID, flowID != ""
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
	leftConfig, leftErr := json.Marshal(left.Config)
	rightConfig, rightErr := json.Marshal(right.Config)
	return left.EntityID == right.EntityID && left.FlowID == right.FlowID &&
		left.WorkflowVersion == right.WorkflowVersion && left.Mode == right.Mode &&
		left.ExecutionMode == right.ExecutionMode && left.AddressKind == right.AddressKind &&
		left.Route == right.Route && leftErr == nil && rightErr == nil && string(leftConfig) == string(rightConfig) &&
		selectedContractWorkflowStateAgentsEqual(left.Agents, right.Agents)
}

func selectedContractWorkflowStateAgentsEqual(left, right []runfork.RunForkSelectedContractAgentExpectation) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Plan.Normalize() != right[i].Plan.Normalize() ||
			strings.TrimSpace(left[i].ConfigRevision) != strings.TrimSpace(right[i].ConfigRevision) {
			return false
		}
	}
	return true
}
