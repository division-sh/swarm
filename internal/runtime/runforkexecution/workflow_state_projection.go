package runforkexecution

import (
	"context"
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
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func prepareSelectedContractWorkflowReadiness(
	ctx context.Context,
	replay SelectedContractReplayPersistence,
	loaded LoadedSelectedContractSource,
	planning runfork.RunForkSelectedContractRecipientPlanning,
	plan runfork.RunForkPlan,
	sourceEventIDs []string,
	options SelectedContractAgentRuntimeOptions,
) (selectedContractAgentRuntimePlan, []runfork.RunForkSelectedContractWorkflowState, error) {
	var agentRuntime selectedContractAgentRuntimePlan
	if replay == nil {
		return agentRuntime, nil, fmt.Errorf("selected-contract workflow readiness requires replay persistence")
	}
	if !options.ExecutionPosture.Valid() {
		return agentRuntime, nil, fmt.Errorf("selected-contract execution posture is invalid")
	}
	sourceModes, err := replay.LoadRunForkSelectedContractSourceEventModes(ctx, plan.SourceRunID, sourceEventIDs)
	if err != nil {
		return agentRuntime, nil, err
	}
	if len(sourceModes) != len(sourceEventIDs) {
		return agentRuntime, nil, fmt.Errorf("selected-contract source event mode projection is incomplete")
	}
	sourceModeByEvent := make(map[string]executionmode.Mode, len(sourceEventIDs))
	for i, eventID := range sourceEventIDs {
		mode := sourceModes[i]
		if err := options.ExecutionPosture.Admit(mode, "selected-contract source event admission"); err != nil {
			return agentRuntime, nil, err
		}
		sourceModeByEvent[strings.TrimSpace(eventID)] = mode
	}
	modelOptions, _, err := selectedContractAgentModelOptions(options)
	if err != nil {
		return agentRuntime, nil, err
	}
	prepared, err := selectedContractWorkflowStateProjectionWithReadiness(
		plan, loaded.Source, planning, sourceModeByEvent, modelOptions,
	)
	if err != nil {
		return agentRuntime, nil, err
	}
	agentRuntime, err = prepareSelectedContractAgentRuntimeMaterialization(ctx, loaded, planning, prepared.Blueprints, options)
	agentRuntime.Flows = prepared.Flows
	return agentRuntime, prepared.States, err
}

func bindRecoveredSelectedContractAgentRuntime(
	ctx context.Context,
	workflow runtimepipeline.WorkflowPersistence,
	forkRunID string,
	loaded LoadedSelectedContractSource,
	states []runfork.RunForkSelectedContractWorkflowState,
	agentRuntime selectedContractAgentRuntimePlan,
) (selectedContractAgentRuntimePlan, error) {
	forkRunID = strings.TrimSpace(forkRunID)
	bundleHash := loaded.SourceArtifactFact.BundleHash()
	topologies := make([]runfork.RunForkSelectedContractAgentTopology, 0)
	seen := make(map[runtimeagentidentity.Identity]struct{})
	for _, state := range states {
		if state.Mode != "template" {
			continue
		}
		owner, err := runtimeflowidentity.NewRunScopedFlowInstance(forkRunID, state.Route)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, err
		}
		instance, found, err := workflow.LoadWorkflowInstance(ctx, owner)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("load selected-contract recovered workflow %s: %w", state.Route.InstancePath, err)
		}
		if !found {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow %s has no durable flow owner", state.Route.InstancePath)
		}
		expectedJSON, err := json.Marshal(state.Config)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, err
		}
		actualJSON, err := json.Marshal(instance.Config)
		if err != nil || string(expectedJSON) != string(actualJSON) {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow %s disagrees with declaration configuration", state.Route.InstancePath)
		}
		readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(ctx, forkRunID, state.Route)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("load selected-contract recovered workflow readiness %s: %w", state.Route.InstancePath, err)
		}
		if !found {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow %s has no durable readiness owner", state.Route.InstancePath)
		}
		plan, err := readiness.Plan.Normalized()
		if err != nil {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("normalize selected-contract recovered workflow readiness %s: %w", state.Route.InstancePath, err)
		}
		if plan.RunID != forkRunID || plan.Identity.Route() != state.Route || plan.Identity.TemplateID != state.FlowID ||
			plan.Identity.EntityID != state.EntityID || plan.WorkflowVersion != state.WorkflowVersion ||
			plan.ExecutionMode != state.ExecutionMode || plan.BundleHash != bundleHash {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow readiness %s disagrees with regenerated fork state", state.Route.InstancePath)
		}
		expected := make(map[runtimeagentidentity.Identity]string, len(state.Agents))
		for _, agent := range state.Agents {
			identity, err := agent.Plan.Live(forkRunID)
			if err != nil {
				return selectedContractAgentRuntimePlan{}, fmt.Errorf("bind selected-contract recovered agent plan: %w", err)
			}
			expected[identity.Normalize()] = strings.TrimSpace(agent.ConfigRevision)
		}
		if len(expected) != len(plan.Agents) {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow readiness %s has %d agents; regenerated state requires %d", state.Route.InstancePath, len(plan.Agents), len(expected))
		}
		admission, err := runtimemanager.DynamicFlowAgentTopologyAdmission(plan)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("derive selected-contract recovered topology %s: %w", state.Route.InstancePath, err)
		}
		for _, persisted := range plan.Agents {
			identity := persisted.Identity.Normalize()
			revision, ok := expected[identity]
			if !ok || revision != persisted.ConfigRevision {
				return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered agent %s disagrees with durable readiness", identity.Description())
			}
			if _, duplicate := seen[identity]; duplicate {
				return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered agent %s has multiple readiness owners", identity.Description())
			}
			seen[identity] = struct{}{}
			topologies = append(topologies, runfork.RunForkSelectedContractAgentTopology{
				Identity: identity, Admission: admission,
			})
		}
	}
	bound, err := agentRuntime.bindRun(forkRunID, topologies)
	if err != nil {
		return selectedContractAgentRuntimePlan{}, fmt.Errorf("bind selected-contract recovered agent runtime: %w", err)
	}
	return bound, nil
}

type selectedContractWorkflowPreparation struct {
	States     []runfork.RunForkSelectedContractWorkflowState
	Blueprints []runtimemanager.AgentMaterializationBlueprint
	Flows      []runtimemanager.TemplateFlowMaterializationPlan
}

func selectedContractWorkflowStateProjectionWithReadiness(
	plan runfork.RunForkPlan,
	source semanticview.Source,
	planning runfork.RunForkSelectedContractRecipientPlanning,
	sourceModes map[string]executionmode.Mode,
	modelOptions runtimemanager.AgentManagerOptions,
) (*selectedContractWorkflowPreparation, error) {
	if source == nil {
		return nil, fmt.Errorf("selected-contract workflow state projection requires semantic source")
	}
	blueprints, err := selectedContractStaticAgentBlueprints(source)
	if err != nil {
		return nil, err
	}
	prepared := &selectedContractWorkflowPreparation{}
	for _, blueprint := range blueprints {
		resolved, err := runtimemanager.ResolveAgentMaterializationBlueprint(modelOptions, blueprint)
		if err != nil {
			return nil, err
		}
		prepared.Blueprints = append(prepared.Blueprints, resolved)
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
			flow, err := runtimemanager.TemplateFlowMaterialization(source, state.FlowID, state.Route.InstancePath, state.EntityID)
			if err != nil {
				return err
			}
			if flow.Instance.Route() != state.Route {
				return fmt.Errorf("selected-contract workflow %s disagrees with exact declaration route", state.Route.InstancePath)
			}
			state.Config = flow.Config
			if _, exists := byEntity[state.EntityID]; !exists {
				prepared.Flows = append(prepared.Flows, flow)
			}
			for _, unresolved := range flow.Agents {
				blueprint, err := runtimemanager.ResolveAgentMaterializationBlueprint(modelOptions, unresolved)
				if err != nil {
					return err
				}
				plan := blueprint.Identity.Normalize()
				revision, err := runtimemanager.AgentConfigPlanRevision(blueprint.Config, plan)
				if err != nil {
					return fmt.Errorf("selected-contract workflow agent %s revision: %w", plan.Description(), err)
				}
				state.Agents = append(state.Agents, runfork.RunForkSelectedContractAgentExpectation{
					Plan: plan, ConfigRevision: revision,
				})
				if _, exists := byEntity[state.EntityID]; !exists {
					prepared.Blueprints = append(prepared.Blueprints, blueprint)
				}
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
			if err := recipient.Validate(); err != nil {
				return nil, err
			}
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
	prepared.States = out
	return prepared, nil
}

func selectedContractTemplateAgentWorkflowState(
	source semanticview.Source,
	plan runfork.RunForkPlan,
	eventID string,
	recipient runfork.RunForkContractFrontierRecipient,
) (runfork.RunForkSelectedContractWorkflowState, bool, error) {
	if err := recipient.Validate(); err != nil {
		return runfork.RunForkSelectedContractWorkflowState{}, false, err
	}
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
		state.FlowID = strings.TrimSpace(semanticview.RootExecutionFlowID(source))
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
	flowID := node.FlowPath()
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
	if flowID == strings.TrimSpace(semanticview.RootExecutionFlowID(source)) {
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
