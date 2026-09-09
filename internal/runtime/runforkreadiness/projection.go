package runforkreadiness

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Projection struct {
	States     []runfork.RunForkSelectedContractWorkflowState
	Blueprints []runtimemanager.AgentMaterializationBlueprint
	Flows      []runtimemanager.TemplateFlowMaterializationPlan
}

func Project(
	plan runfork.RunForkPlan,
	source semanticview.Source,
	planning runfork.RunForkSelectedContractRecipientPlanning,
	sourceModes map[string]executionmode.Mode,
	modelOptions runtimemanager.AgentManagerOptions,
) (*Projection, error) {
	if source == nil {
		return nil, fmt.Errorf("selected-contract workflow state projection requires semantic source")
	}
	if err := validateSelectedContractReadinessEntityMetadata(plan.Entities); err != nil {
		return nil, err
	}
	blueprints, err := StaticAgentBlueprints(source)
	if err != nil {
		return nil, err
	}
	prepared := &Projection{}
	for _, blueprint := range blueprints {
		resolved, err := runtimemanager.ResolveAgentMaterializationBlueprint(modelOptions, blueprint)
		if err != nil {
			return nil, err
		}
		prepared.Blueprints = append(prepared.Blueprints, resolved)
	}
	byEntity := make(map[string]runfork.RunForkSelectedContractWorkflowState)
	recordState := func(state runfork.RunForkSelectedContractWorkflowState) error {
		eventID := strings.TrimSpace(state.SourceEventID)
		mode, ok := sourceModes[eventID]
		if !ok || !mode.Valid() {
			return fmt.Errorf("selected-contract workflow state %s has no typed source execution mode", state.EntityID)
		}
		state.SourceEvents = []runfork.RunForkSelectedContractWorkflowStateSourceEvent{{SourceEventID: eventID, ExecutionMode: mode}}
		if state.Mode == "template" {
			state.ExecutionMode = mode
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
			found := false
			for _, association := range existing.SourceEvents {
				if association.SourceEventID == eventID {
					if association.ExecutionMode != mode {
						return fmt.Errorf("selected-contract source event %s has conflicting execution modes", eventID)
					}
					found = true
				}
			}
			if !found {
				existing.SourceEvents = append(existing.SourceEvents, state.SourceEvents...)
				sort.Slice(existing.SourceEvents, func(i, j int) bool {
					return existing.SourceEvents[i].SourceEventID < existing.SourceEvents[j].SourceEventID
				})
				existing.SourceEventID = existing.SourceEvents[0].SourceEventID
				byEntity[state.EntityID] = existing
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
		state, err := selectedContractPlatformActivityWorkflowState(source, plan, pending)
		if err != nil {
			return nil, err
		}
		if err := recordState(state); err != nil {
			return nil, err
		}
	}
	for _, event := range planning.RecipientPlanEvents {
		eventID := strings.TrimSpace(event.SourceEventID)
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
			node, exact := recipient.Recipient.Node()
			if !exact {
				continue
			}
			state, required, err := selectedContractNodeWorkflowState(source, plan, eventID, node, recipient.Path, recipient.HandlerEvent())
			if err != nil {
				return nil, err
			}
			if !required {
				continue
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
	prepared.Blueprints, err = PreparedActorCensus(prepared.Blueprints)
	if err != nil {
		return nil, err
	}
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
	agentPlan := recipient.AgentPlan.Normalize()
	path := agentPlan.FlowInstance()
	flowID, template, err := selectedContractTemplateFlowForPlan(source, plan.SourceRunID, agentPlan)
	if err != nil {
		return runfork.RunForkSelectedContractWorkflowState{}, false, err
	}
	if !template {
		return runfork.RunForkSelectedContractWorkflowState{}, false, nil
	}
	if path != recipient.Path {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract template agent recipient requires an exact declaration plan for %s", path)
	}
	entity, found, err := selectedContractReadinessEntityForRoute(source, plan, flowID, path)
	if err != nil {
		return runfork.RunForkSelectedContractWorkflowState{}, true, err
	}
	if !found {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract template agent %s has no fixed-revision receiving state", agentPlan.Description())
	}
	route := runtimeflowidentity.StoredRoute(agentPlan.Route.ScopeKey, agentPlan.Route.InstanceID, agentPlan.Route.InstancePath)
	if !route.Valid() || route.InstancePath != path {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract template agent %s has invalid flow route", agentPlan.Description())
	}
	state, err := selectedContractReadinessState(source, eventID, flowID, entity)
	if err != nil {
		return runfork.RunForkSelectedContractWorkflowState{}, true, err
	}
	if state.Route != route {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract agent plan disagrees with fixed receiving route")
	}
	return state, true, nil
}

func selectedContractTemplateFlowForPlan(source semanticview.Source, runID string, plan runtimeagentidentity.Plan) (string, bool, error) {
	plan = plan.Normalize()
	if err := plan.Validate(); err != nil {
		return "", false, err
	}
	if plan.Route.Presence == runtimeagentidentity.RouteRoot {
		return "", false, nil
	}
	path := plan.FlowInstance()
	flowID := ""
	for _, scope := range source.FlowScopes() {
		if !strings.EqualFold(strings.TrimSpace(scope.Mode), "template") ||
			runtimeflowidentity.ScopeKey(source, scope.ID) != plan.Route.ScopeKey {
			continue
		}
		owner, err := runtimepipeline.AdmitWorkflowEntityStateSelectionOwner(source, scope.ID, runID)
		if err != nil {
			return "", false, err
		}
		if !owner.Owns(path) {
			return "", false, fmt.Errorf("selected-contract agent plan route %s is outside declared scope %s", path, scope.ID)
		}
		if flowID != "" {
			return "", false, fmt.Errorf("selected-contract agent path %s has multiple selected workflow owners", path)
		}
		flowID = strings.TrimSpace(scope.ID)
	}
	return flowID, flowID != "", nil
}

func selectedContractPlatformActivityWorkflowState(
	source semanticview.Source,
	plan runfork.RunForkPlan,
	pending runfork.RunForkPendingWork,
) (runfork.RunForkSelectedContractWorkflowState, error) {
	eventID := strings.TrimSpace(pending.EventID)
	routingSource := pending.RoutingSource
	route := routingSource.Route()
	entityID := strings.TrimSpace(route.EntityID)
	if eventID == "" || entityID == "" {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract platform activity requires exact event and entity identity")
	}
	flowID := strings.TrimSpace(route.FlowID)
	instancePath := strings.TrimSpace(route.FlowInstance)
	if routingSource.Kind() == events.RoutingSourceRoot {
		flowID = strings.TrimSpace(semanticview.RootExecutionFlowID(source))
		if flowID == "" {
			return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract root platform activity has no workflow identity")
		}
		instancePath = strings.TrimSpace(plan.SourceRunID)
	} else if routingSource.Kind() != events.RoutingSourceStaticFlow &&
		routingSource.Kind() != events.RoutingSourceConcreteTemplateInstance &&
		routingSource.Kind() != events.RoutingSourceFlowOwnedControl {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract platform activity has unsupported routing source %q", routingSource.Kind().StorageCode())
	}
	if flowID == "" || instancePath == "" {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract platform activity requires exact flow identity")
	}
	schema, exists := source.FlowSchemaByID(flowID)
	if !exists {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract platform activity flow %s has no semantic owner", flowID)
	}
	template := strings.EqualFold(strings.TrimSpace(schema.Mode), "template")
	if template && routingSource.Kind() == events.RoutingSourceStaticFlow {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract template flow %s rejects static routing source", flowID)
	}
	if !template && routingSource.Kind() == events.RoutingSourceConcreteTemplateInstance {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract static flow %s rejects template routing source", flowID)
	}
	entity, found, err := selectedContractReadinessEntityForRoute(source, plan, flowID, instancePath)
	if err != nil {
		return runfork.RunForkSelectedContractWorkflowState{}, err
	}
	if !found || strings.TrimSpace(entity.EntityID) != entityID {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract activity producer has no matching fixed-revision entity ownership")
	}
	return selectedContractReadinessState(source, eventID, flowID, entity)
}

func selectedContractNodeWorkflowState(
	source semanticview.Source,
	plan runfork.RunForkPlan,
	eventID string,
	node runtimeidentity.ExecutableNode,
	recipientPath string,
	localEvent events.EventType,
) (runfork.RunForkSelectedContractWorkflowState, bool, error) {
	if !node.Valid() {
		return runfork.RunForkSelectedContractWorkflowState{}, false, fmt.Errorf("selected-contract recipient has no exact executable node identity")
	}
	if _, ok := source.ExecutableNode(node); !ok {
		return runfork.RunForkSelectedContractWorkflowState{}, false, fmt.Errorf("selected-contract node %s has no semantic owner", node.Key())
	}
	flowID := node.FlowPath()
	if flowID == "" {
		flowID = semanticview.RootExecutionFlowID(source)
	}
	if flowID == "" {
		return runfork.RunForkSelectedContractWorkflowState{}, false, fmt.Errorf("selected-contract node %s has no workflow identity", node.Key())
	}
	handler := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, string(localEvent))
	if !handler.Matched {
		return runfork.RunForkSelectedContractWorkflowState{}, false, fmt.Errorf("selected-contract node %s has no admitted handler for %s", node.Key(), localEvent)
	}
	policy, err := runtimepipeline.CompileDeliveryTargetCompatibilityPolicy(source, node, flowID, localEvent, handler.Handler)
	if err != nil {
		return runfork.RunForkSelectedContractWorkflowState{}, false, err
	}
	path := strings.TrimSpace(recipientPath)
	if flowID == semanticview.RootExecutionFlowID(source) {
		coordinate, err := semanticview.AdmitRootExecutionCoordinate(source, plan.SourceRunID)
		if err != nil {
			return runfork.RunForkSelectedContractWorkflowState{}, false, err
		}
		path = coordinate.RunID()
	}
	entity, found, err := selectedContractReadinessEntityForRoute(source, plan, flowID, path)
	if err != nil {
		return runfork.RunForkSelectedContractWorkflowState{}, false, err
	}
	if !found {
		if policy.Dependency == runtimepipeline.DeliveryTargetExistingEntityRequired {
			return runfork.RunForkSelectedContractWorkflowState{}, false, fmt.Errorf("receiver target owner is missing for flow instance %q", path)
		}
		// Optional absence needs no companion. Fresh acquisition stays with the
		// ordinary publication classifier and its canonical materialization owner.
		return runfork.RunForkSelectedContractWorkflowState{}, false, nil
	}
	state, err := selectedContractReadinessState(source, eventID, flowID, entity)
	return state, true, err
}

func validateSelectedContractReadinessEntityMetadata(entities []runfork.RunForkEntityState) error {
	seen := make(map[string]struct{}, len(entities))
	for _, entity := range entities {
		id := strings.TrimSpace(entity.EntityID)
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("%s: duplicate entity ownership %s", runfork.RunForkMaterializedEntitySnapshotMetadataOwner, id)
		}
		metadata := entity.MaterializationMetadata
		if id == "" || metadata == nil || metadata.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner ||
			metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState ||
			strings.TrimSpace(metadata.FlowInstance) == "" || strings.TrimSpace(metadata.EntityType) == "" {
			return fmt.Errorf("%s: entity %s requires exact fixed-revision owner metadata", runfork.RunForkMaterializedEntitySnapshotMetadataOwner, id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func selectedContractReadinessEntityForRoute(source semanticview.Source, plan runfork.RunForkPlan, flowID, path string) (runfork.RunForkEntityState, bool, error) {
	owner, err := runtimepipeline.AdmitWorkflowEntityStateSelectionOwner(source, flowID, plan.SourceRunID)
	if err != nil {
		return runfork.RunForkEntityState{}, false, err
	}
	if !owner.Owns(path) {
		return runfork.RunForkEntityState{}, false, fmt.Errorf("selected-contract workflow route %q is outside flow scope %q", path, flowID)
	}
	var matched runfork.RunForkEntityState
	found := false
	for _, entity := range plan.Entities {
		metadata := entity.MaterializationMetadata
		if metadata.FlowInstance != path {
			continue
		}
		if found {
			return runfork.RunForkEntityState{}, false, fmt.Errorf("selected-contract receiving route %s has multiple fixed-revision entity owners", path)
		}
		contract, ok := entityruntime.ResolveForFlow(source, flowID)
		if !ok || strings.TrimSpace(contract.EntityType) != metadata.EntityType {
			return runfork.RunForkEntityState{}, false, fmt.Errorf("selected-contract entity %s type %q disagrees with selected flow %s", entity.EntityID, metadata.EntityType, flowID)
		}
		matched, found = entity, true
	}
	return matched, found, nil
}

func selectedContractReadinessState(source semanticview.Source, eventID, flowID string, entity runfork.RunForkEntityState) (runfork.RunForkSelectedContractWorkflowState, error) {
	metadata := entity.MaterializationMetadata
	state := runfork.RunForkSelectedContractWorkflowState{
		SourceEventID: eventID, EntityID: entity.EntityID, EntityType: metadata.EntityType, FlowID: flowID,
		WorkflowVersion: strings.TrimSpace(source.WorkflowVersion()), Mode: "static",
	}
	if flowID == semanticview.RootExecutionFlowID(source) {
		state.AddressKind = runfork.RunForkSelectedContractWorkflowStateRunScope
		return state, nil
	}
	if schema, exists := source.FlowSchemaByID(flowID); exists && strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
		state.Mode = "template"
	}
	state.AddressKind = runfork.RunForkSelectedContractWorkflowStateExact
	state.Route = runtimeflowidentity.StoredRoute(runtimeflowidentity.ScopeKey(source, flowID), runtimeflowidentity.LogicalInstanceID(metadata.FlowInstance), metadata.FlowInstance)
	if !state.Route.Valid() {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract state requires exact fixed-revision route")
	}
	return state, nil
}

func selectedContractWorkflowStatesEqual(left, right runfork.RunForkSelectedContractWorkflowState) bool {
	leftConfig, leftErr := json.Marshal(left.Config)
	rightConfig, rightErr := json.Marshal(right.Config)
	return left.EntityID == right.EntityID && left.EntityType == right.EntityType && left.FlowID == right.FlowID &&
		left.WorkflowVersion == right.WorkflowVersion && left.Mode == right.Mode &&
		(left.Mode != "template" || left.ExecutionMode == right.ExecutionMode) && left.AddressKind == right.AddressKind &&
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

func StaticAgentBlueprints(source semanticview.Source) ([]runtimemanager.AgentMaterializationBlueprint, error) {
	staticRecords, err := runtimemanager.StaticAgentMaterializationBlueprints(source)
	if err != nil {
		return nil, err
	}
	requiredRecords, err := runtimemanager.StaticFlowRequiredAgentMaterializationBlueprints(source)
	if err != nil {
		return nil, err
	}
	return append(staticRecords, requiredRecords...), nil
}
