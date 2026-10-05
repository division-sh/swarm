package runforkreadiness

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Projection struct {
	States []runfork.RunForkSelectedContractWorkflowState
	// Attachments carry known construction, not additional delivery frontiers.
	Attachments []runfork.RunForkSelectedContractWorkflowState
	Blueprints  []runtimemanager.AgentMaterializationBlueprint
	Flows       []runtimemanager.FlowInstanceMaterializationPlan
}

// MaterializationStates joins attachment evidence with the actual dispatch
// associations. Known actors outside the frontier do not invent deliveries.
func (p Projection) MaterializationStates() ([]runfork.RunForkSelectedContractWorkflowState, error) {
	out := append([]runfork.RunForkSelectedContractWorkflowState(nil), p.States...)
	byEntity := make(map[string]runfork.RunForkSelectedContractWorkflowState, len(out))
	for _, state := range out {
		if _, duplicate := byEntity[state.EntityID]; duplicate {
			return nil, fmt.Errorf("selected readiness repeats a dispatch state")
		}
		byEntity[state.EntityID] = state
	}
	attachments := make(map[string]struct{}, len(p.Attachments))
	for _, attachment := range p.Attachments {
		if _, duplicate := attachments[attachment.EntityID]; duplicate {
			return nil, fmt.Errorf("selected readiness repeats a construction attachment")
		}
		attachments[attachment.EntityID] = struct{}{}
		if attachment.SourceEventID != "" || len(attachment.SourceEvents) != 0 {
			return nil, fmt.Errorf("construction attachment cannot carry delivery associations")
		}
		if selected, exists := byEntity[attachment.EntityID]; exists {
			attachment.ExecutionMode = selected.ExecutionMode
			if !selectedContractWorkflowStatesEqual(selected, attachment) {
				return nil, fmt.Errorf("selected dispatch state disagrees with its construction attachment")
			}
			continue
		}
		out = append(out, attachment)
		byEntity[attachment.EntityID] = attachment
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EntityID < out[j].EntityID })
	return out, nil
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
	if !modelOptions.ExecutionPosture.Valid() {
		return nil, fmt.Errorf("selected-contract readiness requires an admitted process execution posture")
	}
	instances, err := runforkadmission.ConstructedInstances(source, plan)
	if err != nil {
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
	// The complete fixed header census, not the dispatch frontier, owns all known
	// concrete actors. Keyless descendants keep static mode and exact parents.
	flows := make(map[string]runtimemanager.FlowInstanceMaterializationPlan, len(plan.Entities))
	for _, instance := range instances {
		route := instance.Route()
		_, entity, found, err := runforkadmission.FixedConstructionForRoute(source, plan, route)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("selected fixed actor census missing constructed header for %s", instance.InstancePath)
		}
		config, err := runtimepipeline.WorkflowInstanceBusinessConfigForRoute(route, entity.MaterializationMetadata.FlowConfig)
		if err != nil {
			return nil, err
		}
		flow, err := runtimemanager.ConstructedFlowMaterialization(source, plan.SourceRunID, instance, config)
		if err != nil {
			return nil, err
		}
		for i := range flow.Agents {
			flow.Agents[i], err = runtimemanager.ResolveAgentMaterializationBlueprint(modelOptions, flow.Agents[i])
			if err != nil {
				return nil, err
			}
			prepared.Blueprints = append(prepared.Blueprints, flow.Agents[i])
		}
		flows[entity.EntityID] = flow
		prepared.Flows = append(prepared.Flows, flow)
		attachment, err := selectedContractReadinessState(source, "", instance.TemplateID, entity)
		if err != nil {
			return nil, err
		}
		attachment.ExecutionMode = modelOptions.ExecutionPosture.RootMode()
		if err := bindConstructedWorkflowState(&attachment, flow); err != nil {
			return nil, err
		}
		prepared.Attachments = append(prepared.Attachments, attachment)
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
		} else {
			state.ExecutionMode = modelOptions.ExecutionPosture.RootMode()
		}
		if state.AddressKind == runfork.RunForkSelectedContractWorkflowStateExact {
			flow, found := flows[state.EntityID]
			if !found || flow.Instance.Route() != state.Route {
				return fmt.Errorf("selected-contract workflow %s disagrees with exact declaration route", state.Route.InstancePath)
			}
			if err := bindConstructedWorkflowState(&state, flow); err != nil {
				return err
			}
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
				state, required, err := selectedContractAgentWorkflowState(source, plan, eventID, recipient)
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
	sort.Slice(prepared.Attachments, func(i, j int) bool { return prepared.Attachments[i].EntityID < prepared.Attachments[j].EntityID })
	prepared.Blueprints, err = PreparedActorCensus(prepared.Blueprints)
	if err != nil {
		return nil, err
	}
	return prepared, nil
}

func bindConstructedWorkflowState(state *runfork.RunForkSelectedContractWorkflowState, flow runtimemanager.FlowInstanceMaterializationPlan) error {
	state.Config = flow.Config
	for _, blueprint := range flow.Agents {
		plan := blueprint.Identity.Normalize()
		revision, err := runtimemanager.AgentConfigPlanRevision(blueprint.Config, plan)
		if err != nil {
			return fmt.Errorf("selected-contract workflow agent %s revision: %w", plan.Description(), err)
		}
		state.Agents = append(state.Agents, runfork.RunForkSelectedContractAgentExpectation{Plan: plan, ConfigRevision: revision})
	}
	sort.Slice(state.Agents, func(i, j int) bool {
		return runtimeagentidentity.LessPlan(state.Agents[i].Plan, state.Agents[j].Plan)
	})
	return nil
}

func selectedContractAgentWorkflowState(
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
	instance, err := AgentConstruction(source, plan, agentPlan)
	if err != nil {
		return runfork.RunForkSelectedContractWorkflowState{}, false, err
	}
	if agentPlan.Route.Presence == runtimeagentidentity.RouteRoot {
		return runfork.RunForkSelectedContractWorkflowState{}, false, nil
	}
	flowID := instance.TemplateID
	if path != recipient.Path {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract constructed agent recipient requires an exact declaration plan for %s", path)
	}
	entity, found, err := selectedContractReadinessEntityForRoute(source, plan, flowID, path)
	if err != nil {
		return runfork.RunForkSelectedContractWorkflowState{}, true, err
	}
	if !found {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract constructed agent %s has no fixed-revision receiving state", agentPlan.Description())
	}
	route := runtimeflowidentity.StoredRoute(agentPlan.Route.ScopeKey, agentPlan.Route.InstanceID, agentPlan.Route.InstancePath)
	if !route.Valid() || route.InstancePath != path {
		return runfork.RunForkSelectedContractWorkflowState{}, true, fmt.Errorf("selected-contract constructed agent %s has invalid flow route", agentPlan.Description())
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
	template := strings.EqualFold(strings.TrimSpace(schema.EffectiveMode()), "template")
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
	if err := runtimepipeline.ValidateExecutionHandlerDeclaration(source, node, handler.Handler); err != nil {
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
		return runfork.RunForkSelectedContractWorkflowState{}, false, fmt.Errorf("receiver target owner is missing for flow instance %q", path)
	}
	state, err := selectedContractReadinessState(source, eventID, flowID, entity)
	return state, true, err
}

func selectedContractReadinessEntityForRoute(source semanticview.Source, plan runfork.RunForkPlan, flowID, path string) (runfork.RunForkEntityState, bool, error) {
	route := runtimeflowidentity.StoredRoute(runtimeflowidentity.ScopeKey(source, flowID), runtimeflowidentity.LogicalInstanceID(path), path)
	instance, entity, found, err := runforkadmission.FixedConstructionForRoute(source, plan, route)
	if err != nil {
		return runfork.RunForkEntityState{}, false, err
	}
	if found && instance.TemplateID != flowID {
		return runfork.RunForkEntityState{}, false, fmt.Errorf("selected-contract historical header disagrees with selected flow ownership")
	}
	return entity, found, nil
}

func selectedContractReadinessState(source semanticview.Source, eventID, flowID string, entity runfork.RunForkEntityState) (runfork.RunForkSelectedContractWorkflowState, error) {
	metadata := entity.MaterializationMetadata
	state := runfork.RunForkSelectedContractWorkflowState{
		SourceEventID: eventID, EntityID: entity.EntityID, EntityType: metadata.EntityType, FlowID: flowID,
		WorkflowVersion: strings.TrimSpace(source.WorkflowVersion()), Mode: "static",
	}
	var err error
	state.Config, err = runtimepipeline.WorkflowInstanceBusinessConfigForRoute(runtimeflowidentity.RouteForInstancePath(metadata.FlowInstance), metadata.FlowConfig)
	if err != nil {
		return runfork.RunForkSelectedContractWorkflowState{}, fmt.Errorf("selected-contract fixed-revision receiver configuration for entity %s: %w", entity.EntityID, err)
	}
	if flowID == semanticview.RootExecutionFlowID(source) {
		state.AddressKind = runfork.RunForkSelectedContractWorkflowStateRunScope
		return state, nil
	}
	if schema, exists := source.FlowSchemaByID(flowID); exists && strings.EqualFold(strings.TrimSpace(schema.EffectiveMode()), "template") {
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
	leftConfig, leftErr := canonicaljson.MarshalPreservingNumberKinds(left.Config)
	rightConfig, rightErr := canonicaljson.MarshalPreservingNumberKinds(right.Config)
	return left.EntityID == right.EntityID && left.EntityType == right.EntityType && left.FlowID == right.FlowID &&
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
