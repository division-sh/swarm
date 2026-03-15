package pipeline

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
	runtimeengine "empireai/internal/runtime/engine"
	"empireai/internal/runtime/semanticview"
)

type WorkflowContractWarning struct {
	Category string
	Message  string
}

func ValidateWorkflowContracts(source semanticview.Source) error {
	_, err := validateWorkflowContractsDetailed(source)
	return err
}

func ValidateWorkflowContractsDetailed(source semanticview.Source) ([]WorkflowContractWarning, error) {
	return validateWorkflowContractsDetailed(source)
}

func ValidateDefaultWorkflowContracts() error {
	return ValidateWorkflowContracts(defaultWorkflowModule().SemanticSource())
}

func (pc *FactoryPipelineCoordinator) ValidateWorkflowContracts() error {
	return ValidateWorkflowContracts(pc.SemanticSource())
}

func validateWorkflowContracts(source semanticview.Source) error {
	_, err := validateWorkflowContractsDetailed(source)
	return err
}

func validateWorkflowContractsDetailed(source semanticview.Source) ([]WorkflowContractWarning, error) {
	if source == nil {
		return nil, ErrContractBundleNil
	}
	errs := make([]string, 0, 16)
	warnings := make([]WorkflowContractWarning, 0, 8)
	nodes := source.NodeEntries()
	events := source.EventEntries()
	usesOwningNodeModel := contractBundleUsesOwningNodeModel(source)
	runtimeExecutors := supportedWorkflowRuntimeExecutorIDs(source)
	entityFields := workflowEntitySchemaFields(source)
	addWarning := func(category, message string) {
		warnings = append(warnings, WorkflowContractWarning{
			Category: strings.TrimSpace(category),
			Message:  strings.TrimSpace(message),
		})
	}
	projectScopes := source.ProjectScopes()
	for _, scope := range projectScopes {
		if strings.TrimSpace(scope.Manifest.Name) == "" {
			errs = append(errs, fmt.Sprintf("project package %s missing required field name", workflowProjectScopeLabel(scope)))
		}
		if strings.TrimSpace(scope.Manifest.Version) == "" {
			errs = append(errs, fmt.Sprintf("project package %s missing required field version", workflowProjectScopeLabel(scope)))
		}
		for nodeID, node := range scope.Nodes {
			if strings.TrimSpace(nodeID) == "" {
				errs = append(errs, fmt.Sprintf("node in scope %s missing required field id", workflowProjectScopeLabel(scope)))
				continue
			}
			nodeLabel := workflowScopedObjectLabel(workflowProjectScopeLabel(scope), nodeID)
			if strings.TrimSpace(node.ExecutionType) == "" {
				errs = append(errs, fmt.Sprintf("node %s missing required field execution_type", nodeLabel))
			}
			if len(node.SubscribesTo) == 0 {
				errs = append(errs, fmt.Sprintf("node %s missing required field subscribes_to", nodeLabel))
			}
			if len(node.Produces) == 0 {
				errs = append(errs, fmt.Sprintf("node %s missing required field produces", nodeLabel))
			}
			if len(node.EventHandlers) == 0 {
				errs = append(errs, fmt.Sprintf("node %s missing required field event_handlers", nodeLabel))
			}
		}
		for agentID, agent := range scope.Agents {
			if strings.TrimSpace(agentID) == "" {
				errs = append(errs, fmt.Sprintf("agent in scope %s missing required field id", workflowProjectScopeLabel(scope)))
				continue
			}
			agentLabel := workflowScopedObjectLabel(workflowProjectScopeLabel(scope), agentID)
			if strings.TrimSpace(agent.ModelTier) == "" {
				errs = append(errs, fmt.Sprintf("agent %s missing required field model_tier", agentLabel))
			}
			if strings.TrimSpace(agent.ConversationMode) == "" {
				errs = append(errs, fmt.Sprintf("agent %s missing required field conversation_mode", agentLabel))
			}
			if len(agent.Subscriptions) == 0 {
				errs = append(errs, fmt.Sprintf("agent %s missing required field subscriptions", agentLabel))
			}
			if len(agent.EmitEvents) == 0 {
				errs = append(errs, fmt.Sprintf("agent %s missing required field emit_events", agentLabel))
			}
			workflowValidatePromptWarnings(scope.PromptsDir, workflowProjectScopeLabel(scope), agentID, addWarning)
		}
	}
	for flowID, schema := range source.FlowSchemaEntries() {
		if strings.TrimSpace(schema.Name) == "" {
			errs = append(errs, fmt.Sprintf("flow schema %s missing required field name", strings.TrimSpace(flowID)))
		}
		if len(schema.States) == 0 {
			errs = append(errs, fmt.Sprintf("flow schema %s missing required field states", strings.TrimSpace(flowID)))
		}
	}
	for _, scope := range source.FlowScopes() {
		scopeLabel := workflowFlowScopeLabel(scope)
		for nodeID, node := range scope.Nodes {
			if strings.TrimSpace(nodeID) == "" {
				errs = append(errs, fmt.Sprintf("node in scope %s missing required field id", scopeLabel))
				continue
			}
			nodeLabel := workflowScopedObjectLabel(scopeLabel, nodeID)
			if strings.TrimSpace(node.ExecutionType) == "" {
				errs = append(errs, fmt.Sprintf("node %s missing required field execution_type", nodeLabel))
			}
			if len(node.SubscribesTo) == 0 {
				errs = append(errs, fmt.Sprintf("node %s missing required field subscribes_to", nodeLabel))
			}
			if len(node.Produces) == 0 {
				errs = append(errs, fmt.Sprintf("node %s missing required field produces", nodeLabel))
			}
			if len(node.EventHandlers) == 0 {
				errs = append(errs, fmt.Sprintf("node %s missing required field event_handlers", nodeLabel))
			}
		}
		for agentID, agent := range scope.Agents {
			if strings.TrimSpace(agentID) == "" {
				errs = append(errs, fmt.Sprintf("agent in scope %s missing required field id", scopeLabel))
				continue
			}
			agentLabel := workflowScopedObjectLabel(scopeLabel, agentID)
			if strings.TrimSpace(agent.ModelTier) == "" {
				errs = append(errs, fmt.Sprintf("agent %s missing required field model_tier", agentLabel))
			}
			if strings.TrimSpace(agent.ConversationMode) == "" {
				errs = append(errs, fmt.Sprintf("agent %s missing required field conversation_mode", agentLabel))
			}
			if len(agent.Subscriptions) == 0 {
				errs = append(errs, fmt.Sprintf("agent %s missing required field subscriptions", agentLabel))
			}
			if len(agent.EmitEvents) == 0 {
				errs = append(errs, fmt.Sprintf("agent %s missing required field emit_events", agentLabel))
			}
			workflowValidatePromptWarnings(scope.PromptsDir, scopeLabel, agentID, addWarning)
		}
	}
	if strings.TrimSpace(source.PlatformSpec().Platform.Name) == "" {
		errs = append(errs, "platform.name missing")
	}
	if strings.TrimSpace(source.PlatformSpec().Platform.Version) == "" {
		errs = append(errs, "platform.version missing")
	}

	transitions := source.WorkflowTransitions()
	transitionByID := make(map[string]runtimecontracts.WorkflowTransitionContract, len(transitions))
	for _, transition := range transitions {
		id := strings.TrimSpace(transition.ID)
		if id == "" {
			continue
		}
		transitionByID[id] = transition
		if strings.TrimSpace(transition.Trigger) == "" {
			errs = append(errs, fmt.Sprintf("transition %s missing trigger", id))
		} else if !workflowEventExists(source, strings.TrimSpace(transition.Trigger)) {
			errs = append(errs, fmt.Sprintf("transition %s trigger %s missing from event catalog", id, transition.Trigger))
		}
		for _, field := range transition.DataAccumulation.TargetFields() {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if _, ok := entityFields[field]; !ok {
				errs = append(errs, fmt.Sprintf("transition %s data_accumulation field %s missing from workflow entity_schema", id, field))
			}
		}
		for _, actionID := range transition.Actions {
			actionID = strings.TrimSpace(actionID)
			if actionID == "" {
				continue
			}
			action, ok := source.ActionInstructionByID(actionID)
			if !ok {
				errs = append(errs, fmt.Sprintf("transition %s references unknown action %s", id, actionID))
				continue
			}
			if emits := strings.TrimSpace(action.Emits); emits != "" {
				if !workflowEventExists(source, emits) {
					errs = append(errs, fmt.Sprintf("transition %s action %s emits missing event %s", id, actionID, emits))
				}
			}
			if !action.Executable() {
				errs = append(errs, fmt.Sprintf("transition %s action %s has no executable runtime implementation", id, actionID))
			}
		}
		for _, guardID := range transition.Guards {
			guardID = strings.TrimSpace(guardID)
			if guardID == "" {
				continue
			}
			guard, ok := source.GuardInstructionByID(guardID)
			if !ok {
				errs = append(errs, fmt.Sprintf("transition %s references unknown guard %s", id, guardID))
				continue
			}
			if !guard.Executable() {
				errs = append(errs, fmt.Sprintf("transition %s guard %s has no executable runtime implementation", id, guardID))
			}
		}
	}

	for nodeID, node := range nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		nodeFlowID := workflowNodeFlowID(source, nodeID)
		declaredStates := normalizeStringSet(source.FlowStates(nodeFlowID))
		for eventType, handler := range node.EventHandlers {
			if workflowHandlerDeclaresConflictingCompletion(handler) {
				errs = append(errs, fmt.Sprintf("node %s handler %s declares both on_complete and rules", nodeID, strings.TrimSpace(eventType)))
			}
			if strings.TrimSpace(handler.Condition) != "" {
				errs = append(errs, fmt.Sprintf("node %s handler %s uses deprecated handler-level condition", nodeID, strings.TrimSpace(eventType)))
			}
			if strings.TrimSpace(handler.Logic) != "" {
				errs = append(errs, fmt.Sprintf("node %s handler %s uses deprecated logic field", nodeID, strings.TrimSpace(eventType)))
			}
			if onFail := handlerGuardOnFail(handler.Guard); onFail != "" {
				if err := validateWorkflowGuardOnFail(onFail); err != nil {
					errs = append(errs, fmt.Sprintf("node %s handler %s guard %v", nodeID, strings.TrimSpace(eventType), err))
				}
			}
			for _, expr := range workflowHandlerConditions(handler) {
				if err := validateWorkflowConditionCEL(expr); err != nil {
					errs = append(errs, fmt.Sprintf("node %s handler %s CEL parse failed for %q: %v", nodeID, strings.TrimSpace(eventType), expr, err))
				}
			}
			payloadFields := workflowEventPayloadFields(source, eventType)
			for _, expr := range workflowHandlerConditions(handler) {
				for _, ref := range workflowPayloadReferences(expr) {
					if len(payloadFields) > 0 && !workflowPayloadFieldExists(payloadFields, ref) {
						errs = append(errs, fmt.Sprintf("node %s handler %s references payload.%s outside event payload schema", nodeID, strings.TrimSpace(eventType), ref))
					}
				}
			}
			for _, emitted := range workflowHandlerEmits(handler) {
				if strings.TrimSpace(emitted) == strings.TrimSpace(eventType) {
					errs = append(errs, fmt.Sprintf("node %s handler %s emits its own trigger event", nodeID, strings.TrimSpace(eventType)))
				}
			}
			if len(declaredStates) > 0 {
				for _, target := range workflowHandlerAdvanceTargets(handler) {
					if _, ok := declaredStates[strings.TrimSpace(target)]; !ok {
						errs = append(errs, fmt.Sprintf("node %s handler %s advances_to %s outside flow %s states", nodeID, strings.TrimSpace(eventType), strings.TrimSpace(target), nodeFlowID))
					}
				}
			}
			if actionID := strings.TrimSpace(handler.Action.ID); actionID != "" {
				if !workflowHandlerActionExecutable(source, actionID) {
					errs = append(errs, fmt.Sprintf("node %s handler %s action %s is not executable", nodeID, strings.TrimSpace(eventType), actionID))
				}
			}
		}
		subs := normalizeStringSet(node.SubscribesTo)
		produces := normalizeStringSet(node.Produces)
		for _, transitionID := range node.OwnedTransitions {
			transitionID = strings.TrimSpace(transitionID)
			if transitionID == "" {
				continue
			}
			transition, ok := transitionByID[transitionID]
			if !ok {
				errs = append(errs, fmt.Sprintf("node %s owns unknown transition %s", nodeID, transitionID))
				continue
			}
			if owner := strings.TrimSpace(transition.Node); owner != nodeID {
				errs = append(errs, fmt.Sprintf("node %s owns transition %s but workflow owner is %s", nodeID, transitionID, owner))
			}
			trigger := strings.TrimSpace(transition.Trigger)
			if trigger != "" && !usesOwningNodeModel {
				if _, ok := subs[trigger]; !ok {
					if _, emitted := produces[trigger]; !emitted {
						errs = append(errs, fmt.Sprintf("node %s cannot see trigger %s for owned transition %s", nodeID, trigger, transitionID))
					}
				}
			}
		}
	}

	for flowID := range source.FlowSchemaEntries() {
		states := normalizeStringSet(source.FlowStates(flowID))
		initial := strings.TrimSpace(source.FlowInitialStage(flowID))
		if initial != "" {
			if _, ok := states[initial]; !ok {
				errs = append(errs, fmt.Sprintf("flow %s initial_state %s missing from states", flowID, initial))
			}
		}
		for _, eventType := range source.FlowInputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if !workflowEventExists(source, eventType) {
				errs = append(errs, fmt.Sprintf("flow %s input event %s missing from event catalog", flowID, eventType))
			}
		}
		for _, eventType := range source.FlowOutputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if !workflowEventExists(source, eventType) {
				errs = append(errs, fmt.Sprintf("flow %s output event %s missing from event catalog", flowID, eventType))
			}
		}
	}

	for eventType, entry := range events {
		eventType = strings.TrimSpace(eventType)
		handling := strings.TrimSpace(entry.RuntimeHandling)
		owner := strings.TrimSpace(entry.OwningNode)
		if !requiresOwningNode(handling) || !usesOwningNodeModel {
			continue
		}
		if owner == "" {
			errs = append(errs, fmt.Sprintf("event %s with runtime_handling=%s missing owning_node", eventType, handling))
			continue
		}
		if _, ok := nodes[owner]; !ok {
			errs = append(errs, fmt.Sprintf("event %s owning_node %s missing from system nodes", eventType, owner))
			continue
		}
		if _, ok := runtimeExecutors[owner]; !ok {
			errs = append(errs, fmt.Sprintf("event %s owning_node %s has no runtime executor", eventType, owner))
		}
		if handlers := source.NodeEventHandlers(owner); len(handlers) > 0 && nodeSubscribesToEvent(source, owner, eventType) {
			if _, ok := source.NodeEventHandler(owner, eventType); !ok {
				errs = append(errs, fmt.Sprintf("event %s owning_node %s missing semantic event_handler", eventType, owner))
			}
		}
	}

	for _, timer := range source.WorkflowTimers() {
		owner := strings.TrimSpace(timer.Owner)
		if owner == "" {
			errs = append(errs, fmt.Sprintf("timer %s missing owner", timer.ID))
			continue
		}
		if owner != "runtime" {
			if _, systemNode := nodes[owner]; !systemNode {
				if !workflowParticipantExists(source, owner) {
					errs = append(errs, fmt.Sprintf("timer %s owner %s missing from participants", timer.ID, owner))
				}
			}
		}
		if !workflowEventExists(source, strings.TrimSpace(timer.Event)) {
			errs = append(errs, fmt.Sprintf("timer %s event %s missing from event catalog", timer.ID, timer.Event))
		}
	}

	for pin, owners := range workflowWritePinOwners(source) {
		if len(owners) > 1 {
			errs = append(errs, fmt.Sprintf("write pin %s is owned by multiple flows: %s", pin, strings.Join(owners, ", ")))
		}
	}

	for flowID, requiredAgents := range workflowRequiredAgents(source) {
		for _, required := range requiredAgents {
			role := strings.TrimSpace(required.Role)
			if role == "" {
				errs = append(errs, fmt.Sprintf("flow %s required_agents entry missing role", flowID))
				continue
			}
			agentID, agent, ok := workflowRequiredAgentProvider(source, role)
			if !ok {
				errs = append(errs, fmt.Sprintf("flow %s required agent role %s missing from merged agents", flowID, role))
				continue
			}
			if flowRequiresStaticSubscriptionValidation(source, flowID) {
				if diff := diffMissingStrings(required.SubscribesTo, workflowAgentSubscriptions(agent)); diff != "" {
					errs = append(errs, fmt.Sprintf("flow %s required agent %s subscriptions mismatch (%s)", flowID, agentID, diff))
				}
			}
			if diff := diffMissingStrings(required.Emits, agent.EmitEvents); diff != "" {
				errs = append(errs, fmt.Sprintf("flow %s required agent %s emits mismatch (%s)", flowID, agentID, diff))
			}
		}
	}
	for _, required := range source.RequiredAgents() {
		role := strings.TrimSpace(required.Role)
		if role == "" {
			errs = append(errs, "root schema required_agents entry missing role")
			continue
		}
		agentID, agent, ok := workflowRequiredAgentProvider(source, role)
		if !ok {
			errs = append(errs, fmt.Sprintf("root schema required agent role %s missing from merged agents", role))
			continue
		}
		if diff := diffMissingStrings(required.Emits, agent.EmitEvents); diff != "" {
			errs = append(errs, fmt.Sprintf("root required agent %s emits mismatch (%s)", agentID, diff))
		}
	}

	for agentID, agent := range source.AgentEntries() {
		if len(agent.SubscriptionsBootstrap) > 0 {
			errs = append(errs, fmt.Sprintf("agent %s uses deprecated subscriptions_bootstrap", strings.TrimSpace(agentID)))
		}
		for _, toolID := range agent.ToolsTier2 {
			toolID = strings.TrimSpace(toolID)
			if toolID == "" {
				continue
			}
			if _, ok := source.ToolEntries()[toolID]; !ok {
				errs = append(errs, fmt.Sprintf("agent %s references missing tool %s", strings.TrimSpace(agentID), toolID))
			}
		}
	}
	if err := detectEventCycles(source); err != nil {
		errs = append(errs, err.Error())
	}

	warnings = append(warnings, workflowEventCatalogWarnings(source)...)
	warnings = append(warnings, workflowPolicyConflictWarnings(projectScopes, source.FlowScopes())...)

	for _, transition := range source.DerivedHandlerTransitions() {
		if flowID := strings.TrimSpace(transition.FlowID); flowID != "" {
			if target := strings.TrimSpace(transition.AdvancesTo); target != "" {
				validTargets := normalizeStringSet(source.FlowStates(flowID))
				for _, terminal := range source.FlowTerminalStages(flowID) {
					validTargets[strings.TrimSpace(terminal)] = struct{}{}
				}
				if _, ok := validTargets[target]; !ok {
					errs = append(errs, fmt.Sprintf("handler transition %s advances_to %s outside flow %s states", transition.ID, target, flowID))
				}
			}
		}
		if sourceEvent := strings.TrimSpace(transition.DataAccumulation.SourceEvent); sourceEvent != "" {
			if sourceEvent != strings.TrimSpace(transition.EventType) && !workflowDerivedAccumulationSource(sourceEvent) {
				errs = append(errs, fmt.Sprintf("handler transition %s data_accumulation.source_event %s does not match handler event %s", transition.ID, sourceEvent, transition.EventType))
			}
		}
		if gate := strings.TrimSpace(stringValue(transition.SetsGate)); gate != "" {
			node, ok := nodes[strings.TrimSpace(transition.NodeID)]
			if !ok {
				continue
			}
			validGates := stateSchemaGateNames(node.StateSchema)
			if len(validGates) > 0 {
				if _, ok := validGates[gate]; !ok {
					errs = append(errs, fmt.Sprintf("handler transition %s sets_gate %s not recognized in node %s gate_state schema", transition.ID, gate, transition.NodeID))
				}
			}
		}
	}

	if len(errs) > 0 {
		sort.Strings(errs)
		return warnings, fmt.Errorf("workflow contract validation failed:\n- %s", strings.Join(errs, "\n- "))
	}
	sort.Slice(warnings, func(i, j int) bool {
		if warnings[i].Category == warnings[j].Category {
			return warnings[i].Message < warnings[j].Message
		}
		return warnings[i].Category < warnings[j].Category
	})
	return warnings, nil
}

func workflowHandlerDeclaresConflictingCompletion(handler runtimecontracts.SystemNodeEventHandler) bool {
	return len(handler.Rules) > 0 && workflowHandlerHasOnComplete(handler)
}

func workflowHandlerHasOnComplete(handler runtimecontracts.SystemNodeEventHandler) bool {
	if len(handler.OnComplete) > 0 {
		return true
	}
	return handler.Accumulate != nil && len(handler.Accumulate.OnComplete) > 0
}

func workflowWritePinOwners(source semanticview.Source) map[string][]string {
	out := map[string][]string{}
	if source == nil {
		return out
	}
	for _, flowID := range sortedWorkflowValidationKeys(source.FlowSchemaEntries()) {
		for _, pin := range source.FlowWritePins(flowID) {
			pin = strings.TrimSpace(pin)
			if pin == "" {
				continue
			}
			out[pin] = append(out[pin], flowID)
		}
	}
	for pin, owners := range out {
		normalizedPin := strings.TrimSpace(pin)
		if normalizedPin == "" {
			continue
		}
		normalizedOwners := append([]string{}, owners...)
		sort.Strings(normalizedOwners)
		out[normalizedPin] = normalizedOwners
	}
	return out
}

func workflowRequiredAgents(source semanticview.Source) map[string][]runtimecontracts.FlowRequiredAgent {
	out := map[string][]runtimecontracts.FlowRequiredAgent{}
	if source == nil {
		return out
	}
	for flowID := range source.FlowSchemaEntries() {
		out[strings.TrimSpace(flowID)] = source.FlowRequiredAgents(flowID)
	}
	return out
}

func workflowRequiredAgentProvider(source semanticview.Source, role string) (string, runtimecontracts.AgentRegistryEntry, bool) {
	if source == nil {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	role = strings.TrimSpace(role)
	if role == "" {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	requiredKey := normalizeContractRoleKey(role)
	agents := source.AgentEntries()
	for agentID, agent := range agents {
		if normalizeContractRoleKey(agentID) == requiredKey {
			return strings.TrimSpace(agentID), agent, true
		}
	}
	for agentID, agent := range agents {
		if contractRoleMatches(agentID, agent.Role, role) {
			return strings.TrimSpace(agentID), agent, true
		}
	}
	return "", runtimecontracts.AgentRegistryEntry{}, false
}

func workflowEventExists(source semanticview.Source, eventType string) bool {
	if source == nil {
		return false
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return false
	}
	if _, ok := source.ResolvedEventCatalog()[eventType]; ok {
		return true
	}
	_, ok := source.EventEntry(eventType)
	return ok
}

func workflowHandlerActionExecutable(source semanticview.Source, actionID string) bool {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return true
	}
	if isSupportedWorkflowHandlerActionID(actionID) {
		return true
	}
	entry, ok := source.ActionInstructionByID(actionID)
	return ok && entry.Executable()
}

func validateWorkflowGuardOnFail(onFail string) error {
	parsed, err := runtimeengine.ParseGuardFailure(onFail)
	if err != nil {
		return err
	}
	switch parsed.Action {
	case runtimeengine.GuardFailureReject,
		runtimeengine.GuardFailureBlocked,
		runtimeengine.GuardFailureDiscard,
		runtimeengine.GuardFailureKill:
		return nil
	case runtimeengine.GuardFailureEscalate:
		if strings.TrimSpace(parsed.EventType) == "" {
			return fmt.Errorf("on_fail escalate requires event type")
		}
		return nil
	default:
		return fmt.Errorf("on_fail %q is not supported", onFail)
	}
}

func handlerGuardOnFail(spec *runtimecontracts.GuardSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.OnFail)
}

func stateSchemaGateNames(schema runtimecontracts.NodeStateSchema) map[string]struct{} {
	gates := map[string]struct{}{}
	for _, f := range schema.Fields {
		if strings.TrimSpace(f.Name) != "" {
			gates[strings.TrimSpace(f.Name)] = struct{}{}
		}
	}
	return gates
}

func scopedContractLocalID(scopedID string) string {
	scopedID = strings.TrimSpace(scopedID)
	if scopedID == "" {
		return ""
	}
	parts := strings.Split(scopedID, "::")
	return strings.TrimSpace(parts[len(parts)-1])
}

func contractRoleMatches(agentID, agentRole, requiredRole string) bool {
	requiredKey := normalizeContractRoleKey(requiredRole)
	if requiredKey == "" {
		return false
	}
	return normalizeContractRoleKey(agentID) == requiredKey || normalizeContractRoleKey(agentRole) == requiredKey
}

func normalizeContractRoleKey(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.ReplaceAll(raw, "_", "-")
	raw = strings.Join(strings.Fields(raw), "-")
	return raw
}

func flowRequiresStaticSubscriptionValidation(source semanticview.Source, flowID string) bool {
	if source == nil {
		return true
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return true
	}
	schema, ok := source.FlowSchemaByID(flowID)
	if !ok {
		return true
	}
	return !strings.EqualFold(strings.TrimSpace(schema.Mode), "template")
}

func workflowDerivedAccumulationSource(sourceEvent string) bool {
	sourceEvent = strings.TrimSpace(sourceEvent)
	switch {
	case sourceEvent == "":
		return false
	case strings.HasPrefix(sourceEvent, "fan_out."):
		return true
	default:
		return false
	}
}

func workflowAgentSubscriptions(agent runtimecontracts.AgentRegistryEntry) []string {
	values := make([]string, 0, len(agent.SubscribesTo)+len(agent.Subscriptions)+len(agent.SubscriptionsBootstrap))
	values = append(values, agent.SubscribesTo...)
	values = append(values, agent.Subscriptions...)
	values = append(values, agent.SubscriptionsBootstrap...)
	return values
}

func diffMissingStrings(expected, actual []string) string {
	actualSet := normalizeStringSet(actual)
	missing := make([]string, 0)
	for _, value := range expected {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := actualSet[value]; !ok {
			missing = append(missing, value)
		}
	}
	sort.Strings(missing)
	return strings.Join(missing, ", ")
}

func stringValue(v any) string {
	if typed, ok := v.(string); ok {
		return typed
	}
	return ""
}

func recognizedGateNames(schema any) map[string]struct{} {
	switch typed := schema.(type) {
	case map[string]any:
		out := make(map[string]struct{}, len(typed))
		for key := range typed {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			out[key] = struct{}{}
		}
		return out
	case map[any]any:
		out := make(map[string]struct{}, len(typed))
		for key := range typed {
			name := strings.TrimSpace(fmt.Sprint(key))
			if name == "" {
				continue
			}
			out[name] = struct{}{}
		}
		return out
	case string:
		return parseGateNamesFromSchemaString(typed)
	default:
		return nil
	}
}

func parseGateNamesFromSchemaString(schema string) map[string]struct{} {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return nil
	}
	start := strings.Index(schema, "{")
	end := strings.LastIndex(schema, "}")
	if start == -1 || end == -1 || end <= start {
		return nil
	}
	out := map[string]struct{}{}
	for _, part := range strings.Split(schema[start+1:end], ",") {
		name, _, _ := strings.Cut(part, ":")
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func workflowEntitySchemaFields(source semanticview.Source) map[string]struct{} {
	out := map[string]struct{}{}
	if source == nil {
		return out
	}
	collectEntitySchemaFields(source.WorkflowEntitySchema(), out)
	return out
}

func sortedWorkflowValidationKeys[T any](m map[string]T) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func workflowSystemNodeStateSchemaFields(source semanticview.Source, nodeID string) map[string]struct{} {
	out := map[string]struct{}{}
	if source == nil {
		return out
	}
	node, ok := source.NodeEntries()[strings.TrimSpace(nodeID)]
	if !ok {
		return out
	}
	obj, ok := asObject(node.StateSchema)
	if !ok {
		return out
	}
	fields, ok := asObject(obj["fields"])
	if !ok {
		return out
	}
	for key := range fields {
		key = strings.TrimSpace(key)
		if key != "" {
			out[key] = struct{}{}
		}
	}
	return out
}

func collectEntitySchemaFields(raw any, out map[string]struct{}) {
	switch typed := raw.(type) {
	case runtimecontracts.EntitySchema:
		for _, group := range typed.Groups {
			for _, field := range group.Fields {
				name := strings.TrimSpace(field.Name)
				if name != "" {
					out[name] = struct{}{}
				}
			}
		}
		return
	case *runtimecontracts.EntitySchema:
		if typed != nil {
			collectEntitySchemaFields(*typed, out)
		}
		return
	}
	obj, ok := asObject(raw)
	if !ok {
		return
	}
	for key, value := range obj {
		key = strings.TrimSpace(key)
		if key == "" || key == "description" {
			continue
		}
		if child, ok := asObject(value); ok && len(child) > 0 {
			collectEntitySchemaFields(child, out)
			continue
		}
		out[key] = struct{}{}
	}
}

func supportedWorkflowRuntimeExecutorIDs(source semanticview.Source) map[string]struct{} {
	out := map[string]struct{}{}
	if source == nil {
		return out
	}
	for nodeID, entry := range source.NodeEntries() {
		if strings.TrimSpace(nodeID) == "" {
			continue
		}
		if len(source.NodeEventHandlers(nodeID)) > 0 || len(entry.EventHandlers) > 0 {
			out[nodeID] = struct{}{}
		}
	}
	return out
}

func requiresOwningNode(runtimeHandling string) bool {
	switch strings.TrimSpace(runtimeHandling) {
	case "consuming", "dual_delivery", "projection", "stage_projection":
		return true
	default:
		return false
	}
}

func contractBundleUsesOwningNodeModel(source semanticview.Source) bool {
	if source == nil {
		return false
	}
	for _, entry := range source.EventEntries() {
		if strings.TrimSpace(entry.OwningNode) != "" {
			return true
		}
	}
	for _, node := range source.NodeEntries() {
		if len(source.NodeEventHandlers(node.ID)) > 0 {
			return true
		}
	}
	return false
}

func workflowParticipantExists(source semanticview.Source, participant string) bool {
	participant = strings.TrimSpace(participant)
	if participant == "" || source == nil {
		return false
	}
	if participant == "runtime" || participant == "human" {
		return true
	}
	if _, ok := source.NodeEntries()[participant]; ok {
		return true
	}
	for _, agent := range source.AgentEntries() {
		if strings.TrimSpace(agent.ID) == participant || strings.TrimSpace(agent.Role) == participant {
			return true
		}
	}
	return false
}

func normalizeStringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func nodeSubscribesToEvent(source semanticview.Source, nodeID, eventType string) bool {
	if source == nil {
		return false
	}
	node, ok := source.NodeEntries()[strings.TrimSpace(nodeID)]
	if !ok {
		return false
	}
	eventType = strings.TrimSpace(eventType)
	for _, subscribed := range node.SubscribesTo {
		subscribed = strings.TrimSpace(subscribed)
		if subscribed == eventType || runtimecontractsHandlerPatternMatches(subscribed, eventType) {
			return true
		}
	}
	return false
}

func runtimecontractsHandlerPatternMatches(pattern, eventType string) bool {
	pattern = strings.TrimSpace(pattern)
	eventType = strings.TrimSpace(eventType)
	if pattern == "" || eventType == "" {
		return false
	}
	if pattern == eventType {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	matched, err := path.Match(pattern, eventType)
	return err == nil && matched
}

func guardActionEntryByID(entries []runtimecontracts.GuardActionEntry, id string) (runtimecontracts.GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	for _, entry := range entries {
		if strings.TrimSpace(entry.ID) == id {
			return entry, true
		}
	}
	return runtimecontracts.GuardActionEntry{}, false
}

var workflowPayloadReferencePattern = regexp.MustCompile(`payload\.([a-zA-Z_][a-zA-Z0-9_.]*)`)

func workflowNodeFlowID(source semanticview.Source, nodeID string) string {
	if source == nil {
		return ""
	}
	contractSource, ok := source.NodeContractSource(nodeID)
	if !ok {
		return ""
	}
	return strings.TrimSpace(contractSource.FlowID)
}

func workflowHandlerConditions(handler runtimecontracts.SystemNodeEventHandler) []string {
	out := make([]string, 0, 8)
	if handler.Guard != nil {
		if check := strings.TrimSpace(handler.Guard.Check); check != "" {
			out = append(out, check)
		}
		for _, item := range handler.Guard.Checks {
			if check := strings.TrimSpace(item.Check); check != "" {
				out = append(out, check)
			}
		}
	}
	for _, rule := range handler.Rules {
		if condition := strings.TrimSpace(rule.Condition); condition != "" && !strings.EqualFold(condition, "else") {
			out = append(out, condition)
		}
	}
	for _, rule := range workflowHandlerOnCompleteRules(handler) {
		if condition := strings.TrimSpace(rule.Condition); condition != "" && !strings.EqualFold(condition, "else") {
			out = append(out, condition)
		}
	}
	if handler.Filter != nil {
		if condition := strings.TrimSpace(handler.Filter.Condition); condition != "" {
			out = append(out, condition)
		}
	}
	return out
}

func workflowHandlerOnCompleteRules(handler runtimecontracts.SystemNodeEventHandler) []runtimecontracts.HandlerRuleEntry {
	out := append([]runtimecontracts.HandlerRuleEntry{}, handler.OnComplete...)
	if handler.Accumulate != nil {
		out = append(out, handler.Accumulate.OnComplete...)
	}
	return out
}

func validateWorkflowConditionCEL(expression string) error {
	expression = strings.TrimSpace(expression)
	if expression == "" || strings.EqualFold(expression, "else") {
		return nil
	}
	evaluator := newWorkflowExpressionEvaluator()
	if evaluator == nil {
		return fmt.Errorf("workflow expression evaluator is not initialized")
	}
	normalized, _ := normalizeWorkflowExpression(expression, workflowExpressionContext{})
	if normalized == "" {
		return fmt.Errorf("workflow expression is empty")
	}
	_, err := evaluator.program(normalized)
	return err
}

func workflowPayloadReferences(expression string) []string {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil
	}
	matches := workflowPayloadReferencePattern.FindAllStringSubmatch(expression, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		ref := strings.TrimSpace(match[1])
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out
}

func workflowEventPayloadFields(source semanticview.Source, eventType string) map[string]struct{} {
	if source == nil {
		return nil
	}
	entry, ok := source.EventEntry(strings.TrimSpace(eventType))
	if !ok {
		return nil
	}
	out := map[string]struct{}{}
	collectWorkflowPayloadFields("", entry.Payload.Properties, out)
	return out
}

func collectWorkflowPayloadFields(prefix string, fields map[string]runtimecontracts.EventFieldSpec, out map[string]struct{}) {
	for name, field := range fields {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		full := name
		if prefix != "" {
			full = prefix + "." + name
		}
		out[full] = struct{}{}
		_ = field
	}
}

func workflowPayloadFieldExists(fields map[string]struct{}, ref string) bool {
	ref = strings.TrimSpace(ref)
	for field := range fields {
		if ref == field || strings.HasPrefix(ref, field+".") || strings.HasPrefix(field, ref+".") {
			return true
		}
	}
	return false
}

func workflowHandlerEmits(handler runtimecontracts.SystemNodeEventHandler) []string {
	out := append([]string{}, handler.Emits.Values()...)
	for _, rule := range handler.Rules {
		out = append(out, rule.Emits.Values()...)
	}
	for _, rule := range workflowHandlerOnCompleteRules(handler) {
		out = append(out, rule.Emits.Values()...)
	}
	if handler.FanOut != nil {
		if emit := strings.TrimSpace(handler.FanOut.EmitPerItem); emit != "" {
			out = append(out, emit)
		}
		for _, emitted := range handler.FanOut.EmitMapping {
			if emit := strings.TrimSpace(emitted); emit != "" {
				out = append(out, emit)
			}
		}
	}
	return normalizeWorkflowValidationStrings(out)
}

func workflowHandlerAdvanceTargets(handler runtimecontracts.SystemNodeEventHandler) []string {
	out := make([]string, 0, 8)
	if target := strings.TrimSpace(handler.AdvancesTo); target != "" {
		out = append(out, target)
	}
	for _, rule := range workflowHandlerOnCompleteRules(handler) {
		if target := strings.TrimSpace(rule.AdvancesTo); target != "" {
			out = append(out, target)
		}
	}
	return normalizeWorkflowValidationStrings(out)
}

func workflowValidatePromptWarnings(promptsDir, scopeLabel, agentID string, addWarning func(category, message string)) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" || addWarning == nil {
		return
	}
	if strings.TrimSpace(promptsDir) == "" {
		addWarning("PROMPT-MISSING", fmt.Sprintf("%s/%s: no prompt file", strings.TrimSpace(scopeLabel), agentID))
		return
	}
	path := filepath.Join(strings.TrimSpace(promptsDir), agentID+".md")
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			addWarning("PROMPT-MISSING", fmt.Sprintf("%s/%s: no prompt file", strings.TrimSpace(scopeLabel), agentID))
		}
		return
	}
	text := string(content)
	if strings.Contains(text, "<!-- TODO") && !strings.Contains(text, "<!-- DEFERRED") {
		addWarning("PROMPT-STUB", fmt.Sprintf("%s/%s: prompt contains TODO", strings.TrimSpace(scopeLabel), agentID))
	}
}

func workflowEventCatalogWarnings(source semanticview.Source) []WorkflowContractWarning {
	if source == nil {
		return nil
	}
	eventsEmitted := map[string]struct{}{}
	eventsSubscribed := map[string]struct{}{}
	for _, scope := range source.FlowScopes() {
		if eventType := strings.TrimSpace(scope.AutoEmitEvent); eventType != "" {
			eventsEmitted[eventType] = struct{}{}
		}
	}
	for _, node := range source.NodeEntries() {
		for _, eventType := range node.SubscribesTo {
			eventType = strings.TrimSpace(eventType)
			if eventType != "" && !strings.Contains(eventType, "*") {
				eventsSubscribed[eventType] = struct{}{}
			}
		}
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if eventType != "" && !strings.Contains(eventType, "*") {
				eventsSubscribed[eventType] = struct{}{}
			}
			for _, emitted := range workflowHandlerEmits(handler) {
				if emitted != "" {
					eventsEmitted[emitted] = struct{}{}
				}
			}
		}
	}
	for _, agent := range source.AgentEntries() {
		for _, eventType := range agent.EmitEvents {
			if eventType = strings.TrimSpace(eventType); eventType != "" {
				eventsEmitted[eventType] = struct{}{}
			}
		}
		for _, eventType := range append(append([]string{}, agent.Subscriptions...), agent.SubscribesTo...) {
			if eventType = strings.TrimSpace(eventType); eventType != "" && !strings.Contains(eventType, "*") {
				eventsSubscribed[eventType] = struct{}{}
			}
		}
	}
	warnings := make([]WorkflowContractWarning, 0, 8)
	for _, eventType := range sortedWorkflowValidationSetKeys(eventsEmitted) {
		entry, ok := source.EventEntry(eventType)
		if !ok {
			if strings.HasPrefix(eventType, "timer.") || eventType == "pipeline.dead_letter" {
				continue
			}
			warnings = append(warnings, WorkflowContractWarning{
				Category: "EVENT-NO-SCHEMA",
				Message:  fmt.Sprintf("'%s' emitted but no schema in events.yaml", eventType),
			})
			continue
		}
		if _, ok := eventsSubscribed[eventType]; !ok && !strings.EqualFold(strings.TrimSpace(entry.Source), "external") {
			warnings = append(warnings, WorkflowContractWarning{
				Category: "EVENT-NO-CONSUMER",
				Message:  fmt.Sprintf("'%s' emitted but nobody subscribes", eventType),
			})
		}
	}
	for _, eventType := range sortedWorkflowValidationSetKeys(eventsSubscribed) {
		entry, ok := source.EventEntry(eventType)
		if !ok {
			continue
		}
		if _, ok := eventsEmitted[eventType]; ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(entry.Source), "external") || strings.EqualFold(strings.TrimSpace(entry.Status), "planned") {
			continue
		}
		warnings = append(warnings, WorkflowContractWarning{
			Category: "EVENT-NO-PRODUCER",
			Message:  fmt.Sprintf("'%s' subscribed but nobody emits", eventType),
		})
	}
	return warnings
}

func detectEventCycles(source semanticview.Source) error {
	if source == nil {
		return nil
	}
	graph := map[string]map[string]struct{}{}
	for _, node := range source.NodeEntries() {
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" || strings.Contains(eventType, "*") {
				continue
			}
			for _, emitted := range workflowHandlerEmits(handler) {
				emitted = strings.TrimSpace(emitted)
				if emitted == "" || strings.Contains(emitted, "*") {
					continue
				}
				if graph[eventType] == nil {
					graph[eventType] = map[string]struct{}{}
				}
				graph[eventType][emitted] = struct{}{}
			}
		}
	}
	cycles := workflowFindEventCycles(graph)
	if len(cycles) == 0 {
		return nil
	}
	return fmt.Errorf("EVENT-CYCLE: node handler emit cycle: %s", strings.Join(cycles[0], " -> "))
}

func workflowFindEventCycles(graph map[string]map[string]struct{}) [][]string {
	seen := map[string]struct{}{}
	cycles := make([][]string, 0)
	var walk func(start, current string, path []string)
	walk = func(start, current string, path []string) {
		for _, next := range sortedWorkflowValidationSetKeys(graph[current]) {
			if next == start && len(path) > 1 {
				cycle := append(append([]string{}, path...), next)
				key := strings.Join(cycle, "->")
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				cycles = append(cycles, cycle)
				continue
			}
			if _, ok := graph[next]; !ok || workflowContainsString(path, next) {
				continue
			}
			walk(start, next, append(path, next))
		}
	}
	for _, start := range sortedWorkflowValidationSetKeysFromGraph(graph) {
		walk(start, start, []string{start})
	}
	return cycles
}

func sortedWorkflowValidationSetKeysFromGraph(graph map[string]map[string]struct{}) []string {
	keys := make([]string, 0, len(graph))
	for key := range graph {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func workflowContainsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func workflowPolicyConflictWarnings(projectScopes []semanticview.ProjectScope, flowScopes []semanticview.FlowScope) []WorkflowContractWarning {
	rootPolicy := workflowRootPolicyScope(projectScopes)
	if len(rootPolicy.Policy.Values) == 0 {
		return nil
	}
	// TODO(phase4): policy-only child directories without loadable flow schemas are not represented as
	// FlowScopes; this warning currently applies to child flows that are part of the loaded semantic tree.
	warnings := make([]WorkflowContractWarning, 0, 4)
	for _, flow := range flowScopes {
		for key, value := range flow.Policy.Values {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			rootValue, ok := rootPolicy.Policy.Values[key]
			if !ok {
				continue
			}
			if !reflect.DeepEqual(rootValue.Value, value.Value) {
				warnings = append(warnings, WorkflowContractWarning{
					Category: "POLICY-CONFLICT",
					Message:  fmt.Sprintf("'%s': root=%v, %s=%v", key, rootValue.Value, workflowFlowScopeLabel(flow), value.Value),
				})
			}
		}
	}
	return warnings
}

func workflowRootPolicyScope(scopes []semanticview.ProjectScope) semanticview.ProjectScope {
	if len(scopes) == 0 {
		return semanticview.ProjectScope{}
	}
	root := scopes[0]
	for _, scope := range scopes[1:] {
		if scope.Depth < root.Depth {
			root = scope
		}
	}
	return root
}

func workflowProjectScopeLabel(scope semanticview.ProjectScope) string {
	if key := strings.TrimSpace(scope.Key); key != "" {
		return key
	}
	if name := strings.TrimSpace(scope.Manifest.Name); name != "" {
		return name
	}
	return "root"
}

func workflowFlowScopeLabel(scope semanticview.FlowScope) string {
	if id := strings.TrimSpace(scope.ID); id != "" {
		return id
	}
	return strings.TrimSpace(scope.Path)
}

func workflowScopedObjectLabel(scopeLabel, objectID string) string {
	scopeLabel = strings.TrimSpace(scopeLabel)
	objectID = strings.TrimSpace(objectID)
	if scopeLabel == "" {
		return objectID
	}
	if objectID == "" {
		return scopeLabel
	}
	return scopeLabel + "/" + objectID
}

func normalizeWorkflowValidationStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func sortedWorkflowValidationSetKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
