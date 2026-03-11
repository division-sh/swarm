package pipeline

import (
	"fmt"
	"path"
	"sort"
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func ValidateWorkflowContracts(bundle *runtimecontracts.WorkflowContractBundle) error {
	return validateWorkflowContracts(bundle)
}

func ValidateDefaultWorkflowContracts() error {
	return validateWorkflowContracts(defaultWorkflowModule().ContractBundle())
}

func (pc *FactoryPipelineCoordinator) ValidateWorkflowContracts() error {
	return validateWorkflowContracts(pc.ContractBundle())
}

func validateWorkflowContracts(bundle *runtimecontracts.WorkflowContractBundle) error {
	if bundle == nil {
		return fmt.Errorf("workflow contract bundle is nil")
	}
	errs := make([]string, 0, 16)
	usesOwningNodeModel := contractBundleUsesOwningNodeModel(bundle)
	runtimeExecutors := supportedWorkflowRuntimeExecutorIDs(bundle)
	entityFields := workflowEntitySchemaFields(bundle)
	if bundle.WorkflowName() == "" {
		errs = append(errs, "workflow.name missing")
	}
	if strings.TrimSpace(bundle.Platform.Platform.Name) == "" {
		errs = append(errs, "platform.name missing")
	}
	if strings.TrimSpace(bundle.Platform.Platform.Version) == "" {
		errs = append(errs, "platform.version missing")
	}

	transitions := bundle.WorkflowTransitions()
	transitionByID := make(map[string]runtimecontracts.WorkflowTransitionContract, len(transitions))
	for _, transition := range transitions {
		id := strings.TrimSpace(transition.ID)
		if id == "" {
			continue
		}
		transitionByID[id] = transition
		if strings.TrimSpace(transition.Trigger) == "" {
			errs = append(errs, fmt.Sprintf("transition %s missing trigger", id))
		} else if !workflowEventExists(bundle, strings.TrimSpace(transition.Trigger)) {
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
			action, ok := bundle.ActionEntryByID(actionID)
			if !ok {
				errs = append(errs, fmt.Sprintf("transition %s references unknown action %s", id, actionID))
				continue
			}
			if emits := strings.TrimSpace(action.Emits); emits != "" {
				if !workflowEventExists(bundle, emits) {
					errs = append(errs, fmt.Sprintf("transition %s action %s emits missing event %s", id, actionID, emits))
				}
			}
			if !isExecutableWorkflowActionEntry(action) {
				errs = append(errs, fmt.Sprintf("transition %s action %s has no executable runtime implementation", id, actionID))
			}
		}
		for _, guardID := range transition.Guards {
			guardID = strings.TrimSpace(guardID)
			if guardID == "" {
				continue
			}
			guard, ok := bundle.GuardEntryByID(guardID)
			if !ok {
				errs = append(errs, fmt.Sprintf("transition %s references unknown guard %s", id, guardID))
				continue
			}
			if !isExecutableWorkflowGuardEntry(guard) {
				errs = append(errs, fmt.Sprintf("transition %s guard %s has no executable runtime implementation", id, guardID))
			}
		}
	}

	for nodeID, node := range bundle.Nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
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

	for flowID := range bundle.FlowSchemas {
		states := normalizeStringSet(bundle.FlowStates(flowID))
		initial := strings.TrimSpace(bundle.FlowInitialStage(flowID))
		if initial != "" {
			if _, ok := states[initial]; !ok {
				errs = append(errs, fmt.Sprintf("flow %s initial_state %s missing from states", flowID, initial))
			}
		}
		for _, eventType := range bundle.FlowInputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if !workflowEventExists(bundle, eventType) {
				errs = append(errs, fmt.Sprintf("flow %s input event %s missing from event catalog", flowID, eventType))
			}
		}
		for _, eventType := range bundle.FlowOutputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if !workflowEventExists(bundle, eventType) {
				errs = append(errs, fmt.Sprintf("flow %s output event %s missing from event catalog", flowID, eventType))
			}
		}
	}

	for eventType, entry := range bundle.Events {
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
		if _, ok := bundle.Nodes[owner]; !ok {
			errs = append(errs, fmt.Sprintf("event %s owning_node %s missing from system nodes", eventType, owner))
			continue
		}
		if _, ok := runtimeExecutors[owner]; !ok {
			errs = append(errs, fmt.Sprintf("event %s owning_node %s has no runtime executor", eventType, owner))
		}
		if handlers := bundle.NodeEventHandlers(owner); len(handlers) > 0 && nodeSubscribesToEvent(bundle, owner, eventType) {
			if _, ok := bundle.NodeEventHandler(owner, eventType); !ok {
				errs = append(errs, fmt.Sprintf("event %s owning_node %s missing semantic event_handler", eventType, owner))
			}
		}
	}

	for _, timer := range bundle.WorkflowTimers() {
		owner := strings.TrimSpace(timer.Owner)
		if owner == "" {
			errs = append(errs, fmt.Sprintf("timer %s missing owner", timer.ID))
			continue
		}
		if owner != "runtime" {
			if _, systemNode := bundle.Nodes[owner]; !systemNode {
				if !workflowParticipantExists(bundle, owner) {
					errs = append(errs, fmt.Sprintf("timer %s owner %s missing from participants", timer.ID, owner))
				}
			}
		}
		if !workflowEventExists(bundle, strings.TrimSpace(timer.Event)) {
			errs = append(errs, fmt.Sprintf("timer %s event %s missing from event catalog", timer.ID, timer.Event))
		}
	}

	for pin, owners := range workflowWritePinOwners(bundle) {
		if len(owners) > 1 {
			errs = append(errs, fmt.Sprintf("write pin %s is owned by multiple flows: %s", pin, strings.Join(owners, ", ")))
		}
	}

	for flowID, requiredAgents := range workflowRequiredAgents(bundle) {
		for _, required := range requiredAgents {
			role := strings.TrimSpace(required.Role)
			if role == "" {
				errs = append(errs, fmt.Sprintf("flow %s required_agents entry missing role", flowID))
				continue
			}
			agentID, agent, ok := workflowRequiredAgentProvider(bundle, role)
			if !ok {
				errs = append(errs, fmt.Sprintf("flow %s required agent role %s missing from merged agents", flowID, role))
				continue
			}
			if flowRequiresStaticSubscriptionValidation(bundle, flowID) {
				if diff := diffMissingStrings(required.SubscribesTo, workflowAgentSubscriptions(agent)); diff != "" {
					errs = append(errs, fmt.Sprintf("flow %s required agent %s subscriptions mismatch (%s)", flowID, agentID, diff))
				}
			}
			if diff := diffMissingStrings(required.Emits, agent.EmitEvents); diff != "" {
				errs = append(errs, fmt.Sprintf("flow %s required agent %s emits mismatch (%s)", flowID, agentID, diff))
			}
		}
	}

	for _, transition := range bundle.DerivedHandlerTransitions() {
		if flowID := strings.TrimSpace(transition.FlowID); flowID != "" {
			if target := strings.TrimSpace(transition.AdvancesTo); target != "" {
				validTargets := normalizeStringSet(bundle.FlowStates(flowID))
				for _, terminal := range bundle.FlowTerminalStages(flowID) {
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
			node, ok := bundle.Nodes[strings.TrimSpace(transition.NodeID)]
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
		return fmt.Errorf("workflow contract validation failed:\n- %s", strings.Join(errs, "\n- "))
	}
	return nil
}

func workflowWritePinOwners(bundle *runtimecontracts.WorkflowContractBundle) map[string][]string {
	out := map[string][]string{}
	if bundle == nil {
		return out
	}
	for pin, owners := range bundle.Semantics.WritePinOwners {
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

func workflowRequiredAgents(bundle *runtimecontracts.WorkflowContractBundle) map[string][]runtimecontracts.FlowRequiredAgent {
	out := map[string][]runtimecontracts.FlowRequiredAgent{}
	if bundle == nil {
		return out
	}
	for flowID := range bundle.Semantics.FlowAgents {
		out[strings.TrimSpace(flowID)] = bundle.FlowRequiredAgents(flowID)
	}
	return out
}

func workflowRequiredAgentProvider(bundle *runtimecontracts.WorkflowContractBundle, role string) (string, runtimecontracts.AgentRegistryEntry, bool) {
	if bundle == nil {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	role = strings.TrimSpace(role)
	if role == "" {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	requiredKey := normalizeContractRoleKey(role)
	for scopedID, agent := range bundle.ScopedAgents {
		localID := scopedContractLocalID(scopedID)
		if normalizeContractRoleKey(localID) == requiredKey {
			return strings.TrimSpace(localID), agent, true
		}
	}
	for scopedID, agent := range bundle.ScopedAgents {
		localID := scopedContractLocalID(scopedID)
		if contractRoleMatches(localID, agent.Role, role) {
			return strings.TrimSpace(localID), agent, true
		}
	}
	for agentID, agent := range bundle.MergedAgents {
		if normalizeContractRoleKey(agentID) == requiredKey {
			return strings.TrimSpace(agentID), agent, true
		}
	}
	for agentID, agent := range bundle.MergedAgents {
		if contractRoleMatches(agentID, agent.Role, role) {
			return strings.TrimSpace(agentID), agent, true
		}
	}
	for agentID, agent := range bundle.Agents {
		if normalizeContractRoleKey(agentID) == requiredKey {
			return strings.TrimSpace(agentID), agent, true
		}
	}
	for agentID, agent := range bundle.Agents {
		if contractRoleMatches(agentID, agent.Role, role) {
			return strings.TrimSpace(agentID), agent, true
		}
	}
	return "", runtimecontracts.AgentRegistryEntry{}, false
}

func workflowEventExists(bundle *runtimecontracts.WorkflowContractBundle, eventType string) bool {
	if bundle == nil {
		return false
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return false
	}
	if _, ok := bundle.MergedEvents[eventType]; ok {
		return true
	}
	for scopedID := range bundle.ScopedEvents {
		if scopedContractLocalID(scopedID) == eventType {
			return true
		}
	}
	_, ok := bundle.Events[eventType]
	return ok
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

func flowRequiresStaticSubscriptionValidation(bundle *runtimecontracts.WorkflowContractBundle, flowID string) bool {
	if bundle == nil {
		return true
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return true
	}
	return !strings.EqualFold(strings.TrimSpace(bundle.FlowSchemas[flowID].Mode), "template")
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

func workflowEntitySchemaFields(bundle *runtimecontracts.WorkflowContractBundle) map[string]struct{} {
	out := map[string]struct{}{}
	if bundle == nil {
		return out
	}
	collectEntitySchemaFields(bundle.WorkflowEntitySchema(), out)
	return out
}

func workflowSystemNodeStateSchemaFields(bundle *runtimecontracts.WorkflowContractBundle, nodeID string) map[string]struct{} {
	out := map[string]struct{}{}
	if bundle == nil {
		return out
	}
	node, ok := bundle.Nodes[strings.TrimSpace(nodeID)]
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

func supportedWorkflowRuntimeExecutorIDs(bundle *runtimecontracts.WorkflowContractBundle) map[string]struct{} {
	out := map[string]struct{}{
		ScoringNodeID:             {},
		"scan-orchestrator":       {},
		"discovery-aggregator":    {},
		"validation-orchestrator": {},
		"lifecycle-orchestrator":  {},
		"portfolio-node":          {},
	}
	if bundle == nil {
		return out
	}
	filtered := make(map[string]struct{}, len(out))
	for nodeID := range out {
		if _, ok := bundle.Nodes[nodeID]; ok {
			filtered[nodeID] = struct{}{}
		}
	}
	return filtered
}

func requiresOwningNode(runtimeHandling string) bool {
	switch strings.TrimSpace(runtimeHandling) {
	case "consuming", "dual_delivery", "projection", "stage_projection":
		return true
	default:
		return false
	}
}

func contractBundleUsesOwningNodeModel(bundle *runtimecontracts.WorkflowContractBundle) bool {
	if bundle == nil {
		return false
	}
	for _, entry := range bundle.Events {
		if strings.TrimSpace(entry.OwningNode) != "" {
			return true
		}
	}
	for _, node := range bundle.Nodes {
		if len(bundle.NodeEventHandlers(node.ID)) > 0 {
			return true
		}
	}
	return false
}

func workflowParticipantExists(bundle *runtimecontracts.WorkflowContractBundle, participant string) bool {
	participant = strings.TrimSpace(participant)
	if participant == "" || bundle == nil {
		return false
	}
	if participant == "runtime" || participant == "human" {
		return true
	}
	if _, ok := bundle.Nodes[participant]; ok {
		return true
	}
	for _, agent := range bundle.Agents {
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

func nodeSubscribesToEvent(bundle *runtimecontracts.WorkflowContractBundle, nodeID, eventType string) bool {
	if bundle == nil {
		return false
	}
	node, ok := bundle.Nodes[strings.TrimSpace(nodeID)]
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
