package pipeline

import (
	"fmt"
	"path"
	"sort"
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/semanticview"
)

func ValidateWorkflowContracts(source semanticview.Source) error {
	return validateWorkflowContracts(source)
}

func ValidateDefaultWorkflowContracts() error {
	return validateWorkflowContracts(defaultWorkflowModule().SemanticSource())
}

func (pc *FactoryPipelineCoordinator) ValidateWorkflowContracts() error {
	return validateWorkflowContracts(pc.SemanticSource())
}

func validateWorkflowContracts(source semanticview.Source) error {
	if source == nil {
		return fmt.Errorf("workflow contract bundle is nil")
	}
	errs := make([]string, 0, 16)
	nodes := source.NodeEntries()
	events := source.EventEntries()
	usesOwningNodeModel := contractBundleUsesOwningNodeModel(source)
	runtimeExecutors := supportedWorkflowRuntimeExecutorIDs(source)
	entityFields := workflowEntitySchemaFields(source)
	if source.WorkflowName() == "" {
		errs = append(errs, "workflow.name missing")
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
		for eventType, handler := range node.EventHandlers {
			if workflowHandlerDeclaresConflictingCompletion(handler) {
				errs = append(errs, fmt.Sprintf("node %s handler %s declares both on_complete and rules", nodeID, strings.TrimSpace(eventType)))
			}
			if onFail := handlerGuardOnFail(handler.Guard); onFail != "" {
				if err := validateWorkflowGuardOnFail(onFail); err != nil {
					errs = append(errs, fmt.Sprintf("node %s handler %s guard %v", nodeID, strings.TrimSpace(eventType), err))
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
		return fmt.Errorf("workflow contract validation failed:\n- %s", strings.Join(errs, "\n- "))
	}
	return nil
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
	onFail = normalizeWorkflowGuardFailureAction(onFail)
	switch {
	case onFail == "", onFail == "blocked", onFail == "discard", onFail == "reject", onFail == "kill":
		return nil
	case strings.HasPrefix(onFail, "escalate:"):
		if strings.TrimSpace(strings.TrimPrefix(onFail, "escalate:")) == "" {
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

func normalizeWorkflowGuardFailureAction(action string) string {
	action = strings.TrimSpace(strings.ToLower(action))
	switch action {
	case "":
		return ""
	case "block":
		return "blocked"
	default:
		return action
	}
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
