package pipeline

import (
	"fmt"
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
	if strings.TrimSpace(bundle.Workflow.Workflow.Name) == "" {
		errs = append(errs, "workflow.name missing")
	}
	if strings.TrimSpace(bundle.Platform.Platform.Name) == "" {
		errs = append(errs, "platform.name missing")
	}
	if strings.TrimSpace(bundle.Platform.Platform.Version) == "" {
		errs = append(errs, "platform.version missing")
	}

	transitionByID := make(map[string]runtimecontracts.WorkflowTransitionContract, len(bundle.Workflow.Workflow.Transitions))
	for _, transition := range bundle.Workflow.Workflow.Transitions {
		id := strings.TrimSpace(transition.ID)
		if id == "" {
			continue
		}
		transitionByID[id] = transition
		if strings.TrimSpace(transition.Trigger) == "" {
			errs = append(errs, fmt.Sprintf("transition %s missing trigger", id))
		} else if _, ok := bundle.Events[strings.TrimSpace(transition.Trigger)]; !ok {
			errs = append(errs, fmt.Sprintf("transition %s trigger %s missing from event catalog", id, transition.Trigger))
		}
		for _, field := range transition.DataAccumulation.Writes {
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
			action, ok := guardActionEntryByID(bundle.Hooks.Actions, actionID)
			if !ok {
				errs = append(errs, fmt.Sprintf("transition %s references unknown action %s", id, actionID))
				continue
			}
			if emits := strings.TrimSpace(action.Emits); emits != "" {
				if _, ok := bundle.Events[emits]; !ok {
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
			guard, ok := guardActionEntryByID(bundle.Hooks.Guards, guardID)
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
	}

	for _, timer := range bundle.Workflow.Workflow.Timers {
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
		if _, ok := bundle.Events[strings.TrimSpace(timer.Event)]; !ok {
			errs = append(errs, fmt.Sprintf("timer %s event %s missing from event catalog", timer.ID, timer.Event))
		}
	}

	if len(errs) > 0 {
		sort.Strings(errs)
		return fmt.Errorf("workflow contract validation failed:\n- %s", strings.Join(errs, "\n- "))
	}
	return nil
}

func workflowEntitySchemaFields(bundle *runtimecontracts.WorkflowContractBundle) map[string]struct{} {
	out := map[string]struct{}{}
	if bundle == nil {
		return out
	}
	collectEntitySchemaFields(bundle.Workflow.Workflow.EntitySchema, out)
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
		"pipeline-coordinator":    {},
		"scan-orchestrator":       {},
		"discovery-aggregator":    {},
		"validation-orchestrator": {},
		"lifecycle-orchestrator":  {},
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
		if len(node.EventHandlers) > 0 {
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

func guardActionEntryByID(entries []runtimecontracts.GuardActionEntry, id string) (runtimecontracts.GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	for _, entry := range entries {
		if strings.TrimSpace(entry.ID) == id {
			return entry, true
		}
	}
	return runtimecontracts.GuardActionEntry{}, false
}
