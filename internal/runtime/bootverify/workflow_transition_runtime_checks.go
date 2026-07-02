package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkTransitionReferenceValidation(c *checkerContext) []Finding { return c.transitionReferences() }
func checkTransitionOwnershipValidation(c *checkerContext) []Finding { return c.transitionOwnership() }
func checkEventRuntimeWiringValidation(c *checkerContext) []Finding  { return c.eventRuntimeWiring() }

func (c *checkerContext) transitionReferences() []Finding {
	if c.transitionRefLoaded {
		return c.transitionRefFindings
	}
	c.transitionRefLoaded = true
	for _, transition := range c.source.WorkflowTransitions() {
		id := strings.TrimSpace(transition.ID)
		if id == "" {
			continue
		}
		if strings.TrimSpace(transition.Trigger) == "" {
			c.transitionRefFindings = append(c.transitionRefFindings, Finding{
				CheckID:  "transition_reference_validation",
				Severity: "error",
				Message:  fmt.Sprintf("transition %s missing trigger", id),
				Location: id,
			})
		} else if !eventExists(c.source, strings.TrimSpace(transition.Trigger)) {
			c.transitionRefFindings = append(c.transitionRefFindings, Finding{
				CheckID:  "transition_reference_validation",
				Severity: "error",
				Message:  fmt.Sprintf("transition %s trigger %s missing from event catalog", id, transition.Trigger),
				Location: id,
			})
		}
		for _, actionID := range transition.Actions {
			actionID = strings.TrimSpace(actionID)
			if actionID == "" {
				continue
			}
			action, ok := c.source.ActionInstructionByID(actionID)
			if !ok {
				c.transitionRefFindings = append(c.transitionRefFindings, Finding{
					CheckID:  "transition_reference_validation",
					Severity: "error",
					Message:  fmt.Sprintf("transition %s references unknown action %s", id, actionID),
					Location: id,
				})
				continue
			}
			if emits := strings.TrimSpace(action.Emits); emits != "" && !eventExists(c.source, emits) {
				c.transitionRefFindings = append(c.transitionRefFindings, Finding{
					CheckID:  "transition_reference_validation",
					Severity: "error",
					Message:  fmt.Sprintf("transition %s action %s emits missing event %s", id, actionID, emits),
					Location: id,
				})
			}
		}
		for _, guardID := range transition.Guards {
			guardID = strings.TrimSpace(guardID)
			if guardID == "" {
				continue
			}
			if _, ok := c.source.GuardInstructionByID(guardID); !ok {
				c.transitionRefFindings = append(c.transitionRefFindings, Finding{
					CheckID:  "transition_reference_validation",
					Severity: "error",
					Message:  fmt.Sprintf("transition %s references unknown guard %s", id, guardID),
					Location: id,
				})
			}
		}
	}
	for flowID := range c.source.FlowSchemaEntries() {
		for _, eventType := range c.source.FlowInputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType != "" && !eventExists(c.source, eventType) {
				c.transitionRefFindings = append(c.transitionRefFindings, Finding{
					CheckID:  "transition_reference_validation",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s input event %s missing from event catalog", flowID, eventType),
					Location: strings.TrimSpace(flowID),
				})
			}
		}
		for _, eventType := range c.source.FlowOutputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType != "" && !eventExists(c.source, eventType) {
				c.transitionRefFindings = append(c.transitionRefFindings, Finding{
					CheckID:  "transition_reference_validation",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s output event %s missing from event catalog", flowID, eventType),
					Location: strings.TrimSpace(flowID),
				})
			}
		}
	}
	return c.transitionRefFindings
}

func (c *checkerContext) transitionOwnership() []Finding {
	if c.transitionOwnerLoaded {
		return c.transitionOwnerFindings
	}
	c.transitionOwnerLoaded = true
	transitions := c.source.WorkflowTransitions()
	transitionByID := make(map[string]runtimecontracts.WorkflowTransitionContract, len(transitions))
	for _, transition := range transitions {
		id := strings.TrimSpace(transition.ID)
		if id != "" {
			transitionByID[id] = transition
		}
	}
	usesOwningNodeModel := contractBundleUsesOwningNodeModel(c.source)
	for nodeID, node := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		subs := stringSet(c.source.NodeRuntimeSubscriptions(nodeID))
		produces := stringSet(semanticview.NodeEffectiveProduces(c.source, nodeID))
		for _, transitionID := range node.OwnedTransitions {
			transitionID = strings.TrimSpace(transitionID)
			if transitionID == "" {
				continue
			}
			transition, ok := transitionByID[transitionID]
			if !ok {
				c.transitionOwnerFindings = append(c.transitionOwnerFindings, Finding{
					CheckID:  "transition_ownership_validation",
					Severity: "error",
					Message:  fmt.Sprintf("node %s owns unknown transition %s", nodeID, transitionID),
					Location: nodeID,
				})
				continue
			}
			if owner := strings.TrimSpace(transition.Node); owner != nodeID {
				c.transitionOwnerFindings = append(c.transitionOwnerFindings, Finding{
					CheckID:  "transition_ownership_validation",
					Severity: "error",
					Message:  fmt.Sprintf("node %s owns transition %s but workflow owner is %s", nodeID, transitionID, owner),
					Location: nodeID,
				})
			}
			trigger := strings.TrimSpace(transition.Trigger)
			if trigger != "" && !usesOwningNodeModel {
				if _, ok := subs[trigger]; !ok {
					if _, emitted := produces[trigger]; !emitted {
						c.transitionOwnerFindings = append(c.transitionOwnerFindings, Finding{
							CheckID:  "transition_ownership_validation",
							Severity: "error",
							Message:  fmt.Sprintf("node %s cannot see trigger %s for owned transition %s", nodeID, trigger, transitionID),
							Location: nodeID,
						})
					}
				}
			}
		}
	}
	return c.transitionOwnerFindings
}

func (c *checkerContext) eventRuntimeWiring() []Finding {
	if c.eventRuntimeLoaded {
		return c.eventRuntimeFindings
	}
	c.eventRuntimeLoaded = true
	nodes := c.source.NodeEntries()
	for _, requirement := range runtimeHandledEventRequirements(c.source) {
		if requirement.owner == "" {
			c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
				CheckID:  "event_runtime_wiring_validation",
				Severity: "error",
				Message:  fmt.Sprintf("event %s with runtime_handling=%s missing owning_node", requirement.eventType, requirement.handling),
				Location: requirement.eventType,
			})
			continue
		}
		if _, ok := nodes[requirement.owner]; !ok {
			c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
				CheckID:  "event_runtime_wiring_validation",
				Severity: "error",
				Message:  fmt.Sprintf("event %s owning_node %s missing from system nodes", requirement.eventType, requirement.owner),
				Location: requirement.eventType,
			})
			continue
		}
		if handlers := c.source.NodeEventHandlers(requirement.owner); len(handlers) > 0 {
			if _, ok := c.source.NodeEventHandler(requirement.owner, requirement.eventType); !ok {
				c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
					CheckID:  "event_runtime_wiring_validation",
					Severity: "error",
					Message:  fmt.Sprintf("event %s owning_node %s missing semantic event_handler", requirement.eventType, requirement.owner),
					Location: requirement.eventType,
				})
			}
		}
	}
	return c.eventRuntimeFindings
}

type runtimeHandledEventRequirement struct {
	eventType string
	handling  string
	owner     string
}

func runtimeHandledEventRequirements(source semanticview.Source) []runtimeHandledEventRequirement {
	if source == nil || !contractBundleUsesOwningNodeModel(source) {
		return nil
	}
	out := make([]runtimeHandledEventRequirement, 0)
	for eventType, entry := range source.EventEntries() {
		eventType = strings.TrimSpace(eventType)
		handling := strings.TrimSpace(entry.RuntimeHandling)
		if eventType == "" || !requiresOwningNode(handling) {
			continue
		}
		out = append(out, runtimeHandledEventRequirement{
			eventType: eventType,
			handling:  handling,
			owner:     strings.TrimSpace(entry.OwningNode),
		})
	}
	return out
}

func runtimeHandledEventsMissingExecutors(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	runtimeExecutors := supportedWorkflowRuntimeExecutorIDs(source)
	nodes := source.NodeEntries()
	out := make([]Finding, 0)
	for _, requirement := range runtimeHandledEventRequirements(source) {
		if requirement.owner == "" {
			continue
		}
		if _, ok := nodes[requirement.owner]; !ok {
			continue
		}
		if _, ok := runtimeExecutors[requirement.owner]; ok {
			continue
		}
		out = append(out, Finding{
			CheckID:  "handler_field_compliance",
			Severity: "error",
			Message:  fmt.Sprintf("event %s owning_node %s has no runtime executor", requirement.eventType, requirement.owner),
			Location: requirement.eventType,
		})
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
