package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
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
		} else if transitionTriggerIsTimerReference(c.source, transition) {
			// Timer-triggered transitions are derived from stage timer rows and are
			// not event catalog entries. The timer owner is validated separately.
		} else if !flowEventExists(c.source, transitionOwningFlowID(transition), strings.TrimSpace(transition.Trigger)) {
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
			if emits := strings.TrimSpace(action.Emits); emits != "" && !flowEventExists(c.source, transitionOwningFlowID(transition), emits) {
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
			if eventType != "" && !flowEventExists(c.source, flowID, eventType) {
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
			if eventType != "" && !flowEventExists(c.source, flowID, eventType) {
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

func flowEventExists(source semanticview.Source, flowID, eventType string) bool {
	if proof := semanticview.ResolveFlowEventProof(source, strings.TrimSpace(flowID), strings.TrimSpace(eventType)); proof.HasSchema {
		return true
	}
	return eventExists(source, eventType)
}

func transitionOwningFlowID(transition runtimecontracts.WorkflowTransitionContract) string {
	if flowID := strings.TrimSpace(transition.FlowID); flowID != "" {
		return flowID
	}
	if transition.ExecutableNode.Valid() {
		return transition.ExecutableNode.FlowPath()
	}
	return ""
}

func transitionTriggerIsTimerReference(source semanticview.Source, transition runtimecontracts.WorkflowTransitionContract) bool {
	trigger := strings.TrimSpace(transition.Trigger)
	if !strings.HasPrefix(trigger, "timer:") {
		return false
	}
	timerID := strings.TrimSpace(strings.TrimPrefix(trigger, "timer:"))
	if timerID == "" || source == nil {
		return false
	}
	timer, ok := source.WorkflowStageTimerByID(transitionOwningFlowID(transition), timerID)
	return ok && timer.StageOwned
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
	consumerEventsByNode := map[string]map[string]struct{}{}
	producerEventsByNode := map[string]map[string]struct{}{}
	census := semanticview.BuildAuthoredEventEndpointCensus(c.source)
	for _, endpoint := range census.Consumers() {
		if endpoint.Node.Valid() {
			addTransitionEndpointEvent(consumerEventsByNode, endpoint.Node.Key(), endpoint.Event)
		}
	}
	for _, endpoint := range census.Producers() {
		if endpoint.Node.Valid() {
			addTransitionEndpointEvent(producerEventsByNode, endpoint.Node.Key(), endpoint.Event)
		}
	}
	for _, record := range c.source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		nodeID := node.Key()
		nodeLabel := executableNodeDiagnostic(node)
		subs := consumerEventsByNode[nodeID]
		produces := producerEventsByNode[nodeID]
		for _, transitionID := range record.Entry.OwnedTransitions {
			transitionID = strings.TrimSpace(transitionID)
			if transitionID == "" {
				continue
			}
			transition, ok := transitionByID[transitionID]
			if !ok {
				c.transitionOwnerFindings = append(c.transitionOwnerFindings, Finding{
					CheckID:  "transition_ownership_validation",
					Severity: "error",
					Message:  fmt.Sprintf("%s owns unknown transition %s", nodeLabel, transitionID),
					Location: nodeID,
				})
				continue
			}
			if owner := transition.ExecutableNode; !owner.Equal(node) {
				c.transitionOwnerFindings = append(c.transitionOwnerFindings, Finding{
					CheckID:  "transition_ownership_validation",
					Severity: "error",
					Message:  fmt.Sprintf("%s owns transition %s but workflow owner is %s", nodeLabel, transitionID, executableNodeDiagnostic(owner)),
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

func addTransitionEndpointEvent(byNode map[string]map[string]struct{}, nodeID string, proof semanticview.FlowEventProof) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return
	}
	if byNode[nodeID] == nil {
		byNode[nodeID] = map[string]struct{}{}
	}
	for _, value := range []string{proof.Authored, proof.Local, proof.Canonical, proof.CatalogKey} {
		if value = strings.TrimSpace(value); value != "" {
			byNode[nodeID][value] = struct{}{}
		}
	}
}

func (c *checkerContext) eventRuntimeWiring() []Finding {
	if c.eventRuntimeLoaded {
		return c.eventRuntimeFindings
	}
	c.eventRuntimeLoaded = true
	census := semanticview.BuildAuthoredEventEndpointCensus(c.source)
	for _, requirement := range runtimeHandledEventRequirements(c.source) {
		if !requirement.owner.Valid() {
			c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
				CheckID:  "event_runtime_wiring_validation",
				Severity: "error",
				Message:  fmt.Sprintf("event %s with runtime_handling=%s missing exact owning_node", requirement.eventType, requirement.handling),
				Location: requirement.eventType,
			})
			continue
		}
		if _, ok := c.source.ExecutableNode(requirement.owner); !ok {
			c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
				CheckID:  "event_runtime_wiring_validation",
				Severity: "error",
				Message:  fmt.Sprintf("event %s owning_node %s missing from system nodes", requirement.eventType, executableNodeDiagnostic(requirement.owner)),
				Location: requirement.eventType,
			})
			continue
		}
		if handlers := c.source.ExecutableNodeEventHandlers(requirement.owner); len(handlers) > 0 {
			matched := false
			for _, endpoint := range census.MatchingConsumers(requirement.owner.FlowPath(), requirement.eventType) {
				if endpoint.Kind == semanticview.EventEndpointNodeHandler && endpoint.Node.Equal(requirement.owner) {
					matched = true
					break
				}
			}
			if !matched {
				c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
					CheckID:  "event_runtime_wiring_validation",
					Severity: "error",
					Message:  fmt.Sprintf("event %s owning_node %s missing semantic event_handler", requirement.eventType, executableNodeDiagnostic(requirement.owner)),
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
	owner     runtimeidentity.ExecutableNode
}

func runtimeHandledEventRequirements(source semanticview.Source) []runtimeHandledEventRequirement {
	if source == nil || !contractBundleUsesOwningNodeModel(source) {
		return nil
	}
	out := make([]runtimeHandledEventRequirement, 0)
	appendEntries := func(flowID string, entries map[string]runtimecontracts.EventCatalogEntry) {
		for eventType, entry := range entries {
			eventType = strings.TrimSpace(eventType)
			handling := strings.TrimSpace(entry.RuntimeHandling)
			if eventType == "" || !requiresOwningNode(handling) {
				continue
			}
			owner, _ := runtimeidentity.AdmitExecutableNodeDeclaration(flowID, entry.OwningNode)
			out = append(out, runtimeHandledEventRequirement{
				eventType: eventType,
				handling:  handling,
				owner:     owner,
			})
		}
	}
	for _, scope := range source.FlowScopes() {
		appendEntries(scope.ID, scope.Events)
	}
	return out
}

func runtimeHandledEventsMissingExecutors(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	runtimeExecutors := supportedWorkflowRuntimeExecutorIDs(source)
	out := make([]Finding, 0)
	for _, requirement := range runtimeHandledEventRequirements(source) {
		if !requirement.owner.Valid() {
			continue
		}
		if _, ok := source.ExecutableNode(requirement.owner); !ok {
			continue
		}
		if _, ok := runtimeExecutors[requirement.owner.Key()]; ok {
			continue
		}
		out = append(out, Finding{
			CheckID:  "handler_field_compliance",
			Severity: "error",
			Message:  fmt.Sprintf("event %s owning_node %s has no runtime executor", requirement.eventType, executableNodeDiagnostic(requirement.owner)),
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
	for _, scope := range source.FlowScopes() {
		for _, entry := range scope.Events {
			if strings.TrimSpace(entry.OwningNode) != "" {
				return true
			}
		}
	}
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err == nil && len(source.ExecutableNodeEventHandlers(node)) > 0 {
			return true
		}
	}
	return false
}
