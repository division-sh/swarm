package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkSemanticDriftUnreachableState(c *checkerContext) []Finding {
	return c.stateReachability()
}

func (c *checkerContext) stateReachability() []Finding {
	if c.stateReachabilityLoaded {
		return c.stateReachabilityFindings
	}
	c.stateReachabilityLoaded = true

	for _, entry := range lifecycleFlowSchemas(c.source) {
		flowID := strings.TrimSpace(entry.flowID)
		usesStages := flowUsesAuthoredStages(c.source, flowID)
		if flowID == "" && !usesStages {
			continue
		}
		initial := strings.TrimSpace(c.source.FlowInitialStage(flowID))
		if initial == "" {
			continue
		}
		declaredStates := declaredStatesForFlow(c.source, flowID)
		if len(declaredStates) == 0 {
			continue
		}
		if _, ok := declaredStates[initial]; !ok {
			continue
		}

		reachable := authoredReachableStates(c.source, flowID, initial, declaredStates)
		unreachable := make(map[string]struct{}, len(declaredStates))
		for state := range declaredStates {
			if _, ok := reachable[state]; ok {
				continue
			}
			unreachable[state] = struct{}{}
		}
		if len(unreachable) == 0 {
			continue
		}

		reachableList := strings.Join(sortedSetKeys(reachable), ", ")
		unreachableList := strings.Join(sortedSetKeys(unreachable), ", ")
		declaredNoun := "state"
		ownerField := "states"
		initialField := "initial_state"
		if usesStages {
			declaredNoun = "stage"
			ownerField = "stages"
			initialField = "initial stage"
		}
		for _, state := range sortedSetKeys(unreachable) {
			message := fmt.Sprintf(
				"flow %s declares %s %s but no transition path from %s %s reaches %s in the authored handler graph.\n\nReachable states: %s\nUnreachable states: %s",
				validationFlowLabel(flowID),
				declaredNoun,
				state,
				initialField,
				initial,
				state,
				reachableList,
				unreachableList,
			)
			remediation := fmt.Sprintf(
				"If %s is intentionally unused, remove it from schema.yaml %s. If %s should be reachable, add a handler transition carrier that reaches %s.",
				state,
				ownerField,
				state,
				state,
			)
			if usesStages {
				c.stateReachabilityFindings = append(c.stateReachabilityFindings, NewHardInvalidityFinding(
					"semantic_drift_unreachable_state",
					validationFlowLabel(flowID),
					message,
					remediation,
				))
				continue
			}
			c.stateReachabilityFindings = append(c.stateReachabilityFindings, Finding{
				CheckID:  "semantic_drift_unreachable_state",
				Severity: SeveritySemanticDriftWarn,
				Message:  message + "\n\n" + remediation,
				Location: validationFlowLabel(flowID),
			})
		}
	}

	return c.stateReachabilityFindings
}

func authoredReachableStates(source semanticview.Source, flowID, initial string, declaredStates map[string]struct{}) map[string]struct{} {
	flowID = strings.TrimSpace(flowID)
	initial = strings.TrimSpace(initial)

	reachable := map[string]struct{}{initial: {}}
	edges := authoredStateGraphEdges(source, flowID, initial, declaredStates)
	queue := []string{initial}
	for len(queue) > 0 {
		state := strings.TrimSpace(queue[0])
		queue = queue[1:]
		for next := range edges[state] {
			if _, ok := reachable[next]; ok {
				continue
			}
			reachable[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	return reachable
}

func authoredStateGraphEdges(source semanticview.Source, flowID, initial string, declaredStates map[string]struct{}) map[string]map[string]struct{} {
	return authoredStateGraphEdgesFiltered(source, flowID, initial, declaredStates, nil)
}

func authoredStateGraphEdgesFiltered(
	source semanticview.Source,
	flowID, initial string,
	declaredStates map[string]struct{},
	includeHandler func(nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) bool,
) map[string]map[string]struct{} {
	edges := make(map[string]map[string]struct{}, len(declaredStates))
	nonTerminalStates := authoredNonTerminalStates(source, flowID, declaredStates)
	for nodeID, node := range source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" || strings.TrimSpace(nodeFlowID(source, nodeID)) != flowID {
			continue
		}
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if includeHandler != nil && !includeHandler(nodeID, eventType, handler) {
				continue
			}
			sources := authoredHandlerSourceStates(initial, nonTerminalStates, handler)
			for _, target := range authoredReachabilityTargets(handler) {
				target = strings.TrimSpace(target)
				if target == "" {
					continue
				}
				if _, ok := declaredStates[target]; !ok {
					continue
				}
				for _, sourceState := range sources {
					sourceState = strings.TrimSpace(sourceState)
					if sourceState == "" {
						continue
					}
					if edges[sourceState] == nil {
						edges[sourceState] = map[string]struct{}{}
					}
					edges[sourceState][target] = struct{}{}
				}
			}
		}
	}
	return edges
}

func authoredReachabilityTargets(handler runtimecontracts.SystemNodeEventHandler) []string {
	return runtimecontracts.HandlerAdvanceTargets(handler)
}

func authoredNonTerminalStates(source semanticview.Source, flowID string, declaredStates map[string]struct{}) []string {
	terminalStates := stringSet(source.FlowTerminalStages(flowID))
	out := make([]string, 0, len(declaredStates))
	for _, state := range sortedSetKeys(declaredStates) {
		if _, ok := terminalStates[state]; ok {
			continue
		}
		out = append(out, state)
	}
	return out
}

func authoredHandlerSourceStates(initial string, nonTerminalStates []string, handler runtimecontracts.SystemNodeEventHandler) []string {
	if handler.CreateEntity {
		return []string{strings.TrimSpace(initial)}
	}
	return append([]string{}, nonTerminalStates...)
}
