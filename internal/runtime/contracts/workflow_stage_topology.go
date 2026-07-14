package contracts

import (
	"sort"
	"strings"
)

// BuildWorkflowStageTopology lowers every lifecycle transition carrier through
// one graph owner. Callers provide timers already scoped to the requested flow.
func BuildWorkflowStageTopology(
	flowID, initial string,
	stages, terminal []string,
	transitions []HandlerTransitionSemantic,
	timers []WorkflowTimerContract,
	loops []WorkflowLoopPlan,
	gates ...[]WorkflowGatePlan,
) WorkflowStageTopology {
	flowID = strings.TrimSpace(flowID)
	initial = strings.TrimSpace(initial)
	stageSet := normalizedStringSet(stages)
	terminalSet := normalizedStringSet(terminal)
	nonTerminal := make([]string, 0, len(stageSet))
	for _, stage := range sortedStringSet(stageSet) {
		if _, terminal := terminalSet[stage]; !terminal {
			nonTerminal = append(nonTerminal, stage)
		}
	}
	topology := WorkflowStageTopology{
		FlowID:         flowID,
		InitialStage:   initial,
		Stages:         sortedStringSet(stageSet),
		TerminalStages: sortedStringSet(terminalSet),
	}
	for _, transition := range transitions {
		if strings.TrimSpace(transition.FlowID) != flowID {
			continue
		}
		handlerStages := append([]string{}, nonTerminal...)
		loopKind, loopID := LoopOperationKind(""), ""
		if transition.Loop != nil {
			if kind, id, err := transition.Loop.Operation(); err == nil {
				loopKind, loopID = kind, strings.TrimSpace(id)
				handlerStages = []string{strings.TrimSpace(transition.Loop.From)}
			}
		} else if transition.CreateEntity && initial != "" {
			handlerStages = []string{initial}
		}
		topology.Handlers = append(topology.Handlers, WorkflowHandlerStageScope{
			NodeID:    strings.TrimSpace(transition.NodeID),
			EventType: strings.TrimSpace(transition.EventType),
			Stages:    normalizedStrings(handlerStages),
		})
		for _, carrier := range HandlerTransitionAdvanceCarriers(transition) {
			from := handlerStages
			timed := false
			eventType := strings.TrimSpace(transition.EventType)
			edgeSource := carrier.Source()
			if loopKind != "" {
				edgeSource = "loop." + string(loopKind)
			}
			if transition.Loop == nil && transition.Join != nil {
				switch carrier.Kind {
				case HandlerAdvanceCarrierJoinOnComplete:
					from = []string{strings.TrimSpace(transition.Join.Stage)}
				case HandlerAdvanceCarrierJoinTimeout:
					from = []string{strings.TrimSpace(transition.Join.Stage)}
					eventType = "platform.join_timeout"
					timed = true
				}
			}
			for _, sourceStage := range normalizedStrings(from) {
				topology.Edges = appendTopologyEdge(topology.Edges, stageSet, WorkflowStageTopologyEdge{
					From:          sourceStage,
					To:            strings.TrimSpace(carrier.AdvancesTo),
					Source:        edgeSource,
					NodeID:        strings.TrimSpace(transition.NodeID),
					HandlerEvent:  strings.TrimSpace(transition.EventType),
					EventType:     eventType,
					LoopID:        loopID,
					LoopOperation: loopKind,
					TimerID:       joinTimerID(transition, carrier),
					After:         joinTimerDelay(transition, carrier),
					Timed:         timed,
				})
			}
		}
	}
	for _, plan := range loops {
		if strings.TrimSpace(plan.FlowID) != flowID {
			continue
		}
		for _, operation := range plan.Operations {
			if operation.Kind != LoopOperationRepeat {
				continue
			}
			topology.Edges = appendTopologyEdge(topology.Edges, stageSet, WorkflowStageTopologyEdge{
				From:          strings.TrimSpace(operation.From),
				To:            strings.TrimSpace(plan.Escape.AdvancesTo),
				Source:        "loop.escape",
				NodeID:        strings.TrimSpace(operation.NodeID),
				HandlerEvent:  strings.TrimSpace(operation.HandlerEvent),
				EventType:     strings.TrimSpace(operation.HandlerEvent),
				LoopID:        strings.TrimSpace(plan.ID),
				LoopOperation: LoopOperationRepeat,
			})
		}
	}
	for _, timer := range timers {
		if !timer.StageOwned || strings.TrimSpace(timer.AdvancesTo) == "" {
			continue
		}
		topology.Edges = appendTopologyEdge(topology.Edges, stageSet, WorkflowStageTopologyEdge{
			From:      strings.TrimSpace(timer.Stage),
			To:        strings.TrimSpace(timer.AdvancesTo),
			Source:    "timer",
			NodeID:    "runtime",
			EventType: "timer:" + strings.TrimSpace(timer.ID),
			TimerID:   strings.TrimSpace(timer.ID),
			After:     strings.TrimSpace(timer.Delay),
			Timed:     true,
		})
	}
	if len(gates) > 0 {
		for _, gate := range gates[0] {
			if strings.TrimSpace(gate.FlowID) != flowID || strings.TrimSpace(gate.Stage) == "" {
				continue
			}
			for verdict, outcome := range gate.Outcomes {
				topology.Edges = appendTopologyEdge(topology.Edges, stageSet, WorkflowStageTopologyEdge{
					From: strings.TrimSpace(gate.Stage), To: strings.TrimSpace(outcome.AdvancesTo), Source: "gate",
					NodeID: "runtime", EventType: "mailbox.card_decided", DecisionID: strings.TrimSpace(gate.Decision), Verdict: strings.TrimSpace(verdict),
				})
			}
		}
	}
	sort.Slice(topology.Edges, func(i, j int) bool {
		left, right := topology.Edges[i], topology.Edges[j]
		return topologyEdgeSortKey(left) < topologyEdgeSortKey(right)
	})
	sort.Slice(topology.Handlers, func(i, j int) bool {
		left, right := topology.Handlers[i], topology.Handlers[j]
		return left.NodeID+"\x00"+left.EventType < right.NodeID+"\x00"+right.EventType
	})
	return topology
}

// StronglyConnectedComponent returns the exact component containing stage.
func (t WorkflowStageTopology) StronglyConnectedComponent(stage string) []string {
	stage = strings.TrimSpace(stage)
	if stage == "" {
		return nil
	}
	forward := topologyReachable(stage, t.Edges, false)
	reverse := topologyReachable(stage, t.Edges, true)
	component := make([]string, 0)
	for item := range forward {
		if _, ok := reverse[item]; ok {
			component = append(component, item)
		}
	}
	sort.Strings(component)
	return component
}

// StageCanReenter reports whether a stage can return to itself through the
// canonical lifecycle graph, including a direct self-loop.
func (t WorkflowStageTopology) StageCanReenter(stage string) bool {
	stage = strings.TrimSpace(stage)
	if stage == "" {
		return false
	}
	for _, edge := range t.Edges {
		if strings.TrimSpace(edge.From) == stage && strings.TrimSpace(edge.To) == stage {
			return true
		}
	}
	return len(t.StronglyConnectedComponent(stage)) > 1
}

func (t WorkflowStageTopology) HandlerStages(nodeID, eventType string) []string {
	nodeID, eventType = strings.TrimSpace(nodeID), strings.TrimSpace(eventType)
	for _, handler := range t.Handlers {
		if handler.NodeID == nodeID && handler.EventType == eventType {
			return append([]string{}, handler.Stages...)
		}
	}
	return nil
}

func (t WorkflowStageTopology) HandlerTargets(nodeID, handlerEvent string) []string {
	nodeID, handlerEvent = strings.TrimSpace(nodeID), strings.TrimSpace(handlerEvent)
	targets := map[string]struct{}{}
	for _, edge := range t.Edges {
		if strings.TrimSpace(edge.NodeID) != nodeID || strings.TrimSpace(edge.HandlerEvent) != handlerEvent {
			continue
		}
		if target := strings.TrimSpace(edge.To); target != "" {
			targets[target] = struct{}{}
		}
	}
	return sortedStringSet(targets)
}

func BindWorkflowLoopRegions(plans []WorkflowLoopPlan, topologies map[string]WorkflowStageTopology) []WorkflowLoopPlan {
	out := append([]WorkflowLoopPlan{}, plans...)
	for idx := range out {
		topology, ok := topologies[strings.TrimSpace(out[idx].FlowID)]
		if !ok {
			out[idx].RegionStages = nil
			continue
		}
		out[idx].RegionStages = topology.StronglyConnectedComponent(out[idx].EntryStage)
	}
	return out
}

func appendTopologyEdge(edges []WorkflowStageTopologyEdge, stages map[string]struct{}, edge WorkflowStageTopologyEdge) []WorkflowStageTopologyEdge {
	edge.From, edge.To = strings.TrimSpace(edge.From), strings.TrimSpace(edge.To)
	if edge.From == "" || edge.To == "" {
		return edges
	}
	if _, ok := stages[edge.From]; !ok {
		return edges
	}
	if _, ok := stages[edge.To]; !ok {
		return edges
	}
	key := topologyEdgeSortKey(edge)
	for _, existing := range edges {
		if topologyEdgeSortKey(existing) == key {
			return edges
		}
	}
	return append(edges, edge)
}

func topologyReachable(start string, edges []WorkflowStageTopologyEdge, reverse bool) map[string]struct{} {
	seen := map[string]struct{}{start: {}}
	queue := []string{start}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, edge := range edges {
			from, to := edge.From, edge.To
			if reverse {
				from, to = to, from
			}
			if from != current {
				continue
			}
			if _, ok := seen[to]; ok {
				continue
			}
			seen[to] = struct{}{}
			queue = append(queue, to)
		}
	}
	return seen
}

func joinTimerID(transition HandlerTransitionSemantic, carrier HandlerAdvanceCarrier) string {
	if transition.Join != nil && carrier.Kind == HandlerAdvanceCarrierJoinTimeout {
		return strings.TrimSpace(transition.Join.EffectiveID())
	}
	return ""
}

func joinTimerDelay(transition HandlerTransitionSemantic, carrier HandlerAdvanceCarrier) string {
	if transition.Join != nil && carrier.Kind == HandlerAdvanceCarrierJoinTimeout {
		return strings.TrimSpace(transition.Join.Timeout.After)
	}
	return ""
}

func topologyEdgeSortKey(edge WorkflowStageTopologyEdge) string {
	return strings.Join([]string{edge.From, edge.To, edge.Source, edge.NodeID, edge.HandlerEvent, edge.EventType, edge.LoopID, string(edge.LoopOperation), edge.TimerID, edge.After, edge.DecisionID, edge.Verdict}, "\x00")
}

func normalizedStringSet(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func sortedStringSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func normalizedStrings(values []string) []string {
	return sortedStringSet(normalizedStringSet(values))
}
