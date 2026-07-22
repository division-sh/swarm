package bus

import (
	"context"
	"strings"

	"github.com/division-sh/swarm/internal/events"
)

// ConvergeDeliveryRunCompletion evaluates both standalone platform-run and
// normal run completion after one exact delivery settlement.
func (eb *EventBus) ConvergeDeliveryRunCompletion(ctx context.Context, evt events.Event) error {
	return eb.convergeStandaloneRuntimePlatformRun(ctx, evt)
}

func (eb *EventBus) ConvergeNormalRunCompletionForEvent(ctx context.Context, eventID string) error {
	if eb == nil || eb.store == nil {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	converger, ok := eb.store.(NormalRunCompletionConvergencePersistence)
	if !ok || converger == nil {
		return nil
	}
	workflowTerminals, flowTerminals := eb.normalRunCompletionTerminalStates()
	return converger.ConvergeNormalRunCompletion(ctx, eventID, workflowTerminals, flowTerminals)
}

func (eb *EventBus) normalRunCompletionTerminalStates() ([]string, map[string][]string) {
	if eb == nil {
		return nil, nil
	}
	eb.mu.RLock()
	source := eb.semanticSource
	eb.mu.RUnlock()
	if source == nil {
		return nil, nil
	}
	workflowTerminals := normalizeRunCompletionStates(source.FlowTerminalStages(""))
	flowTerminals := map[string][]string{}
	addFlowTerminals := func(key string, states []string) {
		key = strings.Trim(strings.TrimSpace(key), "/")
		states = normalizeRunCompletionStates(states)
		if key == "" || len(states) == 0 {
			return
		}
		flowTerminals[key] = states
	}
	if workflowName := strings.Trim(strings.TrimSpace(source.WorkflowName()), "/"); workflowName != "" {
		addFlowTerminals(workflowName, workflowTerminals)
	}
	for flowID := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		states := source.FlowTerminalStages(flowID)
		addFlowTerminals(flowID, states)
		addFlowTerminals(source.FlowPath(flowID), states)
	}
	for _, scope := range source.FlowScopes() {
		states := source.FlowTerminalStages(scope.ID)
		addFlowTerminals(scope.ID, states)
		addFlowTerminals(scope.Path, states)
		addFlowTerminals(scope.OwningFlowID, states)
	}
	if len(flowTerminals) == 0 {
		flowTerminals = nil
	}
	return workflowTerminals, flowTerminals
}

func normalizeRunCompletionStates(states []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(states))
	for _, state := range states {
		state = strings.TrimSpace(strings.ToLower(state))
		if state == "" {
			continue
		}
		if _, ok := seen[state]; ok {
			continue
		}
		seen[state] = struct{}{}
		out = append(out, state)
	}
	return out
}
