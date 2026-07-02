package contracts

import (
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

func EffectiveSystemNodeID(nodeKey string, node SystemNodeContract) string {
	if nodeKey := strings.TrimSpace(nodeKey); nodeKey != "" {
		return nodeKey
	}
	return strings.TrimSpace(node.ID)
}

func SystemNodeIDMatchesKey(nodeKey, authoredID string) bool {
	nodeKey = strings.TrimSpace(nodeKey)
	authoredID = strings.TrimSpace(authoredID)
	if authoredID == "" || authoredID == nodeKey {
		return true
	}
	return systemNodeRenderedIDTemplateMatchesKey(nodeKey, authoredID)
}

func systemNodeRenderedIDTemplateMatchesKey(nodeKey, authoredID string) bool {
	if nodeKey == "" || !strings.Contains(authoredID, "{") || !strings.Contains(authoredID, "}") {
		return false
	}
	prefix, _, _ := strings.Cut(authoredID, "{")
	prefix = strings.TrimSuffix(strings.TrimSpace(prefix), "-")
	return prefix == nodeKey
}

func EffectiveSystemNodeExecutionType(node SystemNodeContract) string {
	if executionType := strings.TrimSpace(node.ExecutionType); executionType != "" {
		return executionType
	}
	return SystemNodeExecutionType
}

func EffectiveSystemNodeSubscriptions(node SystemNodeContract) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(node.SubscribesTo)+len(node.EventHandlers))
	appendSubscription := func(value string) {
		value = eventidentity.Normalize(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, eventType := range node.SubscribesTo {
		appendSubscription(eventType)
	}
	for eventType := range node.EventHandlers {
		appendSubscription(eventType)
	}
	sort.Strings(out)
	return out
}

func EffectiveSystemNodeProduces(node SystemNodeContract) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(node.EventHandlers))
	for _, eventType := range sortedContractKeys(node.EventHandlers) {
		for _, emitted := range HandlerEmitEvents(node.EventHandlers[eventType]) {
			emitted = strings.TrimSpace(emitted)
			if emitted == "" {
				continue
			}
			if _, ok := seen[emitted]; ok {
				continue
			}
			seen[emitted] = struct{}{}
			out = append(out, emitted)
		}
	}
	sort.Strings(out)
	return out
}

func DefaultSystemNodeHandlerSourceEvent(handler SystemNodeEventHandler, triggerEvent string) SystemNodeEventHandler {
	triggerEvent = strings.TrimSpace(triggerEvent)
	if triggerEvent == "" {
		return handler
	}
	handler.DataAccumulation = defaultWorkflowDataAccumulationSourceEvent(handler.DataAccumulation, triggerEvent)
	if len(handler.Rules) > 0 {
		handler.Rules = append([]HandlerRuleEntry(nil), handler.Rules...)
		for i := range handler.Rules {
			handler.Rules[i].DataAccumulation = defaultWorkflowDataAccumulationSourceEvent(handler.Rules[i].DataAccumulation, triggerEvent)
		}
	}
	if len(handler.OnComplete) > 0 {
		handler.OnComplete = append([]HandlerRuleEntry(nil), handler.OnComplete...)
		for i := range handler.OnComplete {
			handler.OnComplete[i].DataAccumulation = defaultWorkflowDataAccumulationSourceEvent(handler.OnComplete[i].DataAccumulation, triggerEvent)
		}
	}
	if handler.Accumulate != nil {
		accumulate := *handler.Accumulate
		if len(accumulate.OnComplete) > 0 {
			accumulate.OnComplete = append([]HandlerRuleEntry(nil), accumulate.OnComplete...)
			for i := range accumulate.OnComplete {
				accumulate.OnComplete[i].DataAccumulation = defaultWorkflowDataAccumulationSourceEvent(accumulate.OnComplete[i].DataAccumulation, triggerEvent)
			}
		}
		if accumulate.OnTimeout != nil {
			timeout := *accumulate.OnTimeout
			timeout.DataAccumulation = defaultWorkflowDataAccumulationSourceEvent(timeout.DataAccumulation, triggerEvent)
			accumulate.OnTimeout = &timeout
		}
		handler.Accumulate = &accumulate
	}
	if len(handler.Branch) > 0 {
		handler.Branch = append([]BranchSpec(nil), handler.Branch...)
		for i := range handler.Branch {
			if handler.Branch[i].Then != nil {
				then := *handler.Branch[i].Then
				then.DataAccumulation = defaultWorkflowDataAccumulationSourceEvent(then.DataAccumulation, triggerEvent)
				handler.Branch[i].Then = &then
			}
			if handler.Branch[i].Else != nil {
				otherwise := *handler.Branch[i].Else
				otherwise.DataAccumulation = defaultWorkflowDataAccumulationSourceEvent(otherwise.DataAccumulation, triggerEvent)
				handler.Branch[i].Else = &otherwise
			}
		}
	}
	return handler
}

func defaultWorkflowDataAccumulationSourceEvent(accumulation WorkflowDataAccumulation, triggerEvent string) WorkflowDataAccumulation {
	if accumulation.HasWrites() && strings.TrimSpace(accumulation.SourceEvent) == "" {
		accumulation.SourceEvent = triggerEvent
	}
	return accumulation
}
