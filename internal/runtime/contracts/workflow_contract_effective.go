package contracts

import (
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
)

const (
	RequiredAgentSourceExplicit = "explicit"
	RequiredAgentSourceInferred = "inferred"
)

const (
	AgentFieldSourceAuthored        = "authored"
	AgentFieldSourcePlatformDefault = "platform_default"
	AgentFieldSourceDerived         = "derived"

	DefaultAgentType            = "generic"
	DefaultAgentMode            = "task"
	DefaultAgentMaxTurnsPerTask = 100
)

func EffectiveAgentRegistryEntries(entries map[string]AgentRegistryEntry) map[string]AgentRegistryEntry {
	if len(entries) == 0 {
		return map[string]AgentRegistryEntry{}
	}
	out := make(map[string]AgentRegistryEntry, len(entries))
	for key, entry := range entries {
		out[key] = EffectiveAgentRegistryEntry(key, entry)
	}
	return out
}

func EffectiveAgentRegistryEntry(logicalID string, entry AgentRegistryEntry) AgentRegistryEntry {
	_ = strings.TrimSpace(logicalID)
	effective := cloneAgentRegistryEntry(entry)
	effective.AuthoredFields = cloneBoolMap(entry.AuthoredFields)
	effective.EffectiveFieldSources = cloneStringMap(entry.EffectiveFieldSources)
	if effective.EffectiveFieldSources == nil {
		effective.EffectiveFieldSources = map[string]string{}
	}

	if strings.TrimSpace(effective.Type) == "" {
		effective.Type = DefaultAgentType
		effective.EffectiveFieldSources["type"] = AgentFieldSourcePlatformDefault
	} else {
		setAgentFieldSourceIfEmpty(effective.EffectiveFieldSources, "type", AgentFieldSourceAuthored)
	}

	modeValue := firstNonEmpty(effective.Mode, effective.ConversationMode)
	if modeValue == "" {
		modeValue = DefaultAgentMode
		effective.EffectiveFieldSources["mode"] = AgentFieldSourcePlatformDefault
	} else {
		setAgentFieldSourceIfEmpty(effective.EffectiveFieldSources, "mode", AgentFieldSourceAuthored)
	}
	mode, scope, err := runtimesessions.ResolveAuthoredAgentMemoryMode(modeValue)
	if err == nil {
		effective.Mode = mode.String()
		effective.ConversationMode = mode.String()
		effective.SessionScope = scope.String()
	}
	if strings.TrimSpace(effective.SessionScope) != "" {
		setAgentFieldSourceIfEmpty(effective.EffectiveFieldSources, "session_scope", AgentFieldSourceDerived)
	}

	if effective.MaxTurnsPerTask <= 0 {
		effective.MaxTurnsPerTask = DefaultAgentMaxTurnsPerTask
		effective.EffectiveFieldSources["max_turns_per_task"] = AgentFieldSourcePlatformDefault
	} else {
		setAgentFieldSourceIfEmpty(effective.EffectiveFieldSources, "max_turns_per_task", AgentFieldSourceAuthored)
	}

	if _, ok := effective.EffectiveFieldSources["workspace_class"]; !ok {
		if effective.AuthoredFields["workspace_class"] {
			effective.EffectiveFieldSources["workspace_class"] = AgentFieldSourceAuthored
		} else {
			effective.EffectiveFieldSources["workspace_class"] = AgentFieldSourcePlatformDefault
		}
	}
	if strings.TrimSpace(effective.Model) != "" || effective.AuthoredFields["model"] {
		setAgentFieldSourceIfEmpty(effective.EffectiveFieldSources, "model", AgentFieldSourceAuthored)
	}
	if strings.TrimSpace(effective.ManagerFallback) != "" || effective.AuthoredFields["manager_fallback"] {
		setAgentFieldSourceIfEmpty(effective.EffectiveFieldSources, "manager_fallback", AgentFieldSourceAuthored)
	}

	return effective
}

func (e AgentRegistryEntry) EffectiveSourceForField(field string) string {
	if len(e.EffectiveFieldSources) == 0 {
		return ""
	}
	return strings.TrimSpace(e.EffectiveFieldSources[strings.TrimSpace(field)])
}

func setAgentFieldSourceIfEmpty(sources map[string]string, field, source string) {
	field = strings.TrimSpace(field)
	if field == "" || strings.TrimSpace(sources[field]) != "" {
		return
	}
	sources[field] = strings.TrimSpace(source)
}

func cloneBoolMap(in map[string]bool) map[string]bool {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]bool, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func EffectiveSystemNodeID(nodeKey string, node SystemNodeContract) string {
	if nodeKey := strings.TrimSpace(nodeKey); nodeKey != "" {
		return nodeKey
	}
	return strings.TrimSpace(node.ID)
}

func RequiredAgentsDeclared(schema FlowSchemaDocument) bool {
	return schema.RequiredAgentsDeclared || len(schema.RequiredAgents) > 0
}

func EffectiveRequiredAgentFacts(schema FlowSchemaDocument, agents map[string]AgentRegistryEntry, schemaFile, agentsFile string) []RequiredAgentFact {
	if RequiredAgentsDeclared(schema) {
		return explicitRequiredAgentFacts(schema.RequiredAgents, schemaFile)
	}
	return inferredRequiredAgentFacts(agents, agentsFile)
}

func FlowRequiredAgentsFromFacts(facts []RequiredAgentFact) []FlowRequiredAgent {
	if len(facts) == 0 {
		return nil
	}
	out := make([]FlowRequiredAgent, 0, len(facts))
	for _, fact := range facts {
		out = append(out, FlowRequiredAgent{
			Role:         strings.TrimSpace(fact.Role),
			SubscribesTo: normalizeStrings(fact.SubscribesTo),
			Emits:        normalizeStrings(fact.Emits),
			Description:  strings.TrimSpace(fact.Description),
		})
	}
	return out
}

func cloneRequiredAgentFacts(facts []RequiredAgentFact) []RequiredAgentFact {
	if len(facts) == 0 {
		return nil
	}
	out := make([]RequiredAgentFact, len(facts))
	for i, fact := range facts {
		out[i] = RequiredAgentFact{
			Role:         strings.TrimSpace(fact.Role),
			SubscribesTo: normalizeStrings(fact.SubscribesTo),
			Emits:        normalizeStrings(fact.Emits),
			Description:  strings.TrimSpace(fact.Description),
			Source:       strings.TrimSpace(fact.Source),
			SourceFile:   strings.TrimSpace(fact.SourceFile),
		}
	}
	return out
}

func explicitRequiredAgentFacts(required []FlowRequiredAgent, sourceFile string) []RequiredAgentFact {
	if len(required) == 0 {
		return nil
	}
	sourceFile = strings.TrimSpace(sourceFile)
	out := make([]RequiredAgentFact, 0, len(required))
	for _, requiredAgent := range required {
		out = append(out, RequiredAgentFact{
			Role:         strings.TrimSpace(requiredAgent.Role),
			SubscribesTo: normalizeStrings(requiredAgent.SubscribesTo),
			Emits:        normalizeStrings(requiredAgent.Emits),
			Description:  strings.TrimSpace(requiredAgent.Description),
			Source:       RequiredAgentSourceExplicit,
			SourceFile:   sourceFile,
		})
	}
	return out
}

func inferredRequiredAgentFacts(agents map[string]AgentRegistryEntry, sourceFile string) []RequiredAgentFact {
	if len(agents) == 0 {
		return nil
	}
	sourceFile = strings.TrimSpace(sourceFile)
	out := make([]RequiredAgentFact, 0, len(agents))
	for _, agentID := range sortedContractKeys(agents) {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		agent := agents[agentID]
		out = append(out, RequiredAgentFact{
			Role:         agentID,
			SubscribesTo: normalizeStrings(agent.Subscriptions),
			Emits:        normalizeStrings(agent.EmitEvents),
			Source:       RequiredAgentSourceInferred,
			SourceFile:   sourceFile,
		})
	}
	return out
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
	return handler
}

func defaultWorkflowDataAccumulationSourceEvent(accumulation WorkflowDataAccumulation, triggerEvent string) WorkflowDataAccumulation {
	if accumulation.HasWrites() && strings.TrimSpace(accumulation.SourceEvent) == "" {
		accumulation.SourceEvent = triggerEvent
	}
	return accumulation
}
