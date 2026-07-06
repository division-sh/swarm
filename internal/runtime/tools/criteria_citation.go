package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (e *Executor) validateEmitCriteriaCitations(actor models.AgentConfig, eventType string, schema EmitSchema, payload map[string]any) error {
	if len(schema.CitationFields) == 0 {
		return nil
	}
	if e == nil || e.workflowSource == nil {
		return NewRuntimeError(
			"criteria_citation_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_criteria_citations",
			false,
			"emit %s declares criteria citations but no workflow source is available",
			strings.TrimSpace(eventType),
		)
	}
	flowID, refs := criteriaRefsForActor(e.workflowSource, actor)
	declared := criteriaStringSet(refs)
	policy := e.workflowSource.ResolvedPolicyForFlow(flowID)
	for _, fieldName := range sortedCriteriaCitationFields(schema.CitationFields) {
		citation := schema.CitationFields[fieldName]
		value, exists := payload[fieldName]
		if !exists || value == nil {
			continue
		}
		setName := strings.TrimSpace(citation.Criteria)
		if setName == "" {
			return criteriaCitationRuntimeError(eventType, fieldName, "citation criteria set is not declared")
		}
		if _, ok := declared[setName]; !ok {
			return criteriaCitationRuntimeError(eventType, fieldName, "agent %s does not declare criteria set %q", actorLabel(actor), setName)
		}
		set, ok := policy.Criteria[setName]
		if !ok {
			return criteriaCitationRuntimeError(eventType, fieldName, "criteria set %q does not resolve for flow %s", setName, flowID)
		}
		ids, err := criteriaCitationIDs(value)
		if err != nil {
			return criteriaCitationRuntimeError(eventType, fieldName, "%v", err)
		}
		rules := criteriaRulesByID(set)
		allowed := criteriaStringSet(citation.AllowedClasses)
		validIDs := sortedCriteriaRuleIDs(rules)
		for _, id := range ids {
			rule, ok := rules[id]
			if !ok {
				return criteriaCitationRuntimeError(eventType, fieldName, "unknown criteria id %q for set %q; valid ids: %s", id, setName, strings.Join(validIDs, ", "))
			}
			className := strings.TrimSpace(rule.Class)
			if _, ok := allowed[className]; !ok {
				return criteriaCitationRuntimeError(eventType, fieldName, "criteria id %q has class %q, not one of allowed classes: %s", id, className, strings.Join(sortedStringSet(allowed), ", "))
			}
		}
	}
	return nil
}

func criteriaRefsForActor(source semanticview.Source, actor models.AgentConfig) (string, []string) {
	flowID := strings.TrimSpace(actor.Mode)
	if source == nil {
		return flowID, UniqueNonEmpty(actor.Criteria)
	}
	entry, contractFlowID, ok := criteriaAgentContractDeclaration(source, actor)
	if !ok {
		return flowID, nil
	}
	return contractFlowID, UniqueNonEmpty(entry.Criteria)
}

func criteriaAgentContractDeclaration(source semanticview.Source, actor models.AgentConfig) (runtimecontracts.AgentRegistryEntry, string, bool) {
	actorID := strings.TrimSpace(actor.ID)
	if source == nil || actorID == "" {
		return runtimecontracts.AgentRegistryEntry{}, "", false
	}
	type match struct {
		entry  runtimecontracts.AgentRegistryEntry
		flowID string
	}
	matches := []match{}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" {
			continue
		}
		for _, logicalID := range sortedAgentIDs(scope.Agents) {
			entry := scope.Agents[logicalID]
			if !criteriaAgentIdentityMatches(logicalID, entry.ID, actorID) {
				continue
			}
			matches = append(matches, match{entry: entry, flowID: flowID})
		}
	}
	if len(matches) != 1 {
		return runtimecontracts.AgentRegistryEntry{}, "", false
	}
	return matches[0].entry, matches[0].flowID, true
}

func criteriaAgentIdentityMatches(logicalID, declaredID, actorID string) bool {
	logicalID = strings.TrimSpace(logicalID)
	declaredID = strings.TrimSpace(declaredID)
	actorID = strings.TrimSpace(actorID)
	if actorID == "" {
		return false
	}
	return logicalID == actorID || declaredID == actorID
}

func criteriaCitationIDs(value any) ([]string, error) {
	switch typed := value.(type) {
	case string:
		id := strings.TrimSpace(typed)
		if id == "" {
			return nil, fmt.Errorf("criteria citation id must not be empty")
		}
		return []string{id}, nil
	case []any:
		if len(typed) == 0 {
			return nil, fmt.Errorf("criteria citation list must not be empty")
		}
		out := make([]string, 0, len(typed))
		for i, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("criteria citation item %d must be string", i)
			}
			text = strings.TrimSpace(text)
			if text == "" {
				return nil, fmt.Errorf("criteria citation item %d must not be empty", i)
			}
			out = append(out, text)
		}
		return out, nil
	case []string:
		if len(typed) == 0 {
			return nil, fmt.Errorf("criteria citation list must not be empty")
		}
		out := make([]string, 0, len(typed))
		for i, text := range typed {
			text = strings.TrimSpace(text)
			if text == "" {
				return nil, fmt.Errorf("criteria citation item %d must not be empty", i)
			}
			out = append(out, text)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("criteria citation must be string or list of strings")
	}
}

func criteriaRulesByID(set runtimecontracts.PolicyCriteriaSet) map[string]runtimecontracts.PolicyCriteriaRule {
	out := map[string]runtimecontracts.PolicyCriteriaRule{}
	for _, rule := range set.Rules {
		id := strings.TrimSpace(rule.ID)
		if id != "" {
			out[id] = rule
		}
	}
	return out
}

func criteriaCitationRuntimeError(eventType, fieldName, format string, args ...any) error {
	return NewRuntimeError(
		"criteria_citation_validation_failed",
		"tool-executor",
		"handle_emit_tool.validate_criteria_citations",
		false,
		"emit %s field %s: "+format,
		append([]any{strings.TrimSpace(eventType), strings.TrimSpace(fieldName)}, args...)...,
	)
}

func sortedCriteriaCitationFields(in map[string]runtimecontracts.CriteriaCitation) []string {
	names := make([]string, 0, len(in))
	for name := range in {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func sortedCriteriaRuleIDs(in map[string]runtimecontracts.PolicyCriteriaRule) []string {
	ids := make([]string, 0, len(in))
	for id := range in {
		id = strings.TrimSpace(id)
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func sortedStringSet(in map[string]struct{}) []string {
	values := make([]string, 0, len(in))
	for value := range in {
		value = strings.TrimSpace(value)
		if value != "" {
			values = append(values, value)
		}
	}
	sort.Strings(values)
	return values
}

func criteriaStringSet(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range UniqueNonEmpty(values) {
		out[value] = struct{}{}
	}
	return out
}
