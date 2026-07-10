package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (e *Executor) validateEmitCriteriaCitations(actor models.AgentConfig, eventType string, schema EmitSchema, payload map[string]any) error {
	if len(schema.CitationFields) == 0 {
		return nil
	}
	if e == nil || e.workflowSource == nil {
		return failures.New(
			failures.ClassDependencyUnavailable,
			"criteria_policy_source_unavailable",
			"tool-executor",
			"handle_emit_tool.validate_criteria_citations",
			map[string]any{"event": strings.TrimSpace(eventType)},
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
			return criteriaCitationFailure(eventType, fieldName, "criteria_set_not_declared", nil)
		}
		if _, ok := declared[setName]; !ok {
			return criteriaCitationFailure(eventType, fieldName, "criteria_set_not_allowed", map[string]any{"actor": actorLabel(actor), "criteria_set": setName})
		}
		set, ok := policy.Criteria[setName]
		if !ok {
			return criteriaCitationFailure(eventType, fieldName, "criteria_set_unresolved", map[string]any{"criteria_set": setName, "flow": flowID})
		}
		ids, err := criteriaCitationIDs(value)
		if err != nil {
			return criteriaCitationFailure(eventType, fieldName, "criteria_citation_shape_invalid", nil)
		}
		rules := criteriaRulesByID(set)
		allowed := criteriaStringSet(citation.AllowedClasses)
		validIDs := sortedCriteriaRuleIDs(rules)
		for _, id := range ids {
			rule, ok := rules[id]
			if !ok {
				return criteriaCitationFailure(eventType, fieldName, "criteria_id_unknown", map[string]any{"criteria_set": setName, "criteria_id": id, "valid_ids": validIDs})
			}
			className := strings.TrimSpace(rule.Class)
			if _, ok := allowed[className]; !ok {
				return criteriaCitationFailure(eventType, fieldName, "criteria_class_not_allowed", map[string]any{"criteria_id": id, "criteria_class": className, "allowed_classes": sortedStringSet(allowed)})
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

func criteriaCitationFailure(eventType, fieldName, reason string, attributes map[string]any) error {
	if attributes == nil {
		attributes = map[string]any{}
	}
	attributes["event"] = strings.TrimSpace(eventType)
	attributes["field"] = strings.TrimSpace(fieldName)
	attributes["reason"] = strings.TrimSpace(reason)
	return failures.NewDetail(
		"criteria_citation_validation_failed",
		"tool-executor",
		"handle_emit_tool.validate_criteria_citations",
		attributes,
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
