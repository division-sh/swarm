package requiredagents

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Scope struct {
	ID       string
	Agents   map[string]runtimecontracts.AgentRegistryEntry
	Required []runtimecontracts.FlowRequiredAgent
}

type FindingKind string

const (
	FindingMissingRole          FindingKind = "missing_role"
	FindingMissingAgent         FindingKind = "missing_agent"
	FindingMissingSubscriptions FindingKind = "missing_subscriptions"
	FindingMissingEmits         FindingKind = "missing_emits"
)

type Finding struct {
	Kind    FindingKind
	ScopeID string
	Role    string
	AgentID string
	Missing []string
}

func RootScope(source semanticview.Source) (Scope, bool) {
	if source == nil {
		return Scope{}, false
	}
	scope := Scope{
		ID:       "root",
		Required: source.RequiredAgents(),
	}
	for _, projectScope := range source.ProjectScopes() {
		if strings.TrimSpace(projectScope.OwningFlowID) == "" && projectScope.Depth == 0 {
			scope.Agents = projectScope.Agents
			return scope, true
		}
	}
	for _, projectScope := range source.ProjectScopes() {
		if strings.TrimSpace(projectScope.OwningFlowID) == "" {
			scope.Agents = projectScope.Agents
			return scope, true
		}
	}
	scope.Agents = source.AgentEntries()
	return scope, true
}

func FlowScopes(source semanticview.Source) []Scope {
	if source == nil {
		return nil
	}
	scopes := source.FlowScopes()
	out := make([]Scope, 0, len(scopes))
	for _, flowScope := range scopes {
		flowID := strings.TrimSpace(flowScope.ID)
		if flowID == "" {
			continue
		}
		out = append(out, Scope{
			ID:       flowID,
			Agents:   flowScope.Agents,
			Required: source.FlowRequiredAgents(flowID),
		})
	}
	return out
}

func AllScopes(source semanticview.Source) []Scope {
	if source == nil {
		return nil
	}
	out := make([]Scope, 0, len(source.FlowScopes())+1)
	if root, ok := RootScope(source); ok {
		out = append(out, root)
	}
	out = append(out, FlowScopes(source)...)
	return out
}

func CheckScope(scope Scope) []Finding {
	scope.ID = scopeLabel(scope.ID)
	if len(scope.Required) == 0 {
		return nil
	}
	findings := make([]Finding, 0)
	for _, required := range scope.Required {
		role := strings.TrimSpace(required.Role)
		if role == "" {
			findings = append(findings, Finding{
				Kind:    FindingMissingRole,
				ScopeID: scope.ID,
			})
			continue
		}
		agentID, agent, ok := ResolveAgent(scope.Agents, required)
		if !ok {
			findings = append(findings, Finding{
				Kind:    FindingMissingAgent,
				ScopeID: scope.ID,
				Role:    role,
			})
			continue
		}
		if missing := missingStrings(required.SubscribesTo, AgentSubscriptions(agent)); len(missing) > 0 {
			findings = append(findings, Finding{
				Kind:    FindingMissingSubscriptions,
				ScopeID: scope.ID,
				Role:    role,
				AgentID: agentID,
				Missing: missing,
			})
		}
		if missing := missingStrings(required.Emits, agent.EmitEvents); len(missing) > 0 {
			findings = append(findings, Finding{
				Kind:    FindingMissingEmits,
				ScopeID: scope.ID,
				Role:    role,
				AgentID: agentID,
				Missing: missing,
			})
		}
	}
	return findings
}

func EffectiveRequirements(agents map[string]runtimecontracts.AgentRegistryEntry, explicit []runtimecontracts.FlowRequiredAgent, declared bool) []runtimecontracts.FlowRequiredAgent {
	if declared || len(explicit) > 0 {
		return cloneRequiredAgents(explicit)
	}
	if len(agents) == 0 {
		return nil
	}
	out := make([]runtimecontracts.FlowRequiredAgent, 0, len(agents))
	for _, agentID := range sortedAgentKeys(agents) {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		agent := agents[agentID]
		out = append(out, runtimecontracts.FlowRequiredAgent{
			Role:         agentID,
			SubscribesTo: normalizeStrings(agent.Subscriptions),
			Emits:        normalizeStrings(agent.EmitEvents),
		})
	}
	return out
}

func ResolveAgent(agents map[string]runtimecontracts.AgentRegistryEntry, required runtimecontracts.FlowRequiredAgent) (string, runtimecontracts.AgentRegistryEntry, bool) {
	role := strings.TrimSpace(required.Role)
	if role == "" {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	agent, ok := agents[role]
	if !ok {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	return role, agent, true
}

func AgentSubscriptions(agent runtimecontracts.AgentRegistryEntry) []string {
	return append([]string{}, agent.Subscriptions...)
}

func cloneRequiredAgents(in []runtimecontracts.FlowRequiredAgent) []runtimecontracts.FlowRequiredAgent {
	if len(in) == 0 {
		return nil
	}
	out := make([]runtimecontracts.FlowRequiredAgent, len(in))
	for i, required := range in {
		out[i] = runtimecontracts.FlowRequiredAgent{
			Role:         strings.TrimSpace(required.Role),
			SubscribesTo: normalizeStrings(required.SubscribesTo),
			Emits:        normalizeStrings(required.Emits),
			Description:  strings.TrimSpace(required.Description),
		}
	}
	return out
}

func sortedAgentKeys(agents map[string]runtimecontracts.AgentRegistryEntry) []string {
	if len(agents) == 0 {
		return nil
	}
	keys := make([]string, 0, len(agents))
	for key := range agents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func MissingList(values []string) string {
	clean := normalizeStrings(values)
	if len(clean) == 0 {
		return ""
	}
	return fmt.Sprintf("missing %v", clean)
}

func scopeLabel(scopeID string) string {
	scopeID = strings.TrimSpace(scopeID)
	if scopeID == "" {
		return "root"
	}
	return scopeID
}

func missingStrings(expected, actual []string) []string {
	actualSet := stringSet(actual)
	missing := make([]string, 0)
	for _, value := range expected {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := actualSet[value]; !ok {
			missing = append(missing, value)
		}
	}
	sort.Strings(missing)
	return missing
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func normalizeStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}
