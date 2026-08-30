package requiredagents

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Scope struct {
	ID           string
	Declarations []semanticview.AgentDeclaration
	Required     []runtimecontracts.FlowRequiredAgent
}

type FindingKind string

const (
	FindingMissingRole          FindingKind = "missing_role"
	FindingMissingAgent         FindingKind = "missing_agent"
	FindingAmbiguousAgent       FindingKind = "ambiguous_agent"
	FindingMissingSubscriptions FindingKind = "missing_subscriptions"
	FindingMissingEmits         FindingKind = "missing_emits"
)

type Finding struct {
	Kind       FindingKind
	ScopeID    string
	Role       string
	AgentID    string
	Missing    []string
	Candidates []string
}

func RootScope(source semanticview.Source) (Scope, bool) {
	if source == nil {
		return Scope{}, false
	}
	scope := Scope{
		ID:       "root",
		Required: source.RequiredAgents(),
	}
	scope.Declarations = semanticview.AgentDeclarationsForOwner(source, ".")
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
		if flowID == "" || flowID == "." {
			continue
		}
		out = append(out, Scope{
			ID:           flowID,
			Declarations: semanticview.AgentDeclarationsForOwner(source, flowID),
			Required:     source.FlowRequiredAgents(flowID),
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
		declaration, candidates := resolveAgentDeclaration(scope.Declarations, role)
		if len(candidates) > 1 {
			findings = append(findings, Finding{
				Kind:       FindingAmbiguousAgent,
				ScopeID:    scope.ID,
				Role:       role,
				Candidates: candidates,
			})
			continue
		}
		if len(candidates) == 0 {
			findings = append(findings, Finding{
				Kind:    FindingMissingAgent,
				ScopeID: scope.ID,
				Role:    role,
			})
			continue
		}
		agentID := strings.TrimSpace(declaration.LocalID)
		agent := declaration.Entry
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

func resolveAgentDeclaration(declarations []semanticview.AgentDeclaration, role string) (semanticview.AgentDeclaration, []string) {
	role = strings.TrimSpace(role)
	if role == "" {
		return semanticview.AgentDeclaration{}, nil
	}
	var matched semanticview.AgentDeclaration
	candidates := make([]string, 0, 1)
	for _, declaration := range declarations {
		if strings.TrimSpace(declaration.LocalID) != role {
			continue
		}
		matched = declaration
		label := strings.TrimSpace(declaration.Source.File)
		if label == "" {
			label = declaration.Label(true)
		}
		candidates = append(candidates, label)
	}
	sort.Strings(candidates)
	return matched, candidates
}

func AgentSubscriptions(agent runtimecontracts.AgentRegistryEntry) []string {
	return append([]string{}, agent.Subscriptions...)
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
