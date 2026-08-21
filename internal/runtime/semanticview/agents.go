package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func ResolveAgentRegistryEntry(source Source, cfg models.AgentConfig) (string, runtimecontracts.AgentRegistryEntry, bool) {
	declaration, ok := ResolveAgentDeclaration(source, cfg)
	if !ok {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	return strings.TrimSpace(declaration.LocalID), declaration.Entry, true
}

// ResolveAgentDeclaration returns the exact scoped declaration owned by a
// runtime actor. Effective public names are only unique within a scope, so the
// scope must remain attached throughout selection.
func ResolveAgentDeclaration(source Source, cfg models.AgentConfig) (AgentDeclaration, bool) {
	if source == nil {
		return AgentDeclaration{}, false
	}
	agentID := strings.TrimSpace(cfg.ID)
	flowID := strings.TrimSpace(cfg.FlowID)
	name := cfg.Identity.Name.Normalize()
	if name.Source == agentidentity.NameSourceDeclared {
		if err := name.Validate(); err != nil || (agentID != "" && agentID != name.AgentID) {
			return AgentDeclaration{}, false
		}
		if declaration, ok := resolveAgentDeclarationByName(source, flowID, name.AgentID, name.Owner); ok {
			return declaration, true
		}
		return AgentDeclaration{}, false
	}
	if name != (agentidentity.Name{}) {
		return AgentDeclaration{}, false
	}
	if declaration, ok := resolveAgentDeclarationByID(source, flowID, agentID); ok {
		return declaration, true
	}
	if agentID != "" && retiredLocalAgentAlias(source, flowID, agentID) {
		return AgentDeclaration{}, false
	}

	role := canonicalLookupValue(cfg.Role)
	if role == "" {
		return AgentDeclaration{}, false
	}
	var matched AgentDeclaration
	for _, declaration := range AgentDeclarations(source) {
		plan, err := ScopedAgentNamePlan(source, declaration)
		if err != nil || canonicalLookupValue(plan.EffectiveRole(declaration.Entry)) != role {
			continue
		}
		if !agentDeclarationMatchesFlow(declaration, flowID) {
			continue
		}
		if strings.TrimSpace(matched.LocalID) != "" {
			return AgentDeclaration{}, false
		}
		matched = declaration
	}
	if strings.TrimSpace(matched.LocalID) == "" {
		return AgentDeclaration{}, false
	}
	return matched, true
}

func retiredLocalAgentAlias(source Source, flowID, candidate string) bool {
	flowID = strings.TrimSpace(flowID)
	candidate = strings.TrimSpace(candidate)
	for _, declaration := range AgentDeclarations(source) {
		if strings.TrimSpace(declaration.LocalID) != candidate {
			continue
		}
		if flowID != "" && strings.TrimSpace(declaration.OwnerFlowID) != flowID {
			continue
		}
		plan, err := ScopedAgentNamePlan(source, declaration)
		if err == nil && plan.AgentID != candidate {
			return true
		}
	}
	return false
}

func AgentDeclarationOwner(source Source, flowID, logicalID string) (string, bool) {
	flowID = strings.TrimSpace(flowID)
	logicalID = strings.TrimSpace(logicalID)
	var matched AgentDeclaration
	for _, declaration := range AgentDeclarations(source) {
		if strings.TrimSpace(declaration.OwnerFlowID) != flowID || strings.TrimSpace(declaration.LocalID) != logicalID {
			continue
		}
		if strings.TrimSpace(matched.LocalID) != "" {
			return "", false
		}
		matched = declaration
	}
	if strings.TrimSpace(matched.LocalID) == "" {
		return "", false
	}
	return agentDeclarationOwner(source, matched)
}

func ScopedAgentDeclarationOwner(source Source, declaration AgentDeclaration) (string, bool) {
	return agentDeclarationOwner(source, declaration)
}

func agentDeclarationOwner(source Source, declaration AgentDeclaration) (string, bool) {
	if source == nil {
		return "", false
	}
	logicalID := strings.TrimSpace(declaration.LocalID)
	if logicalID == "" {
		return "", false
	}

	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		return "", false
	}
	owner := strings.TrimSpace(declaration.OwnerURI)
	if owner == "" {
		return "", false
	}
	ref, exists := bundle.URIRegistry.ByURI[owner]
	if !exists || strings.TrimSpace(ref.Kind) != "agent" || strings.TrimSpace(ref.LocalID) != logicalID {
		return "", false
	}
	for _, candidate := range AgentDeclarations(source) {
		if candidate.Source == declaration.Source &&
			strings.TrimSpace(candidate.ScopeKind) == strings.TrimSpace(declaration.ScopeKind) &&
			strings.TrimSpace(candidate.ScopeID) == strings.TrimSpace(declaration.ScopeID) &&
			strings.TrimSpace(candidate.OwnerFlowID) == strings.TrimSpace(declaration.OwnerFlowID) &&
			strings.TrimSpace(candidate.LocalID) == logicalID && strings.TrimSpace(candidate.OwnerURI) == owner {
			return owner, true
		}
	}
	return "", false
}

func resolveAgentDeclarationByName(source Source, flowID, agentID, ownerURI string) (AgentDeclaration, bool) {
	flowID = strings.TrimSpace(flowID)
	agentID = strings.TrimSpace(agentID)
	ownerURI = strings.TrimSpace(ownerURI)
	if source == nil || agentID == "" || ownerURI == "" {
		return AgentDeclaration{}, false
	}
	var matched AgentDeclaration
	for _, declaration := range AgentDeclarations(source) {
		if !agentDeclarationMatchesFlow(declaration, flowID) {
			continue
		}
		plan, err := ScopedAgentNamePlan(source, declaration)
		if err != nil || plan.AgentID != agentID || plan.OwnerURI != ownerURI {
			continue
		}
		if strings.TrimSpace(matched.LocalID) != "" {
			return AgentDeclaration{}, false
		}
		matched = declaration
	}
	if strings.TrimSpace(matched.LocalID) == "" {
		return AgentDeclaration{}, false
	}
	return matched, true
}

func resolveAgentDeclarationByID(source Source, flowID, agentID string) (AgentDeclaration, bool) {
	flowID = strings.TrimSpace(flowID)
	agentID = strings.TrimSpace(agentID)
	if source == nil || agentID == "" {
		return AgentDeclaration{}, false
	}
	var matched AgentDeclaration
	for _, declaration := range AgentDeclarations(source) {
		if !agentDeclarationMatchesFlow(declaration, flowID) {
			continue
		}
		plan, err := ScopedAgentNamePlan(source, declaration)
		if err != nil || plan.AgentID != agentID {
			continue
		}
		if strings.TrimSpace(matched.LocalID) != "" {
			return AgentDeclaration{}, false
		}
		matched = declaration
	}
	if strings.TrimSpace(matched.LocalID) == "" {
		return AgentDeclaration{}, false
	}
	return matched, true
}

func agentDeclarationMatchesFlow(declaration AgentDeclaration, flowID string) bool {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return true
	}
	return strings.TrimSpace(declaration.OwnerFlowID) == flowID
}

func canonicalLookupValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "-")
	return value
}
