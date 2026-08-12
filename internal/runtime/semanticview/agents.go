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
	return agentDeclarationOwner(source, AgentDeclaration{OwnerFlowID: flowID, LocalID: logicalID})
}

func ScopedAgentDeclarationOwner(source Source, declaration AgentDeclaration) (string, bool) {
	declarations := AgentDeclarations(source)
	for _, candidate := range declarations {
		if strings.TrimSpace(candidate.ScopeKind) != strings.TrimSpace(declaration.ScopeKind) ||
			strings.TrimSpace(candidate.ScopeID) != strings.TrimSpace(declaration.ScopeID) ||
			strings.TrimSpace(candidate.LocalID) != strings.TrimSpace(declaration.LocalID) {
			continue
		}
		ownerURI := strings.TrimSpace(candidate.OwnerURI)
		for _, other := range declarations {
			if strings.TrimSpace(other.ScopeKind) == strings.TrimSpace(candidate.ScopeKind) &&
				strings.TrimSpace(other.ScopeID) == strings.TrimSpace(candidate.ScopeID) &&
				strings.TrimSpace(other.LocalID) == strings.TrimSpace(candidate.LocalID) {
				continue
			}
			if ownerURI != "" && strings.TrimSpace(other.OwnerURI) == ownerURI {
				return "", false
			}
		}
		return agentDeclarationOwner(source, candidate)
	}
	return "", false
}

func agentDeclarationOwner(source Source, declaration AgentDeclaration) (string, bool) {
	if source == nil {
		return "", false
	}
	flowID := strings.TrimSpace(declaration.OwnerFlowID)
	logicalID := strings.TrimSpace(declaration.LocalID)
	if logicalID == "" {
		return "", false
	}

	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		return "", false
	}
	if owner := strings.TrimSpace(declaration.OwnerURI); owner != "" {
		ref, exists := bundle.URIRegistry.ByURI[owner]
		if exists && strings.TrimSpace(ref.Kind) == "agent" && strings.TrimSpace(ref.LocalID) == logicalID {
			return owner, true
		}
		return "", false
	}
	owners := map[string]struct{}{}
	for _, ref := range bundle.URIRegistry.Agents {
		if strings.TrimSpace(ref.LocalID) != logicalID {
			continue
		}
		switch strings.TrimSpace(declaration.ScopeKind) {
		case "flow":
			if strings.TrimSpace(ref.FlowID) != flowID {
				continue
			}
		case "project":
			if strings.TrimSpace(ref.FlowID) != "" || !projectAgentRefOwnedByFlow(source, ref, flowID, logicalID) {
				continue
			}
		default:
			refFlowID := strings.TrimSpace(ref.FlowID)
			if refFlowID != flowID {
				if refFlowID != "" || !projectAgentRefOwnedByFlow(source, ref, flowID, logicalID) {
					continue
				}
			}
		}
		owner := strings.TrimSpace(ref.Full)
		if owner == "" {
			owner = strings.TrimSpace(ref.Absolute)
		}
		if owner != "" {
			owners[owner] = struct{}{}
		}
	}
	if len(owners) == 1 {
		for owner := range owners {
			return owner, true
		}
	}
	return "", false
}

func projectAgentRefOwnedByFlow(source Source, ref runtimecontracts.ContractURIRef, flowID, logicalID string) bool {
	refPath := strings.Trim(strings.TrimSpace(ref.Path), "/")
	matchingScopes := 0
	for _, scope := range source.ProjectScopes() {
		if strings.TrimSpace(scope.OwningFlowID) != strings.TrimSpace(flowID) {
			continue
		}
		if _, ok := scope.Agents[strings.TrimSpace(logicalID)]; !ok {
			continue
		}
		matchingScopes++
		if strings.Trim(strings.TrimSpace(scope.Key), "/") == refPath {
			return true
		}
	}
	if matchingScopes != 1 {
		return false
	}
	matchingRefs := 0
	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		return false
	}
	for _, candidate := range bundle.URIRegistry.Agents {
		if strings.TrimSpace(candidate.FlowID) == "" && strings.TrimSpace(candidate.LocalID) == strings.TrimSpace(logicalID) {
			matchingRefs++
		}
	}
	return matchingRefs == 1
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
