package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type AgentMemoryLocator struct {
	AgentID         string
	ProjectScopeKey string
	FlowID          string
}

type AgentMemoryProof struct {
	AgentID         string
	ProjectScopeKey string
	ContractSource  runtimecontracts.ContractItemSource
	OwningFlowID    string
	FlowPath        string
}

func ResolveAgentMemoryProof(source Source, locator AgentMemoryLocator) AgentMemoryProof {
	proof := AgentMemoryProof{
		AgentID:         strings.TrimSpace(locator.AgentID),
		ProjectScopeKey: strings.TrimSpace(locator.ProjectScopeKey),
	}
	if source == nil {
		return proof
	}

	if declaration, ok := agentMemoryDeclaration(source, locator); ok {
		projection, projected := ScopedAgentContractProjection(source, declaration)
		if projected {
			proof.ContractSource = projection.ContractSource
			proof.OwningFlowID = projection.OwnerFlowID
			if proof.ProjectScopeKey == "" {
				proof.ProjectScopeKey = strings.TrimSpace(projection.ContractSource.PackageKey)
			}
		}
	}

	if proof.AgentID == "" && proof.ProjectScopeKey == "" && strings.TrimSpace(locator.FlowID) == "" {
		return proof
	}

	if flowID := strings.TrimSpace(locator.FlowID); flowID != "" {
		proof.OwningFlowID = flowID
	} else if proof.ProjectScopeKey != "" {
		if projectScope, ok := projectScopeByKey(source, proof.ProjectScopeKey); ok {
			proof.ProjectScopeKey = projectScope.Key
			proof.OwningFlowID = strings.TrimSpace(projectScope.OwningFlowID)
		}
	}

	if proof.OwningFlowID == "" {
		return proof
	}

	if flowScope, ok := source.FlowScopeByID(proof.OwningFlowID); ok {
		proof.FlowPath = strings.Trim(strings.TrimSpace(flowScope.Path), "/")
		return proof
	}

	proof.FlowPath = strings.Trim(strings.TrimSpace(source.FlowPath(proof.OwningFlowID)), "/")
	return proof
}

func agentMemoryDeclaration(source Source, locator AgentMemoryLocator) (AgentDeclaration, bool) {
	agentID := strings.TrimSpace(locator.AgentID)
	projectScopeKey := strings.TrimSpace(locator.ProjectScopeKey)
	flowID := strings.TrimSpace(locator.FlowID)
	var matched AgentDeclaration
	for _, declaration := range AgentDeclarations(source) {
		if projectScopeKey != "" && strings.TrimSpace(declaration.ScopeID) != projectScopeKey {
			continue
		}
		if flowID != "" && !agentDeclarationMatchesFlow(declaration, flowID) {
			continue
		}
		plan, err := ScopedAgentNamePlan(source, declaration)
		if err != nil || (agentID != declaration.LocalID && agentID != plan.AgentID) {
			continue
		}
		if strings.TrimSpace(matched.LocalID) != "" {
			return AgentDeclaration{}, false
		}
		matched = declaration
	}
	return matched, strings.TrimSpace(matched.LocalID) != ""
}

func projectScopeByKey(source Source, key string) (ProjectScope, bool) {
	key = strings.TrimSpace(key)
	if source == nil || key == "" {
		return ProjectScope{}, false
	}
	for _, scope := range source.ProjectScopes() {
		if strings.TrimSpace(scope.Key) == key {
			return scope, true
		}
	}
	return ProjectScope{}, false
}
