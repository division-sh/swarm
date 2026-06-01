package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type AgentSessionScopeLocator struct {
	AgentID         string
	ProjectScopeKey string
	FlowID          string
}

type AgentSessionScopeProof struct {
	AgentID         string
	ProjectScopeKey string
	ContractSource  runtimecontracts.ContractItemSource
	OwningFlowID    string
	FlowPath        string
}

func ResolveAgentSessionScopeProof(source Source, locator AgentSessionScopeLocator) AgentSessionScopeProof {
	proof := AgentSessionScopeProof{
		AgentID:         strings.TrimSpace(locator.AgentID),
		ProjectScopeKey: strings.TrimSpace(locator.ProjectScopeKey),
	}
	if source == nil {
		return proof
	}

	if proof.AgentID != "" {
		if contractSource, ok := source.AgentContractSource(proof.AgentID); ok {
			proof.ContractSource = contractSource
			if proof.ProjectScopeKey == "" {
				proof.ProjectScopeKey = strings.TrimSpace(contractSource.PackageKey)
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
