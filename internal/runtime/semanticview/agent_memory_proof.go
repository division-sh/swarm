package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type AgentMemoryLocator struct {
	AgentID  string
	FlowPath string
}

type AgentMemoryProof struct {
	AgentID        string
	ContractSource runtimecontracts.ContractItemSource
	OwningFlowID   string
	FlowPath       string
}

func ResolveAgentMemoryProof(source Source, locator AgentMemoryLocator) AgentMemoryProof {
	proof := AgentMemoryProof{
		AgentID: strings.TrimSpace(locator.AgentID),
	}
	if source == nil {
		return proof
	}

	declaration, ok := agentMemoryDeclaration(source, locator)
	if !ok {
		return proof
	}
	proof.ContractSource = declaration.Source
	proof.OwningFlowID = strings.TrimSpace(declaration.OwnerFlowID)
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

func AgentMemoryProofForDeclaration(source Source, declaration AgentDeclaration) AgentMemoryProof {
	proof := AgentMemoryProof{
		AgentID:        strings.TrimSpace(declaration.LocalID),
		ContractSource: declaration.Source,
		OwningFlowID:   strings.TrimSpace(declaration.OwnerFlowID),
	}
	if source != nil && proof.OwningFlowID != "" {
		proof.FlowPath = strings.Trim(strings.TrimSpace(source.FlowPath(proof.OwningFlowID)), "/")
	}
	return proof
}

func agentMemoryDeclaration(source Source, locator AgentMemoryLocator) (AgentDeclaration, bool) {
	agentID := strings.TrimSpace(locator.AgentID)
	flowID := strings.TrimSpace(locator.FlowPath)
	var matched AgentDeclaration
	for _, declaration := range AgentDeclarations(source) {
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
