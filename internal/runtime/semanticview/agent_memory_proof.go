package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type AgentMemoryProof struct {
	AgentID        string
	ContractSource runtimecontracts.ContractItemSource
	OwningFlowID   string
	FlowPath       string
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
