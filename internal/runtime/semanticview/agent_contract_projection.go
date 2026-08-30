package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

// AgentContractProjection keeps declaration scope attached to contract and
// tool lookup. Public agent names are not declaration coordinates.
type AgentContractProjection struct {
	Declaration    AgentDeclaration
	ContractSource runtimecontracts.ContractItemSource
	OwnerFlowID    string
	scopedTools    map[string]runtimecontracts.ToolSchemaEntry
	globalTools    map[string]runtimecontracts.ToolSchemaEntry
}

func ResolveAgentContractProjection(source Source, actor models.AgentConfig) (AgentContractProjection, bool) {
	declaration, ok := ResolveAgentDeclaration(source, actor)
	if !ok {
		return AgentContractProjection{}, false
	}
	return ScopedAgentContractProjection(source, declaration)
}

func ScopedAgentContractProjection(source Source, declaration AgentDeclaration) (AgentContractProjection, bool) {
	if source == nil || strings.TrimSpace(declaration.LocalID) == "" {
		return AgentContractProjection{}, false
	}
	projection := AgentContractProjection{
		Declaration:    declaration,
		ContractSource: declaration.Source,
		OwnerFlowID:    strings.TrimSpace(declaration.OwnerFlowID),
		globalTools:    source.ToolEntries(),
	}
	switch strings.TrimSpace(declaration.ScopeKind) {
	case "flow":
		for _, scope := range source.FlowScopes() {
			if strings.TrimSpace(scope.ID) != strings.TrimSpace(declaration.ScopeID) {
				continue
			}
			projection.scopedTools = scope.Tools
			return projection, true
		}
	default:
		return AgentContractProjection{}, false
	}
	return AgentContractProjection{}, false
}

func (p AgentContractProjection) ToolEntry(toolID string) (runtimecontracts.ToolSchemaEntry, bool) {
	toolID = strings.TrimSpace(toolID)
	if toolID == "" {
		return runtimecontracts.ToolSchemaEntry{}, false
	}
	if entry, ok := p.scopedTools[toolID]; ok {
		return entry, true
	}
	entry, ok := p.globalTools[toolID]
	return entry, ok
}
