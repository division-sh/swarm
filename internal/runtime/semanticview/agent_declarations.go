package semanticview

import (
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type AgentDeclaration struct {
	ScopeKind   string
	ScopeID     string
	OwnerFlowID string
	OwnerURI    string
	LocalID     string
	Entry       runtimecontracts.AgentRegistryEntry
	Source      runtimecontracts.ContractItemSource
}

func (d AgentDeclaration) Label(qualified bool) string {
	if !qualified || strings.TrimSpace(d.ScopeKind) == "" {
		return strings.TrimSpace(d.LocalID)
	}
	scopeID := strings.TrimSpace(d.ScopeID)
	if scopeID == "" || scopeID == "." {
		scopeID = "root"
	}
	return strings.Join([]string{strings.TrimSpace(d.ScopeKind), scopeID, "agent", strings.TrimSpace(d.LocalID)}, " ")
}

// AgentDeclarations projects the contracts-owned physical declaration census.
// The admitted filesystem flow tree is the declaration authority.
func AgentDeclarations(source Source) []AgentDeclaration {
	if source == nil {
		return nil
	}
	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		return nil
	}
	records := bundle.AgentDeclarationRecords()
	entries := make([]AgentDeclaration, 0, len(records))
	for _, record := range records {
		scopeKind := "flow"
		scopeID := strings.TrimSpace(record.Source.FlowPath)
		entries = append(entries, AgentDeclaration{
			ScopeKind:   scopeKind,
			ScopeID:     scopeID,
			OwnerFlowID: strings.TrimSpace(record.OwnerFlowID),
			OwnerURI:    strings.TrimSpace(record.OwnerURI),
			LocalID:     strings.TrimSpace(record.LogicalID),
			Entry:       record.Entry,
			Source:      record.Source,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Label(true) < entries[j].Label(true) })
	return entries
}

// AgentDeclarationsForOwner returns the complete declaration set owned by one
// semantic flow. The explicit root is ".".
func AgentDeclarationsForOwner(source Source, ownerFlowID string) []AgentDeclaration {
	ownerFlowID = strings.TrimSpace(ownerFlowID)
	declarations := AgentDeclarations(source)
	out := make([]AgentDeclaration, 0, len(declarations))
	for _, declaration := range declarations {
		if strings.TrimSpace(declaration.OwnerFlowID) == ownerFlowID {
			out = append(out, declaration)
		}
	}
	return out
}

func SourceDeclaresAgents(source Source) bool {
	return len(AgentDeclarations(source)) > 0
}
