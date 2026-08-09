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
	LocalID     string
	Entry       runtimecontracts.AgentRegistryEntry
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

// AgentDeclarations enumerates canonical project/flow declarations directly.
// Flattened aliases are retained only for declarations not represented by a
// canonical scope, so ambiguous local IDs cannot disappear from a census.
func AgentDeclarations(source Source) []AgentDeclaration {
	if source == nil {
		return nil
	}
	entries := []AgentDeclaration{}
	representedLocalIDs := map[string]struct{}{}
	appendScoped := func(scopeKind, scopeID, ownerFlowID string, agents map[string]runtimecontracts.AgentRegistryEntry) {
		keys := make([]string, 0, len(agents))
		for rawID := range agents {
			if id := strings.TrimSpace(rawID); id != "" {
				keys = append(keys, rawID)
			}
		}
		sort.Slice(keys, func(i, j int) bool { return strings.TrimSpace(keys[i]) < strings.TrimSpace(keys[j]) })
		for _, rawID := range keys {
			localID := strings.TrimSpace(rawID)
			representedLocalIDs[localID] = struct{}{}
			entries = append(entries, AgentDeclaration{
				ScopeKind:   scopeKind,
				ScopeID:     strings.TrimSpace(scopeID),
				OwnerFlowID: strings.TrimSpace(ownerFlowID),
				LocalID:     localID,
				Entry:       agents[rawID],
			})
		}
	}
	for _, scope := range source.ProjectScopes() {
		appendScoped("project", scope.Key, scope.OwningFlowID, scope.Agents)
	}
	for _, scope := range source.FlowScopes() {
		appendScoped("flow", scope.ID, scope.ID, scope.Agents)
	}
	aliases := source.AgentEntries()
	aliasKeys := make([]string, 0, len(aliases))
	for rawID := range aliases {
		if id := strings.TrimSpace(rawID); id != "" {
			aliasKeys = append(aliasKeys, rawID)
		}
	}
	sort.Slice(aliasKeys, func(i, j int) bool { return strings.TrimSpace(aliasKeys[i]) < strings.TrimSpace(aliasKeys[j]) })
	for _, rawID := range aliasKeys {
		localID := strings.TrimSpace(rawID)
		if _, represented := representedLocalIDs[localID]; represented {
			continue
		}
		entries = append(entries, AgentDeclaration{LocalID: localID, Entry: aliases[rawID]})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Label(true) < entries[j].Label(true) })
	return entries
}

func SourceDeclaresAgents(source Source) bool {
	return len(AgentDeclarations(source)) > 0
}
