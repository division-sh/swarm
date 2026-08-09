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
	representedDeclarations := map[string]struct{}{}
	appendScoped := func(scopeKind, scopeID, ownerFlowID, canonicalScopeID string, agents map[string]runtimecontracts.AgentRegistryEntry, agentURIs map[string]string) {
		keys := make([]string, 0, len(agents))
		for rawID := range agents {
			if id := strings.TrimSpace(rawID); id != "" {
				keys = append(keys, rawID)
			}
		}
		sort.Slice(keys, func(i, j int) bool { return strings.TrimSpace(keys[i]) < strings.TrimSpace(keys[j]) })
		for _, rawID := range keys {
			localID := strings.TrimSpace(rawID)
			declarationKey := strings.Join([]string{
				strings.TrimSpace(ownerFlowID),
				strings.TrimSpace(canonicalScopeID),
				localID,
			}, "\x00")
			if _, represented := representedDeclarations[declarationKey]; represented {
				continue
			}
			representedDeclarations[declarationKey] = struct{}{}
			representedLocalIDs[localID] = struct{}{}
			entries = append(entries, AgentDeclaration{
				ScopeKind:   scopeKind,
				ScopeID:     strings.TrimSpace(scopeID),
				OwnerFlowID: strings.TrimSpace(ownerFlowID),
				OwnerURI:    strings.TrimSpace(agentURIs[rawID]),
				LocalID:     localID,
				Entry:       agents[rawID],
			})
		}
	}
	projectScopes := sortedAuthoredProjectScopes(source.ProjectScopes())
	flowScopes := sortedAuthoredFlowScopes(source.FlowScopes())
	preferredFlowScopeKeys := authoredPreferredFlowScopeKeys(projectScopes, flowScopes)
	for _, scope := range flowScopes {
		flowID := strings.TrimSpace(scope.ID)
		scopeKey := authoredEmitSiteFlowScopeKey(scope)
		if preferred := preferredFlowScopeKeys[flowID]; preferred != "" && scopeKey != preferred {
			continue
		}
		appendScoped("flow", flowID, flowID, scopeKey, scope.Agents, scope.AgentURIs)
	}
	for _, scope := range projectScopes {
		if authoredEmitSiteSkipsProjectScope(scope) {
			continue
		}
		appendScoped("project", scope.Key, scope.OwningFlowID, scope.Key, scope.Agents, scope.AgentURIs)
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
