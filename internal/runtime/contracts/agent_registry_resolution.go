package contracts

import (
	"strings"

	"empireai/internal/models"
)

// ResolveAgentRegistryEntry matches a runtime agent config back to the MAS
// contract registry entry that defined it when possible.
func ResolveAgentRegistryEntry(bundle *WorkflowContractBundle, cfg models.AgentConfig) (string, AgentRegistryEntry, bool) {
	if bundle == nil {
		return "", AgentRegistryEntry{}, false
	}
	if matched := resolveAgentRegistryByID(bundle, strings.TrimSpace(cfg.ID)); matched != "" {
		entry, ok := bundle.MergedAgents[matched]
		return matched, entry, ok
	}

	role := canonicalPromptLookupValue(cfg.Role)
	if role == "" {
		return "", AgentRegistryEntry{}, false
	}
	mode := canonicalPromptLookupValue(cfg.Mode)
	for logicalID, entry := range bundle.MergedAgents {
		if canonicalPromptLookupValue(entry.Role) != role {
			continue
		}
		if mode != "" {
			if source, ok := bundle.AgentSources[strings.TrimSpace(logicalID)]; ok {
				if flowMode := promptFlowMode(bundle, source.FlowID); flowMode != "" && flowMode != mode {
					continue
				}
			}
		}
		return strings.TrimSpace(logicalID), entry, true
	}
	return "", AgentRegistryEntry{}, false
}

func resolveAgentRegistryByID(bundle *WorkflowContractBundle, agentID string) string {
	agentID = strings.TrimSpace(agentID)
	if bundle == nil || agentID == "" {
		return ""
	}
	if _, ok := bundle.MergedAgents[agentID]; ok {
		return agentID
	}
	for logicalID, entry := range bundle.MergedAgents {
		if promptRegistryIDMatches(entry.ID, agentID) {
			return strings.TrimSpace(logicalID)
		}
	}
	return ""
}
