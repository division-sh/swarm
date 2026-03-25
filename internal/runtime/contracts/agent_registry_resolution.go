package contracts

import (
	"strings"

	models "empireai/internal/runtime/core/actors"
)

type bundleAgentRecord struct {
	LogicalID string
	Entry     AgentRegistryEntry
	Source    ContractItemSource
}

// ResolveAgentRegistryEntry matches a runtime agent config back to the MAS
// contract registry entry that defined it when possible.
func ResolveAgentRegistryEntry(bundle *WorkflowContractBundle, cfg models.AgentConfig) (string, AgentRegistryEntry, bool) {
	if bundle == nil {
		return "", AgentRegistryEntry{}, false
	}
	if matched := resolveAgentRegistryByID(bundle, strings.TrimSpace(cfg.ID)); matched != "" {
		for _, record := range bundleAgentRecords(bundle) {
			if strings.TrimSpace(record.LogicalID) == matched {
				return matched, record.Entry, true
			}
		}
		return "", AgentRegistryEntry{}, false
	}

	role := canonicalPromptLookupValue(cfg.Role)
	if role == "" {
		return "", AgentRegistryEntry{}, false
	}
	mode := canonicalPromptLookupValue(cfg.Mode)
	for _, record := range bundleAgentRecords(bundle) {
		if canonicalPromptLookupValue(record.Entry.Role) != role {
			continue
		}
		if mode != "" {
			if flowMode := promptFlowMode(bundle, record.Source.FlowID); flowMode != "" && flowMode != mode {
				continue
			}
		}
		return strings.TrimSpace(record.LogicalID), record.Entry, true
	}
	return "", AgentRegistryEntry{}, false
}

func resolveAgentRegistryByID(bundle *WorkflowContractBundle, agentID string) string {
	agentID = strings.TrimSpace(agentID)
	if bundle == nil || agentID == "" {
		return ""
	}
	for _, record := range bundleAgentRecords(bundle) {
		if strings.TrimSpace(record.LogicalID) == agentID || promptRegistryIDMatches(record.Entry.ID, agentID) {
			return strings.TrimSpace(record.LogicalID)
		}
	}
	return ""
}

func bundleAgentRecords(bundle *WorkflowContractBundle) []bundleAgentRecord {
	if bundle == nil {
		return nil
	}
	records := make([]bundleAgentRecord, 0, len(bundle.ProjectViews())+len(bundle.FlowTree.ByID))
	for _, view := range bundle.ProjectViews() {
		key := strings.TrimSpace(view.Paths.Key)
		agentIDs := sortedContractKeys(view.Agents)
		for _, logicalID := range agentIDs {
			records = append(records, bundleAgentRecord{
				LogicalID: logicalID,
				Entry:     view.Agents[logicalID],
				Source:    ContractItemSource{PackageKey: key, Layer: "project"},
			})
		}
	}
	for _, view := range bundle.FlowViews() {
		flowID := strings.TrimSpace(view.Paths.ID)
		agentIDs := sortedContractKeys(view.Agents)
		for _, logicalID := range agentIDs {
			records = append(records, bundleAgentRecord{
				LogicalID: logicalID,
				Entry:     view.Agents[logicalID],
				Source: ContractItemSource{
					PackageKey: view.Paths.PackageKey,
					FlowID:     flowID,
					Layer:      "flow",
				},
			})
		}
	}
	return records
}

func bundleAgentRecordByLogicalID(bundle *WorkflowContractBundle, logicalID string) (bundleAgentRecord, bool) {
	logicalID = strings.TrimSpace(logicalID)
	if bundle == nil || logicalID == "" {
		return bundleAgentRecord{}, false
	}
	for _, record := range bundleAgentRecords(bundle) {
		if strings.TrimSpace(record.LogicalID) == logicalID {
			return record, true
		}
	}
	return bundleAgentRecord{}, false
}
