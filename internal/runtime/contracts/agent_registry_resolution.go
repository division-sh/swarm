package contracts

import (
	"path/filepath"
	"sort"
	"strings"
)

type bundleAgentRecord struct {
	LogicalID string
	Entry     AgentRegistryEntry
	Source    ContractItemSource
}

func bundleAgentRecords(bundle *WorkflowContractBundle) []bundleAgentRecord {
	if bundle == nil {
		return nil
	}
	flowDeclarations := map[string]struct{}{}
	flowRecords := make([]bundleAgentRecord, 0, len(bundle.FlowTree.ByID))
	for _, view := range bundle.FlowViews() {
		flowID := strings.TrimSpace(view.Paths.ID)
		agentIDs := sortedContractKeys(view.Agents)
		for _, logicalID := range agentIDs {
			source := ContractItemSource{
				PackageKey: view.Paths.PackageKey,
				FlowID:     flowID,
				Layer:      "flow",
				File:       view.Paths.AgentsFile,
			}
			if strings.TrimSpace(source.File) != "" {
				flowDeclarations[agentDeclarationRecordKey(source.File, logicalID)] = struct{}{}
			}
			flowRecords = append(flowRecords, bundleAgentRecord{LogicalID: logicalID, Entry: view.Agents[logicalID], Source: source})
		}
	}
	records := make([]bundleAgentRecord, 0, len(bundle.ProjectViews())+len(bundle.FlowTree.ByID))
	for _, view := range bundle.ProjectViews() {
		key := strings.TrimSpace(view.Paths.Key)
		agentIDs := sortedContractKeys(view.Agents)
		for _, logicalID := range agentIDs {
			source := ContractItemSource{PackageKey: key, Layer: "project", File: view.Paths.ProjectAgentsFile}
			if strings.TrimSpace(source.File) != "" {
				if _, representedByFlow := flowDeclarations[agentDeclarationRecordKey(source.File, logicalID)]; representedByFlow {
					continue
				}
			}
			records = append(records, bundleAgentRecord{
				LogicalID: logicalID,
				Entry:     view.Agents[logicalID],
				Source:    source,
			})
		}
	}
	records = append(records, flowRecords...)
	sort.Slice(records, func(i, j int) bool {
		left := contractScopeKey(records[i].Source, records[i].LogicalID)
		right := contractScopeKey(records[j].Source, records[j].LogicalID)
		if left == right {
			return strings.TrimSpace(records[i].Source.File) < strings.TrimSpace(records[j].Source.File)
		}
		return left < right
	})
	return records
}

func agentDeclarationRecordKey(sourceFile, logicalID string) string {
	sourceFile = strings.TrimSpace(sourceFile)
	if sourceFile != "" {
		sourceFile = filepath.Clean(sourceFile)
	}
	return sourceFile + "\x00" + strings.TrimSpace(logicalID)
}
