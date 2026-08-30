package contracts

import (
	"path/filepath"
	"sort"
	"strings"
)

// AgentDeclarationRecord is the immutable semantic projection of one physical
// agents.yaml declaration. The physical file/key identity remains private to
// contracts; consumers receive only the exact authored owner facts.
type AgentDeclarationRecord struct {
	LogicalID   string
	OwnerURI    string
	OwnerFlowID string
	Entry       AgentRegistryEntry
	Source      ContractItemSource
}

// AgentDeclarationRecords returns one record per physical declaration. Values
// are copied so callers cannot mutate the loader-owned contract snapshot.
func (b *WorkflowContractBundle) AgentDeclarationRecords() []AgentDeclarationRecord {
	records := bundleAgentRecords(b)
	out := make([]AgentDeclarationRecord, len(records))
	for index, record := range records {
		record.Entry = EffectiveAgentRegistryEntry(record.LogicalID, record.Entry)
		out[index] = record
	}
	return out
}

func bundleAgentRecords(bundle *WorkflowContractBundle) []AgentDeclarationRecord {
	if bundle == nil {
		return nil
	}
	records := make([]AgentDeclarationRecord, 0, len(bundle.FlowTree.ByID))
	for _, view := range bundle.FlowViews() {
		flowPath := strings.TrimSpace(view.Paths.FlowPath)
		agentIDs := sortedContractKeys(view.Agents)
		for _, logicalID := range agentIDs {
			source := ContractItemSource{FlowPath: flowPath, Family: "agents", File: view.Paths.AgentsFile}
			ownerURI := bundle.agentDeclarationOwnerURI(source, logicalID, view.AgentURIs[logicalID])
			records = append(records, AgentDeclarationRecord{LogicalID: logicalID, OwnerURI: ownerURI, OwnerFlowID: flowPath, Entry: view.Agents[logicalID], Source: source})
		}
	}
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

func (b *WorkflowContractBundle) agentDeclarationOwnerURI(source ContractItemSource, logicalID, preferred string) string {
	if owner := strings.TrimSpace(preferred); owner != "" {
		return owner
	}
	if b == nil || strings.TrimSpace(source.FlowPath) == "" {
		return ""
	}
	owner := ""
	for _, ref := range b.URIRegistry.Agents {
		if strings.TrimSpace(ref.Kind) != "agent" || strings.TrimSpace(ref.FlowID) != strings.TrimSpace(source.FlowPath) || strings.TrimSpace(ref.LocalID) != strings.TrimSpace(logicalID) {
			continue
		}
		candidate := strings.TrimSpace(ref.Full)
		if candidate == "" {
			candidate = strings.TrimSpace(ref.Absolute)
		}
		if candidate == "" || (owner != "" && owner != candidate) {
			return ""
		}
		owner = candidate
	}
	return owner
}

func agentDeclarationRecordKey(sourceFile, logicalID string) string {
	sourceFile = strings.TrimSpace(sourceFile)
	if sourceFile != "" {
		sourceFile = filepath.Clean(sourceFile)
	}
	return sourceFile + "\x00" + strings.TrimSpace(logicalID)
}
