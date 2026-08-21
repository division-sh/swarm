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
	projectOwners := map[string]string{}
	for _, view := range bundle.ProjectViews() {
		ownerFlowID := strings.TrimSpace(view.Paths.OwningFlowID)
		for _, logicalID := range sortedContractKeys(view.Agents) {
			source := ContractItemSource{PackageKey: strings.TrimSpace(view.Paths.Key), FlowID: ownerFlowID, Layer: "project", File: view.Paths.ProjectAgentsFile}
			if strings.TrimSpace(source.File) != "" {
				projectOwners[agentDeclarationRecordKey(source.File, logicalID)] = strings.TrimSpace(view.AgentURIs[logicalID])
			}
		}
	}
	flowDeclarations := map[string]struct{}{}
	flowRecords := make([]AgentDeclarationRecord, 0, len(bundle.FlowTree.ByID))
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
			ownerURI := bundle.agentDeclarationOwnerURI(source, logicalID, view.AgentURIs[logicalID])
			if canonical := projectOwners[agentDeclarationRecordKey(source.File, logicalID)]; canonical != "" {
				ownerURI = canonical
			}
			flowRecords = append(flowRecords, AgentDeclarationRecord{LogicalID: logicalID, OwnerURI: ownerURI, OwnerFlowID: flowID, Entry: view.Agents[logicalID], Source: source})
		}
	}
	records := make([]AgentDeclarationRecord, 0, len(bundle.ProjectViews())+len(bundle.FlowTree.ByID))
	for _, view := range bundle.ProjectViews() {
		key := strings.TrimSpace(view.Paths.Key)
		ownerFlowID := strings.TrimSpace(view.Paths.OwningFlowID)
		agentIDs := sortedContractKeys(view.Agents)
		for _, logicalID := range agentIDs {
			source := ContractItemSource{PackageKey: key, FlowID: ownerFlowID, Layer: "project", File: view.Paths.ProjectAgentsFile}
			if strings.TrimSpace(source.File) != "" {
				if _, representedByFlow := flowDeclarations[agentDeclarationRecordKey(source.File, logicalID)]; representedByFlow {
					continue
				}
			}
			records = append(records, AgentDeclarationRecord{
				LogicalID:   logicalID,
				OwnerURI:    strings.TrimSpace(view.AgentURIs[logicalID]),
				OwnerFlowID: ownerFlowID,
				Entry:       view.Agents[logicalID],
				Source:      source,
			})
		}
	}
	if len(records) == 0 && len(flowRecords) == 0 && len(bundle.Agents) > 0 {
		for _, logicalID := range sortedContractKeys(bundle.Agents) {
			ownerURI := ""
			if ref, ok := bundle.URIRegistry.Agents[logicalID]; ok {
				ownerURI = strings.TrimSpace(ref.Full)
				if ownerURI == "" {
					ownerURI = strings.TrimSpace(ref.Absolute)
				}
			}
			records = append(records, AgentDeclarationRecord{
				LogicalID: logicalID,
				OwnerURI:  ownerURI,
				Entry:     bundle.Agents[logicalID],
				Source:    ContractItemSource{PackageKey: ".", Layer: "project", File: bundle.Paths.ProjectAgentsFile},
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

// PackageOwningFlowID returns the immutable ownership fact established while
// the package tree is discovered. Runtime consumers must not rederive it.
func (b *WorkflowContractBundle) PackageOwningFlowID(packageKey string) string {
	packageKey = strings.TrimSpace(packageKey)
	if b == nil || packageKey == "" {
		return ""
	}
	view, ok := b.ProjectViewByKey(packageKey)
	if !ok {
		return ""
	}
	return strings.TrimSpace(view.Paths.OwningFlowID)
}

func (b *WorkflowContractBundle) agentDeclarationOwnerURI(source ContractItemSource, logicalID, preferred string) string {
	if owner := strings.TrimSpace(preferred); owner != "" {
		return owner
	}
	if b == nil || strings.TrimSpace(source.Layer) != "flow" || strings.TrimSpace(source.FlowID) == "" {
		return ""
	}
	owner := ""
	for _, ref := range b.URIRegistry.Agents {
		if strings.TrimSpace(ref.Kind) != "agent" || strings.TrimSpace(ref.FlowID) != strings.TrimSpace(source.FlowID) || strings.TrimSpace(ref.LocalID) != strings.TrimSpace(logicalID) {
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
