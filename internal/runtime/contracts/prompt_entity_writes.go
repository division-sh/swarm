package contracts

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

type PromptEntityWriteEvidence struct {
	AgentID      string
	Source       ContractItemSource
	Entry        AgentRegistryEntry
	PromptFile   string
	CreateEntity bool
	SaveEntity   bool
	SaveFields   []string
}

var promptSaveFieldPattern = regexp.MustCompile("`([a-zA-Z_][a-zA-Z0-9_]*)`")

func DerivePromptEntityWriteEvidence(bundle *WorkflowContractBundle) ([]PromptEntityWriteEvidence, error) {
	if bundle == nil {
		return nil, fmt.Errorf("workflow contract bundle is required")
	}
	out := make([]PromptEntityWriteEvidence, 0)
	for _, record := range bundleAgentRecords(bundle) {
		agentID := strings.TrimSpace(record.LogicalID)
		if agentID == "" {
			continue
		}
		path, text, ok, err := loadPromptEntityWriteText(bundle, agentID, record.Entry, record.Source)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		createEntity, saveEntity, saveFields := extractPromptEntityWriteEvidence(text)
		if !createEntity && !saveEntity {
			continue
		}
		out = append(out, PromptEntityWriteEvidence{
			AgentID:      agentID,
			Source:       record.Source,
			Entry:        record.Entry,
			PromptFile:   path,
			CreateEntity: createEntity,
			SaveEntity:   saveEntity,
			SaveFields:   saveFields,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AgentID == out[j].AgentID {
			if out[i].Source.FlowID == out[j].Source.FlowID {
				return out[i].PromptFile < out[j].PromptFile
			}
			return out[i].Source.FlowID < out[j].Source.FlowID
		}
		return out[i].AgentID < out[j].AgentID
	})
	return out, nil
}

func loadPromptEntityWriteText(bundle *WorkflowContractBundle, agentID string, entry AgentRegistryEntry, source ContractItemSource) (string, string, bool, error) {
	dirs := promptEntityWritePromptDirs(bundle, source)
	if len(dirs) == 0 {
		return "", "", false, nil
	}
	mode := promptFlowMode(bundle, source.FlowID)
	for _, dir := range dirs {
		for _, candidate := range promptPathCandidates(dir, agentID, entry, mode) {
			raw, err := os.ReadFile(candidate)
			if err == nil {
				return candidate, string(raw), true, nil
			}
			if !os.IsNotExist(err) {
				return "", "", false, fmt.Errorf("read %s: %w", candidate, err)
			}
		}
	}
	return "", "", false, nil
}

func promptEntityWritePromptDirs(bundle *WorkflowContractBundle, source ContractItemSource) []string {
	if bundle == nil {
		return nil
	}
	dirs := make([]string, 0, 4)
	if flowID := strings.TrimSpace(source.FlowID); flowID != "" {
		if flow, ok := bundle.FlowViewByID(flowID); ok && strings.TrimSpace(flow.Paths.PromptsDir) != "" {
			dirs = append(dirs, flow.Paths.PromptsDir)
		}
	}
	if pkgKey := strings.TrimSpace(source.PackageKey); pkgKey != "" {
		for _, pkg := range bundle.ProjectViews() {
			if strings.TrimSpace(pkg.Paths.Key) == pkgKey && strings.TrimSpace(pkg.Paths.ProjectPromptsDir) != "" {
				dirs = append(dirs, pkg.Paths.ProjectPromptsDir)
				break
			}
		}
	}
	if strings.TrimSpace(bundle.Paths.ProjectPromptsDir) != "" {
		dirs = append(dirs, bundle.Paths.ProjectPromptsDir)
	}
	return uniqueStrings(dirs...)
}

func extractPromptEntityWriteEvidence(promptText string) (bool, bool, []string) {
	createEntity := promptContainsToken(promptText, "create_entity")
	saveEntity := promptContainsToken(promptText, "save_entity_field")
	if !saveEntity {
		return createEntity, false, nil
	}
	fields := make([]string, 0)
	for _, rawLine := range strings.Split(promptText, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || !promptContainsToken(line, "save_entity_field") {
			continue
		}
		matches := promptSaveFieldPattern.FindAllStringSubmatch(line, -1)
		for _, match := range matches {
			if len(match) < 2 {
				continue
			}
			field := strings.TrimSpace(match[1])
			if field == "" || field == "save_entity_field" {
				continue
			}
			fields = append(fields, field)
		}
	}
	return createEntity, true, uniquePromptStrings(fields)
}
