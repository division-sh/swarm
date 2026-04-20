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
	agentIDs := make([]string, 0, len(bundle.Agents))
	for agentID := range bundle.Agents {
		agentID = strings.TrimSpace(agentID)
		if agentID != "" {
			agentIDs = append(agentIDs, agentID)
		}
	}
	sort.Strings(agentIDs)

	out := make([]PromptEntityWriteEvidence, 0)
	for _, agentID := range agentIDs {
		entry, ok := bundle.Agents[agentID]
		if !ok {
			continue
		}
		source, ok := bundle.AgentContractSource(agentID)
		if !ok {
			continue
		}
		path, text, ok, err := loadPromptEntityWriteText(bundle, agentID, entry, source)
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
			PromptFile:   path,
			CreateEntity: createEntity,
			SaveEntity:   saveEntity,
			SaveFields:   saveFields,
		})
	}
	return out, nil
}

func loadPromptEntityWriteText(bundle *WorkflowContractBundle, agentID string, entry AgentRegistryEntry, source ContractItemSource) (string, string, bool, error) {
	dirs := promptDirsForBundleAgent(bundle, agentID)
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
			fields = append(fields, strings.TrimSpace(match[1]))
		}
	}
	return createEntity, true, uniquePromptStrings(fields)
}
