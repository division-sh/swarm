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

var promptSaveFieldPattern = regexp.MustCompile("`([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)`")

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
	mode := promptFlowPromptMode(bundle, source.FlowID)
	resolution, ok, err := ResolvePromptFileForContractAgent(bundle, agentID, entry, source, mode)
	if err != nil {
		return "", "", false, err
	}
	if !ok {
		return "", "", false, nil
	}
	raw, err := os.ReadFile(resolution.Path)
	if err != nil {
		return "", "", false, fmt.Errorf("read %s: %w", resolution.Path, err)
	}
	return resolution.Path, string(raw), true, nil
}

func extractPromptEntityWriteEvidence(promptText string) (bool, bool, []string) {
	createEntity := promptContainsToken(promptText, "create_entity")
	saveEntity := promptContainsToken(promptText, "save_entity_field")
	if !saveEntity {
		return createEntity, false, nil
	}
	fields := make([]string, 0)
	collectingSaveFields := false
	sawSaveFieldInBlock := false
	blankContinuationBudget := 0
	for _, rawLine := range strings.Split(promptText, "\n") {
		line := strings.TrimSpace(rawLine)
		lineHasSaveEntity := promptContainsToken(line, "save_entity_field")
		switch {
		case lineHasSaveEntity:
			collectingSaveFields = true
			sawSaveFieldInBlock = false
			blankContinuationBudget = 1
		case !collectingSaveFields:
			continue
		case line == "":
			if sawSaveFieldInBlock || blankContinuationBudget == 0 {
				collectingSaveFields = false
				sawSaveFieldInBlock = false
				blankContinuationBudget = 0
			} else {
				blankContinuationBudget--
			}
			continue
		}
		matches := promptSaveFieldPattern.FindAllStringSubmatch(line, -1)
		collectedFromLine := false
		for _, match := range matches {
			if len(match) < 2 {
				continue
			}
			field := strings.TrimSpace(match[1])
			if promptEntityWriteToolToken(field) {
				continue
			}
			fields = append(fields, field)
			collectedFromLine = true
		}
		if collectedFromLine {
			sawSaveFieldInBlock = true
			blankContinuationBudget = 0
		}
	}
	return createEntity, true, uniquePromptStrings(fields)
}

func promptEntityWriteToolToken(token string) bool {
	switch strings.TrimSpace(token) {
	case "", "create_entity", "get_entity", "query_entities", "query_metrics", "save_entity_field", "search_entities":
		return true
	default:
		return false
	}
}
