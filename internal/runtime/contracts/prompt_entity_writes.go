package contracts

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

type PromptEntityWriteEvidence struct {
	AgentID      string
	Source       ContractItemSource
	Entry        AgentRegistryEntry
	IntentSource string
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
		path, text, ok, err := loadPromptEntityWriteText(record.Entry)
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
			IntentSource: path,
			CreateEntity: createEntity,
			SaveEntity:   saveEntity,
			SaveFields:   saveFields,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AgentID == out[j].AgentID {
			if out[i].Source.FlowPath == out[j].Source.FlowPath {
				return out[i].IntentSource < out[j].IntentSource
			}
			return out[i].Source.FlowPath < out[j].Source.FlowPath
		}
		return out[i].AgentID < out[j].AgentID
	})
	return out, nil
}

func loadPromptEntityWriteText(entry AgentRegistryEntry) (string, string, bool, error) {
	if err := entry.ResolvedIntent.Validate(); err != nil {
		return "", "", false, err
	}
	return entry.ResolvedIntent.Coordinate, entry.ResolvedIntent.Content, true, nil
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
