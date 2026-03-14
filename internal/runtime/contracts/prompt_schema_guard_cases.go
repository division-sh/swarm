package contracts

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type PromptSchemaGuardCase struct {
	PromptFile       string
	EmitTool         string
	RequiredTopLevel []string
	ForbiddenTokens  []string
}

func PromptSchemaGuards() []PromptSchemaGuardCase {
	return nil
}

func DerivePromptSchemaGuards(bundle *WorkflowContractBundle) []PromptSchemaGuardCase {
	if bundle == nil {
		return nil
	}
	agentIDs := make([]string, 0, len(bundle.AgentEntries()))
	for agentID := range bundle.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		if agentID != "" {
			agentIDs = append(agentIDs, agentID)
		}
	}
	sort.Strings(agentIDs)

	cases := make([]PromptSchemaGuardCase, 0, len(agentIDs))
	seen := map[string]struct{}{}
	for _, agentID := range agentIDs {
		entry := bundle.AgentEntries()[agentID]
		if len(entry.EmitEvents) == 0 {
			continue
		}
		promptFile := resolvePromptSchemaGuardFile(bundle, agentID, entry)
		if promptFile == "" {
			continue
		}
		for _, emitEvent := range entry.EmitEvents {
			emitEvent = strings.TrimSpace(emitEvent)
			if emitEvent == "" {
				continue
			}
			eventEntry, ok := bundle.EventEntry(emitEvent)
			if !ok {
				continue
			}
			required := normalizeStrings(eventEntry.Payload.Required)
			if len(required) == 0 {
				continue
			}
			key := promptFile + "|" + emitEvent
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			cases = append(cases, PromptSchemaGuardCase{
				PromptFile:       promptFile,
				EmitTool:         "emit_" + strings.ReplaceAll(emitEvent, ".", "_"),
				RequiredTopLevel: required,
			})
		}
	}
	return cases
}

func resolvePromptSchemaGuardFile(bundle *WorkflowContractBundle, agentID string, entry AgentRegistryEntry) string {
	if bundle == nil {
		return ""
	}
	candidates := make([]string, 0, 4)
	for _, candidate := range []string{agentID, entry.ID} {
		candidate = strings.TrimSpace(candidate)
		if candidate != "" {
			candidates = append(candidates, candidate)
		}
	}
	candidates = uniqueStrings(candidates...)
	dirs := append(promptDirsForBundleAgent(bundle, agentID), promptBundlePromptDirs(bundle)...)
	for _, dir := range uniqueStrings(dirs...) {
		for _, candidate := range candidates {
			path := filepath.Join(strings.TrimSpace(dir), candidate+".md")
			info, err := os.Stat(path)
			if err == nil && !info.IsDir() {
				return path
			}
		}
	}
	return ""
}
