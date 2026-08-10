package contracts

import (
	"strings"
)

type PromptSchemaGuardCase struct {
	IntentSource     string
	IntentContent    string
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
	records := bundleAgentRecords(bundle)
	cases := make([]PromptSchemaGuardCase, 0, len(records))
	seen := map[string]struct{}{}
	for _, record := range records {
		entry := record.Entry
		if len(entry.EmitEvents) == 0 {
			continue
		}
		if err := entry.ResolvedIntent.Validate(); err != nil {
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
			key := entry.ResolvedIntent.Identity + "|" + emitEvent
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			cases = append(cases, PromptSchemaGuardCase{
				IntentSource:     entry.ResolvedIntent.Coordinate,
				IntentContent:    entry.ResolvedIntent.Content,
				EmitTool:         "emit_" + strings.ReplaceAll(emitEvent, ".", "_"),
				RequiredTopLevel: required,
			})
		}
	}
	return cases
}
