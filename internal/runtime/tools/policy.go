package tools

import "strings"

var universalRuntimeTools = map[string]struct{}{
	"agent_message":     {},
	"mailbox_send":      {},
	"get_entity":        {},
	"save_entity_field": {},
	"query_entities":    {},
	"search_entities":   {},
	"query_metrics":     {},
}

var entityScopedUniversalRuntimeTools = map[string]struct{}{
	"get_entity":        {},
	"save_entity_field": {},
	"query_entities":    {},
	"search_entities":   {},
	"query_metrics":     {},
}

func IsUniversal(toolName string) bool {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return false
	}
	_, ok := universalRuntimeTools[toolName]
	return ok
}

func isEntityScopedUniversalRuntimeTool(toolName string) bool {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return false
	}
	_, ok := entityScopedUniversalRuntimeTools[toolName]
	return ok
}
