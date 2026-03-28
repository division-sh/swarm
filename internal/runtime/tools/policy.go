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

func IsUniversal(toolName string) bool {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return false
	}
	_, ok := universalRuntimeTools[toolName]
	return ok
}
