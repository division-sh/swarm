package runtime

import "strings"

var universalRuntimeTools = map[string]struct{}{
	"agent_message": {},
	"mailbox_send":  {},
}

func IsUniversalRuntimeTool(toolName string) bool {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return false
	}
	_, ok := universalRuntimeTools[toolName]
	return ok
}
