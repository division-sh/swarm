package pipeline

import (
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func workflowGuardExecutionKey(entry runtimecontracts.GuardActionEntry) string {
	if builtin := strings.TrimSpace(entry.PlatformBuiltin); builtin != "" {
		return normalizeWorkflowBuiltinGuardID(builtin)
	}
	return strings.TrimSpace(entry.ID)
}

func workflowActionExecutionKey(entry runtimecontracts.GuardActionEntry) string {
	if builtin := strings.TrimSpace(entry.PlatformBuiltin); builtin != "" {
		return normalizeWorkflowBuiltinActionID(builtin)
	}
	return strings.TrimSpace(entry.ID)
}

func isExecutableWorkflowGuardEntry(entry runtimecontracts.GuardActionEntry) bool {
	if strings.TrimSpace(entry.Check) != "" {
		return true
	}
	return isSupportedWorkflowGuardBuiltin(firstNonEmptyString(entry.PlatformBuiltin, entry.ID))
}

func isExecutableWorkflowActionEntry(entry runtimecontracts.GuardActionEntry) bool {
	if strings.TrimSpace(entry.Emits) != "" {
		return true
	}
	return isSupportedWorkflowActionBuiltin(firstNonEmptyString(entry.PlatformBuiltin, entry.ID))
}
