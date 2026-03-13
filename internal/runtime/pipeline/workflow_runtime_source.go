package pipeline

import "strings"

const runtimeWorkflowID = "workflow-runtime"

func isRuntimeWorkflowSource(source string) bool {
	return strings.TrimSpace(source) == runtimeWorkflowID
}
