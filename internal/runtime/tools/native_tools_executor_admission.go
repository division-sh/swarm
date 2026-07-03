package tools

import (
	"context"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func (e *Executor) ValidateNativeToolAdmission(ctx context.Context, actor models.AgentConfig) error {
	if e == nil || !actor.NativeTools.Any() {
		return nil
	}
	return ValidateNativeToolAgentAdmission(ctx, actor, e.nativeToolAdmissionOptions())
}

func (e *Executor) nativeToolAdmissionOptions() NativeToolAdmissionOptions {
	if e == nil {
		return NativeToolAdmissionOptions{}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return NativeToolAdmissionOptions{
		Runtime:     e.modelRuntime,
		Credentials: e.credentials,
		Source:      e.workflowSource,
		Workspaces:  e.workspaces,
	}
}

func (e *Executor) nativeToolAdmittedForTool(ctx context.Context, actor models.AgentConfig, toolName string) bool {
	admitted, _ := e.nativeToolAdmissionForTool(ctx, actor, toolName)
	return admitted
}

func (e *Executor) nativeToolAdmissionForTool(ctx context.Context, actor models.AgentConfig, toolName string) (bool, string) {
	toolName = normalizeNativeToolName(strings.TrimSpace(toolName))
	if !isNativeFallbackToolName(toolName) {
		return true, ""
	}
	for _, decision := range NativeToolAdmissionDecisions(ctx, actor, e.nativeToolAdmissionOptions()) {
		for _, name := range decision.ToolNames {
			if normalizeNativeToolName(name) != toolName {
				continue
			}
			if decision.FallbackAdmitted {
				return true, ""
			}
			if decision.ProviderNativeAdmitted {
				return false, nativeToolProviderOnlyFallbackDeny
			}
			return false, decision.DenialReason
		}
	}
	return false, "native tool capability is not enabled"
}

func isNativeFallbackToolName(name string) bool {
	switch normalizeNativeToolName(name) {
	case "bash", "web_search", "read_file", "write_file":
		return true
	default:
		return false
	}
}
