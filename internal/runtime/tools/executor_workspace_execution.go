package tools

import (
	"context"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func nativeExecutionCapabilityForTool(name string) workspace.ExecutionCapability {
	switch normalizeNativeToolName(name) {
	case "bash":
		return workspace.ExecutionCapabilityNativeCommand
	case "read_file":
		return workspace.ExecutionCapabilityFileRead
	case "write_file":
		return workspace.ExecutionCapabilityFileWrite
	default:
		return ""
	}
}

func (e *Executor) nativeWorkspaceExecutionDecision(ctx context.Context, actor models.AgentConfig, name string) (bool, string) {
	capability := nativeExecutionCapabilityForTool(name)
	if capability == "" {
		return true, ""
	}
	target, err := e.resolveNativeWorkspace(ctx, actor)
	if err != nil {
		return false, err.Error()
	}
	execTarget := target.ExecutionTarget()
	if !execTarget.Supports(capability) {
		return false, execTarget.UnsupportedMessage(capability)
	}
	return true, ""
}

func (e *Executor) applyWorkspaceExecutionDecision(ctx context.Context, actor models.AgentConfig, cap toolcapabilities.Capability) toolcapabilities.Capability {
	if !cap.Visible && !cap.Callable {
		return cap
	}
	if _, ok := nativeFallbackRegisteredTool(actor, cap.Name); !ok {
		return cap
	}
	ok, reason := e.nativeWorkspaceExecutionDecision(ctx, actor, cap.Name)
	if ok {
		return cap
	}
	cap.Visible = false
	cap.Callable = false
	if strings.TrimSpace(reason) == "" {
		reason = "workspace_execution_unsupported"
	}
	cap.DenialReason = reason
	return cap
}

func (e *Executor) filterNativeDefinitionsForWorkspaceExecution(ctx context.Context, actor models.AgentConfig, defs []llm.ToolDefinition) []llm.ToolDefinition {
	filtered := make([]llm.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		if _, ok := nativeFallbackRegisteredTool(actor, def.Name); !ok {
			filtered = append(filtered, def)
			continue
		}
		ok, _ := e.nativeWorkspaceExecutionDecision(ctx, actor, def.Name)
		if ok {
			filtered = append(filtered, def)
		}
	}
	return filtered
}
