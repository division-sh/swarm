package tools

import (
	"context"
	"fmt"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func (e *Executor) ValidateNativeToolCapabilityAdmission(ctx context.Context, actor models.AgentConfig) error {
	if e == nil || !actor.NativeTools.Any() {
		return nil
	}
	resolvedActor, opts, err := e.nativeToolAdmissionOptions(actor)
	if err != nil {
		return err
	}
	if resolvedActor.ExecutionMode == runtimeeffects.ExecutionModeMock {
		return nil
	}
	return validateNativeToolAgentCapabilityAdmission(ctx, resolvedActor, opts)
}

func (e *Executor) nativeToolAdmissionOptions(actor models.AgentConfig) (models.AgentConfig, NativeToolAdmissionOptions, error) {
	if e == nil {
		return actor, NativeToolAdmissionOptions{}, nil
	}
	e.mu.RLock()
	runtimes := e.modelRuntimes
	credentials := e.credentials
	source := e.workflowSource
	workspaces := e.workspaces
	e.mu.RUnlock()
	if runtimes == nil {
		return actor, NativeToolAdmissionOptions{}, fmt.Errorf("agent llm runtime resolver is required")
	}
	resolved, err := runtimes.ResolveAgentRuntime(actor)
	if err != nil {
		return actor, NativeToolAdmissionOptions{}, err
	}
	return resolved.Actor, NativeToolAdmissionOptions{
		Runtime:     resolved.Runtime,
		Credentials: credentials,
		Source:      source,
		Workspaces:  workspaces,
	}, nil
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
	resolvedActor, opts, err := e.nativeToolAdmissionOptions(actor)
	if err != nil {
		return false, err.Error()
	}
	for _, decision := range NativeToolAdmissionDecisions(ctx, resolvedActor, opts) {
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
