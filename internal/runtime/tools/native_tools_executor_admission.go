package tools

import (
	"context"
	"fmt"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func (e *Executor) ValidateNativeToolAdmission(ctx context.Context, actor models.AgentConfig) error {
	if e == nil || !actor.NativeTools.Any() {
		return nil
	}
	opts, err := e.nativeToolAdmissionOptions(actor)
	if err != nil {
		return err
	}
	return ValidateNativeToolAgentAdmission(ctx, actor, opts)
}

func (e *Executor) nativeToolAdmissionOptions(actor models.AgentConfig) (NativeToolAdmissionOptions, error) {
	if e == nil {
		return NativeToolAdmissionOptions{}, nil
	}
	e.mu.RLock()
	runtimes := e.modelRuntimes
	credentials := e.credentials
	source := e.workflowSource
	workspaces := e.workspaces
	e.mu.RUnlock()
	if runtimes == nil {
		return NativeToolAdmissionOptions{}, fmt.Errorf("agent llm runtime resolver is required")
	}
	runtime, err := runtimes.RuntimeForAgent(actor)
	if err != nil {
		return NativeToolAdmissionOptions{}, err
	}
	return NativeToolAdmissionOptions{
		Runtime:     runtime,
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
	opts, err := e.nativeToolAdmissionOptions(actor)
	if err != nil {
		return false, err.Error()
	}
	for _, decision := range NativeToolAdmissionDecisions(ctx, actor, opts) {
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
