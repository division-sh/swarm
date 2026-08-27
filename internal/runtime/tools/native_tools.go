package tools

import (
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
)

func nativeFallbackExecutionTool(actor models.AgentConfig, name string) (ExecutionTool, bool, error) {
	name = strings.TrimSpace(name)
	var draft builtinToolDraft
	switch name {
	case "bash":
		if !nativeToolCapabilityEnabledForActor(actor, "bash") {
			return ExecutionTool{}, false, nil
		}
		draft = builtinToolDraft{
			Description: "Execute a shell command locally in the agent workspace.",
			InputSchema: ObjectSchema(map[string]any{
				"command":         map[string]any{"type": "string"},
				"timeout_seconds": map[string]any{"type": "integer", "minimum": 1, "maximum": 300},
			}, "command"),
		}
	case "web_search":
		if !nativeToolCapabilityEnabledForActor(actor, "web_search") {
			return ExecutionTool{}, false, nil
		}
		draft = builtinToolDraft{
			Description: "Search the web and return normalized results.",
			InputSchema: ObjectSchema(map[string]any{
				"query":       map[string]any{"type": "string"},
				"max_results": map[string]any{"type": "integer", "minimum": 1, "maximum": 20},
			}, "query"),
		}
	case "read_file":
		if !nativeToolCapabilityEnabledForActor(actor, "file_io") {
			return ExecutionTool{}, false, nil
		}
		draft = builtinToolDraft{
			Description: "Read a file from the workspace or mounted read-only paths.",
			InputSchema: ObjectSchema(map[string]any{
				"path": map[string]any{"type": "string"},
			}, "path"),
		}
	case "write_file":
		if !nativeToolCapabilityEnabledForActor(actor, "file_io") {
			return ExecutionTool{}, false, nil
		}
		draft = builtinToolDraft{
			Description: "Write a file within the agent workspace.",
			InputSchema: ObjectSchema(map[string]any{
				"path":    map[string]any{"type": "string"},
				"content": map[string]any{"type": "string"},
			}, "path", "content"),
		}
	default:
		return ExecutionTool{}, false, nil
	}
	tool, err := admitBuiltinExecutionTool(name, draft)
	return tool, err == nil, err
}

func nativeToolCapabilityEnabledForActor(actor models.AgentConfig, capability string) bool {
	capability = strings.TrimSpace(capability)
	if capability == "" {
		return false
	}
	return actor.NativeTools.Enabled(capability)
}

func normalizeNativeToolName(name string) string {
	return toolidentity.CanonicalName(name)
}

func nativeToolNameForCapability(capability string) []string {
	switch strings.TrimSpace(capability) {
	case "bash":
		return []string{"bash"}
	case "web_search":
		return []string{"web_search"}
	case "file_io":
		return []string{"read_file", "write_file"}
	default:
		return nil
	}
}
