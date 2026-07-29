package tools

import (
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
)

const runtimeToolsMCPPrefix = toolidentity.RuntimeToolsMCPPrefix

func nativeFallbackExecutionTool(actor models.AgentConfig, name string) (ExecutionTool, bool) {
	name = strings.TrimSpace(name)
	switch name {
	case "bash":
		if !nativeToolCapabilityEnabledForActor(actor, "bash") {
			return ExecutionTool{}, false
		}
		return newExecutionTool(executionToolValue{
			name: name, description: "Execute a shell command locally in the agent workspace.",
			usage: runtimeOwnedToolUsage(name), handler: implementationPlatformBuiltin,
			inputSchema: ObjectSchema(map[string]any{
				"command":         map[string]any{"type": "string"},
				"timeout_seconds": map[string]any{"type": "integer", "minimum": 1, "maximum": 300},
			}, "command"),
		}), true
	case "web_search":
		if !nativeToolCapabilityEnabledForActor(actor, "web_search") {
			return ExecutionTool{}, false
		}
		return newExecutionTool(executionToolValue{
			name: name, description: "Search the web and return normalized results.",
			usage: runtimeOwnedToolUsage(name), handler: implementationPlatformBuiltin,
			inputSchema: ObjectSchema(map[string]any{
				"query":       map[string]any{"type": "string"},
				"max_results": map[string]any{"type": "integer", "minimum": 1, "maximum": 20},
			}, "query"),
		}), true
	case "read_file":
		if !nativeToolCapabilityEnabledForActor(actor, "file_io") {
			return ExecutionTool{}, false
		}
		return newExecutionTool(executionToolValue{
			name: name, description: "Read a file from the workspace or mounted read-only paths.",
			usage: runtimeOwnedToolUsage(name), handler: implementationPlatformBuiltin,
			inputSchema: ObjectSchema(map[string]any{
				"path": map[string]any{"type": "string"},
			}, "path"),
		}), true
	case "write_file":
		if !nativeToolCapabilityEnabledForActor(actor, "file_io") {
			return ExecutionTool{}, false
		}
		return newExecutionTool(executionToolValue{
			name: name, description: "Write a file within the agent workspace.",
			usage: runtimeOwnedToolUsage(name), handler: implementationPlatformBuiltin,
			inputSchema: ObjectSchema(map[string]any{
				"path":    map[string]any{"type": "string"},
				"content": map[string]any{"type": "string"},
			}, "path", "content"),
		}), true
	default:
		return ExecutionTool{}, false
	}
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

func nativeFallbackToolDefinitionsForActor(actor models.AgentConfig) []llm.ToolDefinition {
	defs := make([]llm.ToolDefinition, 0, 4)
	for _, name := range []string{"bash", "web_search", "read_file", "write_file"} {
		tool, ok := nativeFallbackExecutionTool(actor, name)
		if !ok {
			continue
		}
		defs = append(defs, llm.ToolDefinition{
			Name:        tool.Name(),
			Description: tool.Description(),
			Usage:       tool.Usage(),
			Schema:      tool.InputSchema(),
		})
	}
	return defs
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
