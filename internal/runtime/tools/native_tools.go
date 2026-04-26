package tools

import (
	"strings"

	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolidentity"
	llm "swarm/internal/runtime/llm"
)

const runtimeToolsMCPPrefix = toolidentity.RuntimeToolsMCPPrefix

func nativeFallbackRegisteredTool(actor models.AgentConfig, name string) (RegisteredTool, bool) {
	name = strings.TrimSpace(name)
	switch name {
	case "bash":
		if !nativeToolCapabilityEnabledForActor(actor, "bash") {
			return RegisteredTool{}, false
		}
		return RegisteredTool{
			Name:        name,
			Description: "Execute a shell command locally in the agent workspace.",
			Usage:       runtimeOwnedToolUsage(name),
			HandlerType: implementationPlatformBuiltin,
			InputSchema: ObjectSchema(map[string]any{
				"command":         map[string]any{"type": "string"},
				"timeout_seconds": map[string]any{"type": "integer", "minimum": 1, "maximum": 300},
			}, "command"),
		}, true
	case "web_search":
		if !nativeToolCapabilityEnabledForActor(actor, "web_search") {
			return RegisteredTool{}, false
		}
		return RegisteredTool{
			Name:        name,
			Description: "Search the web and return normalized results.",
			Usage:       runtimeOwnedToolUsage(name),
			HandlerType: implementationPlatformBuiltin,
			InputSchema: ObjectSchema(map[string]any{
				"query":       map[string]any{"type": "string"},
				"max_results": map[string]any{"type": "integer", "minimum": 1, "maximum": 20},
			}, "query"),
		}, true
	case "read_file":
		if !nativeToolCapabilityEnabledForActor(actor, "file_io") {
			return RegisteredTool{}, false
		}
		return RegisteredTool{
			Name:        name,
			Description: "Read a file from the workspace or mounted read-only paths.",
			Usage:       runtimeOwnedToolUsage(name),
			HandlerType: implementationPlatformBuiltin,
			InputSchema: ObjectSchema(map[string]any{
				"path": map[string]any{"type": "string"},
			}, "path"),
		}, true
	case "write_file":
		if !nativeToolCapabilityEnabledForActor(actor, "file_io") {
			return RegisteredTool{}, false
		}
		return RegisteredTool{
			Name:        name,
			Description: "Write a file within the agent workspace.",
			Usage:       runtimeOwnedToolUsage(name),
			HandlerType: implementationPlatformBuiltin,
			InputSchema: ObjectSchema(map[string]any{
				"path":    map[string]any{"type": "string"},
				"content": map[string]any{"type": "string"},
			}, "path", "content"),
		}, true
	default:
		return RegisteredTool{}, false
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
		tool, ok := nativeFallbackRegisteredTool(actor, name)
		if !ok {
			continue
		}
		defs = append(defs, llm.ToolDefinition{
			Name:        tool.Name,
			Description: strings.TrimSpace(tool.Description),
			Usage:       strings.TrimSpace(tool.Usage),
			Schema:      deepCloneJSONValue(tool.InputSchema),
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
