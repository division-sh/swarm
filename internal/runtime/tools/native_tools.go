package tools

import (
	"encoding/json"
	"strings"

	models "swarm/internal/runtime/core/actors"
)

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
	if capability == "" || len(actor.Config) == 0 || !json.Valid(actor.Config) {
		return false
	}
	var parsed map[string]any
	if err := json.Unmarshal(actor.Config, &parsed); err != nil {
		return false
	}
	items, ok := parsed["native_tools"].(map[string]any)
	if !ok {
		return false
	}
	flag, ok := items[capability].(bool)
	return ok && flag
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
