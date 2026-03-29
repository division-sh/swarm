package tools

import (
	"fmt"
	"sort"
	"strings"

	"swarm/internal/runtime/semanticview"
)

func ValidateToolImplementations(source semanticview.Source) ([]error, error) {
	if source == nil {
		return nil, nil
	}
	entries := source.ToolEntries()
	names := make([]string, 0, len(entries))
	for name := range entries {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	warnings := make([]error, 0)
	for _, name := range names {
		entry := entries[name]
		rawType := strings.ToLower(strings.TrimSpace(entry.HandlerType))
		normalized := normalizeImplementationClass(name, entry)
		switch rawType {
		case "workflow_registered", "api_call":
			warnings = append(warnings, fmt.Errorf("tool %s uses deprecated handler_type %s; migrate to handler_type: http or mcp", name, rawType))
		}
		switch normalized {
		case implementationPlatformBuiltin:
			if _, ok := supportedRuntimeToolNames[name]; !ok {
				return warnings, fmt.Errorf("tool %s declares handler_type platform_builtin but is not shipped by the generic runtime", name)
			}
		case implementationHTTP:
			if entry.HTTP == nil {
				return warnings, fmt.Errorf("tool %s resolves as http but has no http block", name)
			}
			if strings.TrimSpace(entry.HTTP.Method) == "" {
				return warnings, fmt.Errorf("tool %s http.method is required", name)
			}
			if strings.TrimSpace(entry.HTTP.URL) == "" {
				return warnings, fmt.Errorf("tool %s http.url is required", name)
			}
		case implementationMCP:
			if !strings.Contains(name, ".") {
				warnings = append(warnings, fmt.Errorf("tool %s uses handler_type mcp but is not prefixed with a server namespace", name))
			}
		case "":
			if rawType == "workflow_registered" || rawType == "api_call" {
				warnings = append(warnings, fmt.Errorf("tool %s uses deprecated handler_type %s with no http block; tool is ignored until migrated to handler_type: http or mcp", name, rawType))
				continue
			}
			if rawType == "" {
				warnings = append(warnings, fmt.Errorf("tool %s has no executable implementation in the generic runtime; provide handler_type: http with an http block or expose it via mcp", name))
				continue
			}
			return warnings, fmt.Errorf("tool %s has unsupported handler_type %q", name, strings.TrimSpace(entry.HandlerType))
		}
	}
	return warnings, nil
}
