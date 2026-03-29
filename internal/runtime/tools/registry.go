package tools

import (
	"encoding/json"
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	llm "swarm/internal/runtime/llm"
	runtimemcp "swarm/internal/runtime/mcp"
	"swarm/internal/runtime/semanticview"
)

type implementationClass string

const (
	implementationPlatformBuiltin implementationClass = "platform_builtin"
	implementationHTTP            implementationClass = "http"
	implementationMCP             implementationClass = "mcp"
)

type RegisteredTool struct {
	Name               string
	Category           string
	Description        string
	RequiredPermission string
	HandlerType        implementationClass
	InputSchema        map[string]any
	OutputSchema       map[string]any
	HTTP               *runtimecontracts.HTTPToolSpec
	ResponseMapping    map[string]any
	Credentials        []string
	MCPServerName      string
	MCPRemoteName      string
}

func toolDefinitionsForRuntime(source semanticview.Source, discovered map[string]runtimemcp.DiscoveredTool) ([]llm.ToolDefinition, error) {
	entries, err := registeredToolsForRuntime(source, discovered)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		if runtimeToolHiddenFromAgents(name) {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	defs := make([]llm.ToolDefinition, 0, len(names))
	for _, name := range names {
		entry := entries[name]
		defs = append(defs, llm.ToolDefinition{
			Name:        name,
			Description: strings.TrimSpace(entry.Description),
			Schema:      deepCloneJSONValue(entry.InputSchema),
		})
	}
	return defs, nil
}

func runtimeToolHiddenFromAgents(name string) bool {
	return false
}

func registeredToolsForRuntime(source semanticview.Source, discovered map[string]runtimemcp.DiscoveredTool) (map[string]RegisteredTool, error) {
	entries := builtinRegisteredTools()
	if source != nil {
		for name, entry := range source.ToolEntries() {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			registered, include, err := registeredToolFromContract(name, entry)
			if err != nil {
				return nil, err
			}
			if !include {
				continue
			}
			entries[name] = registered
		}
	}
	for name, tool := range discovered {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		schema, _ := tool.InputSchema.(map[string]any)
		entries[name] = RegisteredTool{
			Name:          name,
			Description:   strings.TrimSpace(tool.Description),
			HandlerType:   implementationMCP,
			InputSchema:   deepCloneJSONValue(schema).(map[string]any),
			MCPServerName: strings.TrimSpace(tool.ServerName),
			MCPRemoteName: strings.TrimSpace(tool.RemoteName),
		}
	}
	return entries, nil
}

func resolveRegisteredToolForActor(source semanticview.Source, actorID, toolName string, discovered map[string]runtimemcp.DiscoveredTool) (RegisteredTool, bool, error) {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return RegisteredTool{}, false, nil
	}
	if tool, ok := builtinRegisteredTools()[toolName]; ok {
		return tool, true, nil
	}
	if discovered != nil {
		if tool, ok := discovered[toolName]; ok {
			schema, _ := tool.InputSchema.(map[string]any)
			return RegisteredTool{
				Name:          toolName,
				Description:   strings.TrimSpace(tool.Description),
				HandlerType:   implementationMCP,
				InputSchema:   schema,
				MCPServerName: strings.TrimSpace(tool.ServerName),
				MCPRemoteName: strings.TrimSpace(tool.RemoteName),
			}, true, nil
		}
	}
	if source == nil {
		return RegisteredTool{}, false, nil
	}
	entry, ok := source.ToolEntryForAgent(strings.TrimSpace(actorID), toolName)
	if !ok {
		entry, ok = source.ToolEntries()[toolName]
	}
	if !ok {
		return RegisteredTool{}, false, nil
	}
	tool, include, err := registeredToolFromContract(toolName, entry)
	return tool, include, err
}

func registeredToolFromContract(name string, entry runtimecontracts.ToolSchemaEntry) (RegisteredTool, bool, error) {
	handlerType := normalizeImplementationClass(name, entry)
	if handlerType == "" {
		return RegisteredTool{}, false, nil
	}
	inputSchema, err := schemaToMap(entry.InputSchema)
	if err != nil {
		return RegisteredTool{}, false, err
	}
	outputSchema, err := schemaToMap(entry.OutputSchema)
	if err != nil {
		return RegisteredTool{}, false, err
	}
	return RegisteredTool{
		Name:               strings.TrimSpace(name),
		Category:           strings.TrimSpace(entry.Category),
		Description:        strings.TrimSpace(entry.Description),
		RequiredPermission: strings.TrimSpace(toolRequiredPermission(name, entry)),
		HandlerType:        handlerType,
		InputSchema:        inputSchema,
		OutputSchema:       outputSchema,
		HTTP:               entry.HTTP,
		ResponseMapping:    deepCloneMap(entry.ResponseMapping),
		Credentials:        append([]string{}, entry.Credentials...),
	}, true, nil
}

func normalizeImplementationClass(name string, entry runtimecontracts.ToolSchemaEntry) implementationClass {
	if strings.TrimSpace(name) == "configure_routing" {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(entry.HandlerType)) {
	case "":
		if _, ok := supportedRuntimeToolNames[strings.TrimSpace(name)]; ok {
			return implementationPlatformBuiltin
		}
		if entry.HTTP != nil {
			return implementationHTTP
		}
		return ""
	case "platform_builtin":
		return implementationPlatformBuiltin
	case "http":
		return implementationHTTP
	case "mcp":
		return implementationMCP
	case "workflow_registered", "api_call":
		if entry.HTTP != nil {
			return implementationHTTP
		}
	}
	return ""
}

func builtinRegisteredTools() map[string]RegisteredTool {
	entries := builtinRuntimeContractSchemas()
	out := make(map[string]RegisteredTool, len(entries))
	for name, entry := range entries {
		out[name] = RegisteredTool{
			Name:        strings.TrimSpace(name),
			Category:    strings.TrimSpace(entry.Category),
			Description: strings.TrimSpace(entry.Description),
			HandlerType: implementationPlatformBuiltin,
			InputSchema: deepCloneMap(entry.InputSchema),
		}
	}
	return out
}

func toolRequiredPermission(toolID string, entry runtimecontracts.ToolSchemaEntry) string {
	if perm := strings.TrimSpace(entry.Permission); perm != "" {
		return perm
	}
	if perm := strings.TrimSpace(entry.RequiredPermission); perm != "" {
		return perm
	}
	return ""
}

func schemaToMap(schema runtimecontracts.ToolInputSchema) (map[string]any, error) {
	if schema.Type == "" && len(schema.Properties) == 0 && len(schema.Required) == 0 && schema.Items == nil && len(schema.Enum) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func deepCloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	cloned := deepCloneJSONValue(in)
	out, _ := cloned.(map[string]any)
	return out
}
