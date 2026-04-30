package tools

import (
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
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
	Usage              string
	RequiredPermission string
	HandlerType        implementationClass
	InputSchema        map[string]any
	OutputSchema       map[string]any
	GeneratedSchema    bool
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
			Name:            name,
			Description:     strings.TrimSpace(entry.Description),
			Usage:           strings.TrimSpace(entry.Usage),
			Schema:          deepCloneJSONValue(entry.InputSchema),
			GeneratedSchema: entry.GeneratedSchema,
		})
	}
	return defs, nil
}

func toolDefinitionsForActor(source semanticview.Source, actor models.AgentConfig, discovered map[string]runtimemcp.DiscoveredTool) ([]llm.ToolDefinition, error) {
	entries, err := registeredToolsForActor(source, actor, discovered)
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
			Name:            name,
			Description:     strings.TrimSpace(entry.Description),
			Usage:           strings.TrimSpace(entry.Usage),
			Schema:          deepCloneJSONValue(entry.InputSchema),
			GeneratedSchema: entry.GeneratedSchema,
		})
	}
	return defs, nil
}

func runtimeToolHiddenFromAgents(name string) bool {
	return false
}

func registeredToolsForRuntime(source semanticview.Source, discovered map[string]runtimemcp.DiscoveredTool) (map[string]RegisteredTool, error) {
	entries := builtinRegisteredTools(source, nil)
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
			if existing, ok := entries[name]; ok && existing.HandlerType == implementationPlatformBuiltin && registered.HandlerType == implementationPlatformBuiltin {
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
		schema = deepCloneMap(schema)
		entries[name] = RegisteredTool{
			Name:          name,
			Description:   strings.TrimSpace(tool.Description),
			HandlerType:   implementationMCP,
			InputSchema:   schema,
			MCPServerName: strings.TrimSpace(tool.ServerName),
			MCPRemoteName: strings.TrimSpace(tool.RemoteName),
		}
	}
	return entries, nil
}

func registeredToolsForActor(source semanticview.Source, actor models.AgentConfig, discovered map[string]runtimemcp.DiscoveredTool) (map[string]RegisteredTool, error) {
	entries, err := registeredToolsForRuntime(source, discovered)
	if err != nil {
		return nil, err
	}
	if !actorAllowsInternalLegacyEntityTools(actor) {
		removeLegacyEntityToolSurface(entries)
	}
	for name, entry := range builtinRegisteredTools(source, &actor) {
		entries[name] = entry
	}
	candidates := map[string]struct{}{}
	for name := range entries {
		candidates[strings.TrimSpace(name)] = struct{}{}
	}
	if allowed, _ := extractAllowedTools(actor); len(allowed) > 0 {
		for name := range allowed {
			candidates[strings.TrimSpace(name)] = struct{}{}
		}
	}
	if source != nil && strings.TrimSpace(actor.ID) != "" {
		for name := range candidates {
			entry, ok := source.ToolEntryForAgent(strings.TrimSpace(actor.ID), strings.TrimSpace(name))
			if !ok {
				continue
			}
			registered, include, err := registeredToolFromContract(strings.TrimSpace(name), entry)
			if err != nil {
				return nil, err
			}
			if !include {
				continue
			}
			if existing, ok := entries[strings.TrimSpace(name)]; ok && existing.HandlerType == implementationPlatformBuiltin && registered.HandlerType == implementationPlatformBuiltin {
				continue
			}
			entries[strings.TrimSpace(name)] = registered
		}
	}
	for _, def := range nativeFallbackToolDefinitionsForActor(actor) {
		name := strings.TrimSpace(def.Name)
		if name == "" {
			continue
		}
		tool, ok := nativeFallbackRegisteredTool(actor, name)
		if !ok {
			continue
		}
		entries[name] = tool
	}
	if !actorAllowsInternalLegacyEntityTools(actor) {
		removeLegacyEntityToolSurface(entries)
	}
	return entries, nil
}

func resolveRegisteredToolForActor(source semanticview.Source, actor models.AgentConfig, toolName string, discovered map[string]runtimemcp.DiscoveredTool) (RegisteredTool, bool, error) {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return RegisteredTool{}, false, nil
	}
	entries, err := registeredToolsForActor(source, actor, discovered)
	if err != nil {
		return RegisteredTool{}, false, err
	}
	tool, ok := entries[toolName]
	if !ok {
		return RegisteredTool{}, false, nil
	}
	return tool, true, nil
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
		Usage:              runtimeOwnedToolUsage(name),
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
	return toolSchemaToMap(schema), nil
}

func toolSchemaToMap(schema runtimecontracts.ToolInputSchema) map[string]any {
	out := map[string]any{}
	if value := strings.TrimSpace(schema.Type); value != "" {
		out["type"] = value
	}
	if value := strings.TrimSpace(schema.Description); value != "" {
		out["description"] = value
	}
	if len(schema.Properties) > 0 {
		props := make(map[string]any, len(schema.Properties))
		for name, prop := range schema.Properties {
			props[name] = toolSchemaToMap(prop)
		}
		out["properties"] = props
	}
	if len(schema.Required) > 0 {
		out["required"] = append([]string{}, schema.Required...)
	}
	if schema.Items != nil {
		out["items"] = toolSchemaToMap(*schema.Items)
	}
	if len(schema.Enum) > 0 {
		enumValues := make([]any, 0, len(schema.Enum))
		for _, literal := range schema.Enum {
			enumValues = append(enumValues, schemaLiteralValue(literal))
		}
		out["enum"] = enumValues
	}
	if schema.AdditionalProperties.Allowed != nil {
		out["additionalProperties"] = *schema.AdditionalProperties.Allowed
	} else if schema.AdditionalProperties.Schema != nil {
		out["additionalProperties"] = toolSchemaToMap(*schema.AdditionalProperties.Schema)
	}
	if schema.Minimum != nil {
		out["minimum"] = *schema.Minimum
	}
	if schema.Maximum != nil {
		out["maximum"] = *schema.Maximum
	}
	return out
}

func schemaLiteralValue(literal runtimecontracts.SchemaLiteral) any {
	if literal.Node.Kind == 0 {
		return nil
	}
	var out any
	if err := literal.Node.Decode(&out); err != nil {
		return strings.TrimSpace(literal.Node.Value)
	}
	return out
}

func deepCloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	cloned := deepCloneJSONValue(in)
	out, _ := cloned.(map[string]any)
	return out
}
