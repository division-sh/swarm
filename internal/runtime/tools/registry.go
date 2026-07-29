package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type implementationClass string

const (
	implementationPlatformBuiltin implementationClass = "platform_builtin"
	implementationHTTP            implementationClass = "http"
	implementationMCP             implementationClass = "mcp"
	implementationChannel         implementationClass = "channel"
)

type executionToolValue struct {
	name               string
	category           string
	description        string
	usage              string
	requiredPermission string
	handler            implementationClass
	inputSchema        map[string]any
	outputSchema       map[string]any
	generatedSchema    bool
	http               *runtimecontracts.HTTPToolSpec
	responseMapping    map[string]any
	responseSuccess    *runtimecontracts.HTTPResponseSuccess
	credentials        []string
	managedCredential  *runtimecontracts.ManagedCredentialRef
	rateLimit          externalDispatchRateLimitConfig
	mcpServerName      string
	mcpRemoteName      string
}

// ExecutionTool is an immutable runtime view derived from one admitted owner.
// It carries no authored or registry mutation authority.
type ExecutionTool struct {
	value *executionToolValue
}

func newExecutionTool(value executionToolValue) ExecutionTool {
	value.name = strings.TrimSpace(value.name)
	value.category = strings.TrimSpace(value.category)
	value.description = strings.TrimSpace(value.description)
	value.usage = strings.TrimSpace(value.usage)
	value.requiredPermission = strings.TrimSpace(value.requiredPermission)
	value.inputSchema = deepCloneMap(value.inputSchema)
	value.outputSchema = deepCloneMap(value.outputSchema)
	if value.http != nil {
		httpSpec := *value.http
		httpSpec.Headers = cloneRuntimeStringMap(value.http.Headers)
		httpSpec.Body = deepCloneJSONValue(value.http.Body)
		value.http = &httpSpec
	}
	value.responseMapping = deepCloneMap(value.responseMapping)
	value.responseSuccess = cloneResponseSuccess(value.responseSuccess)
	value.credentials = append([]string(nil), value.credentials...)
	value.managedCredential = cloneManagedCredentialRef(value.managedCredential)
	return ExecutionTool{value: &value}
}

func (t ExecutionTool) Name() string {
	if t.value == nil {
		return ""
	}
	return t.value.name
}
func (t ExecutionTool) Category() string {
	if t.value == nil {
		return ""
	}
	return t.value.category
}
func (t ExecutionTool) Description() string {
	if t.value == nil {
		return ""
	}
	return t.value.description
}
func (t ExecutionTool) Usage() string {
	if t.value == nil {
		return ""
	}
	return t.value.usage
}
func (t ExecutionTool) RequiredPermission() string {
	if t.value == nil {
		return ""
	}
	return t.value.requiredPermission
}
func (t ExecutionTool) Handler() implementationClass {
	if t.value == nil {
		return ""
	}
	return t.value.handler
}
func (t ExecutionTool) InputSchema() map[string]any {
	if t.value == nil {
		return nil
	}
	return deepCloneMap(t.value.inputSchema)
}
func (t ExecutionTool) OutputSchema() map[string]any {
	if t.value == nil {
		return nil
	}
	return deepCloneMap(t.value.outputSchema)
}
func (t ExecutionTool) GeneratedSchema() bool {
	return t.value != nil && t.value.generatedSchema
}
func (t ExecutionTool) HTTP() (*runtimecontracts.HTTPToolSpec, bool) {
	if t.value == nil || t.value.http == nil {
		return nil, false
	}
	snapshot := newExecutionTool(executionToolValue{http: t.value.http})
	return snapshot.value.http, true
}
func (t ExecutionTool) ResponseMapping() map[string]any {
	if t.value == nil {
		return nil
	}
	return deepCloneMap(t.value.responseMapping)
}
func (t ExecutionTool) ResponseSuccess() *runtimecontracts.HTTPResponseSuccess {
	if t.value == nil {
		return nil
	}
	return cloneResponseSuccess(t.value.responseSuccess)
}
func (t ExecutionTool) Credentials() []string {
	if t.value == nil {
		return nil
	}
	return append([]string(nil), t.value.credentials...)
}
func (t ExecutionTool) ManagedCredential() *runtimecontracts.ManagedCredentialRef {
	if t.value == nil {
		return nil
	}
	return cloneManagedCredentialRef(t.value.managedCredential)
}
func (t ExecutionTool) RateLimit() externalDispatchRateLimitConfig {
	if t.value == nil {
		return externalDispatchRateLimitConfig{}
	}
	return t.value.rateLimit
}
func (t ExecutionTool) MCPServerName() string {
	if t.value == nil {
		return ""
	}
	return t.value.mcpServerName
}
func (t ExecutionTool) MCPRemoteName() string {
	if t.value == nil {
		return ""
	}
	return t.value.mcpRemoteName
}
func (t ExecutionTool) withInputSchema(schema map[string]any) ExecutionTool {
	copyValue := *t.value
	copyValue.inputSchema = schema
	return newExecutionTool(copyValue)
}
func (t ExecutionTool) withOutputSchema(schema map[string]any) ExecutionTool {
	copyValue := *t.value
	copyValue.outputSchema = schema
	return newExecutionTool(copyValue)
}

func toolDefinitionsForRuntime(source semanticview.Source, discovered map[string]runtimemcp.DiscoveredTool) ([]llm.ToolDefinition, error) {
	entries, err := executionToolsForRuntime(source, discovered)
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
			Description:     entry.Description(),
			Usage:           entry.Usage(),
			Schema:          entry.InputSchema(),
			GeneratedSchema: entry.GeneratedSchema(),
		})
	}
	return defs, nil
}

func toolDefinitionsForActor(source semanticview.Source, actor models.AgentConfig, discovered map[string]runtimemcp.DiscoveredTool) ([]llm.ToolDefinition, error) {
	entries, err := executionToolsForActor(source, actor, discovered)
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
			Description:     entry.Description(),
			Usage:           entry.Usage(),
			Schema:          entry.InputSchema(),
			GeneratedSchema: entry.GeneratedSchema(),
		})
	}
	return defs, nil
}

func runtimeToolHiddenFromAgents(name string) bool {
	return false
}

func executionToolsForRuntime(source semanticview.Source, discovered map[string]runtimemcp.DiscoveredTool) (map[string]ExecutionTool, error) {
	entries := builtinExecutionTools(source, nil)
	if source != nil {
		for name, entry := range source.ToolEntries() {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			execution, include, err := executionToolFromContract(name, entry)
			if err != nil {
				return nil, err
			}
			if !include {
				continue
			}
			if existing, ok := entries[name]; ok && existing.Handler() == implementationPlatformBuiltin && execution.Handler() == implementationPlatformBuiltin {
				continue
			}
			entries[name] = execution
		}
	}
	for name, tool := range discovered {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		schema, _ := tool.InputSchema.(map[string]any)
		schema = deepCloneMap(schema)
		entries[name] = newExecutionTool(executionToolValue{
			name: name, description: tool.Description, handler: implementationMCP,
			inputSchema: schema, mcpServerName: tool.ServerName, mcpRemoteName: tool.RemoteName,
		})
	}
	return entries, nil
}

func executionToolsForActor(source semanticview.Source, actor models.AgentConfig, discovered map[string]runtimemcp.DiscoveredTool) (map[string]ExecutionTool, error) {
	entries, err := executionToolsForRuntime(source, discovered)
	if err != nil {
		return nil, err
	}
	removeLegacyEntityToolSurface(entries)
	for name, entry := range builtinExecutionTools(source, &actor) {
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
			execution, include, err := executionToolFromContract(strings.TrimSpace(name), entry)
			if err != nil {
				return nil, err
			}
			if !include {
				continue
			}
			if existing, ok := entries[strings.TrimSpace(name)]; ok && existing.Handler() == implementationPlatformBuiltin && execution.Handler() == implementationPlatformBuiltin {
				continue
			}
			entries[strings.TrimSpace(name)] = execution
		}
	}
	for _, def := range nativeFallbackToolDefinitionsForActor(actor) {
		name := strings.TrimSpace(def.Name)
		if name == "" {
			continue
		}
		tool, ok := nativeFallbackExecutionTool(actor, name)
		if !ok {
			continue
		}
		entries[name] = tool
	}
	removeLegacyEntityToolSurface(entries)
	return entries, nil
}

func resolveExecutionToolForActor(source semanticview.Source, actor models.AgentConfig, toolName string, discovered map[string]runtimemcp.DiscoveredTool) (ExecutionTool, bool, error) {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return ExecutionTool{}, false, nil
	}
	entries, err := executionToolsForActor(source, actor, discovered)
	if err != nil {
		return ExecutionTool{}, false, err
	}
	tool, ok := entries[toolName]
	if !ok {
		return ExecutionTool{}, false, nil
	}
	return tool, true, nil
}

func executionToolFromContract(name string, entry runtimecontracts.ToolSchemaEntry) (ExecutionTool, bool, error) {
	handlerType := normalizeImplementationClass(name, entry)
	if handlerType == "" {
		return ExecutionTool{}, false, nil
	}
	rateLimitSyntax, maxWaitSyntax := entry.RateLimitSyntax()
	rateLimit, enabled, err := parseExternalDispatchRateLimit(rateLimitSyntax, maxWaitSyntax)
	if err != nil {
		return ExecutionTool{}, false, err
	}
	if enabled && handlerType != implementationHTTP {
		return ExecutionTool{}, false, fmt.Errorf("tool %s rate_limit is only supported for handler_type http", strings.TrimSpace(name))
	}
	if handlerType == implementationMCP {
		return ExecutionTool{}, false, nil
	}
	inputSchema, err := schemaToMap(entry.InputSchema())
	if err != nil {
		return ExecutionTool{}, false, err
	}
	outputSchema, err := schemaToMap(entry.OutputSchema())
	if err != nil {
		return ExecutionTool{}, false, err
	}
	httpSpec, hasHTTP := entry.HTTP()
	var httpPtr *runtimecontracts.HTTPToolSpec
	if hasHTTP {
		httpPtr = &httpSpec
	}
	responseMapping, _ := entry.ResponseMapping()
	responseSuccess, hasResponseSuccess := entry.ResponseSuccess()
	var responseSuccessPtr *runtimecontracts.HTTPResponseSuccess
	if hasResponseSuccess {
		responseSuccessPtr = &responseSuccess
	}
	managed, hasManaged := entry.ManagedCredential()
	var managedPtr *runtimecontracts.ManagedCredentialRef
	if hasManaged {
		managedPtr = &managed
	}
	return newExecutionTool(executionToolValue{
		name: name, category: entry.Category(), description: entry.Description(),
		usage: runtimeOwnedToolUsage(name), requiredPermission: toolRequiredPermission(name, entry),
		handler: handlerType, inputSchema: inputSchema, outputSchema: outputSchema,
		http: httpPtr, responseMapping: responseMapping, responseSuccess: responseSuccessPtr,
		credentials: entry.Credentials(), managedCredential: managedPtr, rateLimit: rateLimit,
	}), true, nil
}

func cloneResponseSuccess(check *runtimecontracts.HTTPResponseSuccess) *runtimecontracts.HTTPResponseSuccess {
	if check == nil {
		return nil
	}
	out := *check
	return &out
}

func cloneManagedCredentialRef(ref *runtimecontracts.ManagedCredentialRef) *runtimecontracts.ManagedCredentialRef {
	if ref == nil {
		return nil
	}
	out := *ref
	out.Scopes = append([]string{}, ref.Scopes...)
	out.TokenRequest.StaticHeaders = cloneRuntimeStringMap(ref.TokenRequest.StaticHeaders)
	return &out
}

func normalizeImplementationClass(name string, entry runtimecontracts.ToolSchemaEntry) implementationClass {
	name = strings.TrimSpace(name)
	if name == "configure_routing" {
		return ""
	}
	switch entry.Handler() {
	case runtimecontracts.ToolHandlerUnspecified:
		if _, ok := supportedRuntimeToolNames[name]; ok {
			return implementationPlatformBuiltin
		}
		return ""
	case runtimecontracts.ToolHandlerPlatformBuiltin:
		return implementationPlatformBuiltin
	case runtimecontracts.ToolHandlerHTTP:
		return implementationHTTP
	case runtimecontracts.ToolHandlerMCP:
		return implementationMCP
	case runtimecontracts.ToolHandlerChannel:
		return implementationChannel
	default:
		return ""
	}
}

func toolRequiredPermission(toolID string, entry runtimecontracts.ToolSchemaEntry) string {
	if perm := strings.TrimSpace(entry.Permission()); perm != "" {
		return perm
	}
	if perm := strings.TrimSpace(entry.RequiredPermission()); perm != "" {
		return perm
	}
	return ""
}

func schemaToMap(schema runtimecontracts.ToolInputSchema) (map[string]any, error) {
	if schema.IsZero() {
		return nil, nil
	}
	return schema.Project()
}

func cloneRuntimeStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
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
