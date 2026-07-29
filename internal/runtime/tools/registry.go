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

type executionToolValue struct {
	name               string
	category           runtimecontracts.ToolCategory
	description        string
	usage              string
	requiredPermission runtimecontracts.ToolPermission
	handler            runtimecontracts.ToolHandlerKind
	inputSchema        map[string]any
	outputSchema       map[string]any
	generatedSchema    bool
	http               *runtimecontracts.HTTPToolSpec
	responseMapping    map[string]any
	responseSuccess    *runtimecontracts.HTTPResponseSuccess
	credentials        []string
	managedCredential  *runtimecontracts.ManagedCredentialRef
	ratePolicy         runtimecontracts.ToolRatePolicy
	mcp                runtimecontracts.ToolMCPBinding
}

// ExecutionTool is an immutable runtime view derived from one admitted owner.
// It carries no authored or registry mutation authority.
type ExecutionTool struct {
	value *executionToolValue
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
	return t.value.category.String()
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
	return t.value.requiredPermission.String()
}
func (t ExecutionTool) Handler() runtimecontracts.ToolHandlerKind {
	if t.value == nil {
		return runtimecontracts.ToolHandlerUnspecified
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
	spec := *t.value.http
	spec.Headers = cloneRuntimeStringMap(spec.Headers)
	spec.Body = deepCloneJSONValue(spec.Body)
	return &spec, true
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
	policy := t.value.ratePolicy
	return externalDispatchRateLimitConfig{
		Enabled: policy.Enabled(),
		Limit:   policy.Limit(),
		Period:  policy.Period(),
		MaxWait: policy.MaxWait(),
	}
}
func (t ExecutionTool) MCPServerName() string {
	if t.value == nil {
		return ""
	}
	return t.value.mcp.Server()
}
func (t ExecutionTool) MCPRemoteName() string {
	if t.value == nil {
		return ""
	}
	return t.value.mcp.Remote()
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
	return strings.TrimSpace(name) == "configure_routing"
}

func executionToolsForRuntime(source semanticview.Source, discovered map[string]runtimemcp.DiscoveredTool) (map[string]ExecutionTool, error) {
	entries, err := builtinExecutionTools(source, nil)
	if err != nil {
		return nil, err
	}
	if source != nil {
		for name, entry := range source.ToolEntries() {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			execution, include := executionToolFromAdmitted(name, entry)
			if !include {
				continue
			}
			if existing, ok := entries[name]; ok && existing.Handler() == runtimecontracts.ToolHandlerPlatformBuiltin && execution.Handler() == runtimecontracts.ToolHandlerPlatformBuiltin {
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
		execution, include := executionToolFromAdmitted(name, tool.Contract)
		if !include {
			return nil, fmt.Errorf("discovered MCP tool %s has no admitted execution binding", name)
		}
		entries[name] = execution
	}
	return entries, nil
}

func executionToolsForActor(source semanticview.Source, actor models.AgentConfig, discovered map[string]runtimemcp.DiscoveredTool) (map[string]ExecutionTool, error) {
	entries, err := executionToolsForRuntime(source, discovered)
	if err != nil {
		return nil, err
	}
	removeLegacyEntityToolSurface(entries)
	builtins, err := builtinExecutionTools(source, &actor)
	if err != nil {
		return nil, err
	}
	for name, entry := range builtins {
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
			execution, include := executionToolFromAdmitted(strings.TrimSpace(name), entry)
			if !include {
				continue
			}
			if existing, ok := entries[strings.TrimSpace(name)]; ok && existing.Handler() == runtimecontracts.ToolHandlerPlatformBuiltin && execution.Handler() == runtimecontracts.ToolHandlerPlatformBuiltin {
				continue
			}
			entries[strings.TrimSpace(name)] = execution
		}
	}
	for _, name := range []string{"bash", "web_search", "read_file", "write_file"} {
		tool, ok, err := nativeFallbackExecutionTool(actor, name)
		if err != nil {
			return nil, err
		}
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

func executionToolFromAdmitted(name string, entry runtimecontracts.ToolSchemaEntry) (ExecutionTool, bool) {
	handlerType := entry.Handler()
	if handlerType == runtimecontracts.ToolHandlerUnspecified {
		return ExecutionTool{}, false
	}
	mcpBinding, hasMCPBinding := entry.MCP()
	if handlerType == runtimecontracts.ToolHandlerMCP && !hasMCPBinding {
		return ExecutionTool{}, false
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
	value := executionToolValue{
		name: name, category: entry.Category(), description: entry.Description(),
		usage: runtimeOwnedToolUsage(name), requiredPermission: entry.Permission(),
		handler: handlerType, inputSchema: entry.InputSchema().Projection(), outputSchema: entry.OutputSchema().Projection(),
		generatedSchema: entry.GeneratedSchema(),
		http:            httpPtr, responseMapping: responseMapping, responseSuccess: responseSuccessPtr,
		credentials: entry.Credentials(), managedCredential: managedPtr, ratePolicy: entry.RatePolicy(),
		mcp: mcpBinding,
	}
	return ExecutionTool{value: &value}, true
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
