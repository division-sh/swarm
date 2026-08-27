package tools

import (
	"errors"
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
	name                 string
	category             runtimecontracts.ToolCategory
	description          string
	usage                string
	requiredPermission   runtimecontracts.ToolPermission
	handler              runtimecontracts.ToolHandlerKind
	inputSchema          runtimecontracts.ToolInputSchema
	outputSchema         runtimecontracts.ToolInputSchema
	generatedSchema      bool
	http                 runtimecontracts.ToolHTTPExecution
	hasHTTP              bool
	responseMapping      runtimecontracts.ToolResponseMapping
	hasResponseMapping   bool
	responseSuccess      runtimecontracts.ToolResponseSuccessPolicy
	hasResponseSuccess   bool
	credentials          []string
	managedCredential    runtimecontracts.ToolManagedCredential
	hasManagedCredential bool
	ratePolicy           runtimecontracts.ToolRatePolicy
	mcp                  runtimecontracts.ToolMCPBinding
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
	return t.value.inputSchema.Projection()
}
func (t ExecutionTool) OutputSchema() map[string]any {
	if t.value == nil {
		return nil
	}
	return t.value.outputSchema.Projection()
}
func (t ExecutionTool) GeneratedSchema() bool {
	return t.value != nil && t.value.generatedSchema
}
func (t ExecutionTool) HTTP() (*runtimecontracts.HTTPToolSpec, bool) {
	if t.value == nil || !t.value.hasHTTP {
		return nil, false
	}
	spec := t.value.http.Readback()
	return &spec, true
}
func (t ExecutionTool) HTTPExecution() (runtimecontracts.ToolHTTPExecution, bool) {
	if t.value == nil || !t.value.hasHTTP {
		return runtimecontracts.ToolHTTPExecution{}, false
	}
	return t.value.http, true
}
func (t ExecutionTool) ResponseMapping() map[string]any {
	if t.value == nil || !t.value.hasResponseMapping {
		return nil
	}
	return t.value.responseMapping.Readback()
}
func (t ExecutionTool) CompiledResponseMapping() (runtimecontracts.ToolResponseMapping, bool) {
	if t.value == nil || !t.value.hasResponseMapping {
		return runtimecontracts.ToolResponseMapping{}, false
	}
	return t.value.responseMapping, true
}
func (t ExecutionTool) ResponseSuccess() *runtimecontracts.HTTPResponseSuccess {
	if t.value == nil || !t.value.hasResponseSuccess {
		return nil
	}
	out := t.value.responseSuccess.Readback()
	return &out
}
func (t ExecutionTool) ResponseSuccessPolicy() (runtimecontracts.ToolResponseSuccessPolicy, bool) {
	if t.value == nil || !t.value.hasResponseSuccess {
		return runtimecontracts.ToolResponseSuccessPolicy{}, false
	}
	return t.value.responseSuccess, true
}
func (t ExecutionTool) Credentials() []string {
	if t.value == nil {
		return nil
	}
	return append([]string(nil), t.value.credentials...)
}
func (t ExecutionTool) ManagedCredential() *runtimecontracts.ManagedCredentialRef {
	if t.value == nil || !t.value.hasManagedCredential {
		return nil
	}
	out := t.value.managedCredential.Readback()
	return &out
}
func (t ExecutionTool) ManagedCredentialExecution() (runtimecontracts.ToolManagedCredential, bool) {
	if t.value == nil || !t.value.hasManagedCredential {
		return runtimecontracts.ToolManagedCredential{}, false
	}
	return t.value.managedCredential, true
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
	switch strings.TrimSpace(name) {
	case "configure_routing":
		return true
	default:
		return false
	}
}

type executionToolOwner string

const (
	executionToolOwnerPlatformBuiltin executionToolOwner = "platform builtin"
	executionToolOwnerRoot            executionToolOwner = "root source declaration"
	executionToolOwnerDiscovered      executionToolOwner = "discovered MCP declaration"
	executionToolOwnerScoped          executionToolOwner = "scoped source declaration"
	executionToolOwnerNativeFallback  executionToolOwner = "native fallback"
)

func mergeExecutionTool(entries map[string]ExecutionTool, name string, execution ExecutionTool, include bool, owner executionToolOwner) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	canonicalPlatformBuiltin := owner == executionToolOwnerPlatformBuiltin && include && execution.Handler() == runtimecontracts.ToolHandlerPlatformBuiltin
	if err := hitlIdentityMergeError(name, string(owner), canonicalPlatformBuiltin); err != nil {
		return err
	}
	if !include {
		return nil
	}
	if existing, ok := entries[name]; ok && existing.Handler() == runtimecontracts.ToolHandlerPlatformBuiltin && execution.Handler() == runtimecontracts.ToolHandlerPlatformBuiltin {
		return nil
	}
	entries[name] = execution
	return nil
}

func executionToolsForRuntime(source semanticview.Source, discovered map[string]runtimemcp.DiscoveredTool) (map[string]ExecutionTool, error) {
	authoredErrors := append(ValidateRetiredDynamicAgentToolReferences(source), ValidateHITLIdentityLifecycleReferences(source)...)
	if len(authoredErrors) > 0 {
		return nil, errors.Join(authoredErrors...)
	}
	builtins, err := builtinExecutionTools(source, nil)
	if err != nil {
		return nil, err
	}
	entries := make(map[string]ExecutionTool, len(builtins))
	for name, execution := range builtins {
		if err := mergeExecutionTool(entries, name, execution, true, executionToolOwnerPlatformBuiltin); err != nil {
			return nil, err
		}
	}
	if source != nil {
		for name, entry := range source.ToolEntries() {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			execution, include := executionToolFromAdmitted(name, entry)
			if err := mergeExecutionTool(entries, name, execution, include, executionToolOwnerRoot); err != nil {
				return nil, err
			}
		}
	}
	discoveredNames := make([]string, 0, len(discovered))
	for name := range discovered {
		discoveredNames = append(discoveredNames, name)
	}
	sort.Strings(discoveredNames)
	for _, name := range discoveredNames {
		tool := discovered[name]
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		execution, include := executionToolFromAdmitted(name, tool.Contract)
		if err := mergeExecutionTool(entries, name, execution, include, executionToolOwnerDiscovered); err != nil {
			return nil, err
		}
		if !include {
			return nil, fmt.Errorf("discovered MCP tool %s has no admitted execution binding", name)
		}
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
		if err := mergeExecutionTool(entries, name, entry, true, executionToolOwnerPlatformBuiltin); err != nil {
			return nil, err
		}
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
		projection, projected := semanticview.ResolveAgentContractProjection(source, actor)
		for name := range candidates {
			if !projected {
				continue
			}
			entry, ok := projection.ToolEntry(name)
			if !ok {
				continue
			}
			execution, include := executionToolFromAdmitted(strings.TrimSpace(name), entry)
			if err := mergeExecutionTool(entries, name, execution, include, executionToolOwnerScoped); err != nil {
				return nil, err
			}
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
		if err := mergeExecutionTool(entries, name, tool, true, executionToolOwnerNativeFallback); err != nil {
			return nil, err
		}
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
	httpExecution, hasHTTP := entry.HTTPExecution()
	responseMapping, hasResponseMapping := entry.CompiledResponseMapping()
	responseSuccess, hasResponseSuccess := entry.ResponseSuccessPolicy()
	managed, hasManaged := entry.ManagedCredentialExecution()
	value := executionToolValue{
		name: name, category: entry.Category(), description: entry.Description(),
		usage: runtimeOwnedToolUsage(name), requiredPermission: entry.Permission(),
		handler: handlerType, inputSchema: entry.InputSchema(), outputSchema: entry.OutputSchema(),
		generatedSchema: entry.GeneratedSchema(),
		http:            httpExecution, hasHTTP: hasHTTP,
		responseMapping: responseMapping, hasResponseMapping: hasResponseMapping,
		responseSuccess: responseSuccess, hasResponseSuccess: hasResponseSuccess,
		credentials: entry.Credentials(), managedCredential: managed, hasManagedCredential: hasManaged,
		ratePolicy: entry.RatePolicy(),
		mcp:        mcpBinding,
	}
	return ExecutionTool{value: &value}, true
}

func deepCloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	cloned := deepCloneJSONValue(in)
	out, _ := cloned.(map[string]any)
	return out
}
