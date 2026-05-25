package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"swarm/internal/config"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolcapabilities"
	llm "swarm/internal/runtime/llm"
	llmselection "swarm/internal/runtime/llm/selection"
	runtimemanager "swarm/internal/runtime/manager"
	runtimemcp "swarm/internal/runtime/mcp"
	"swarm/internal/runtime/semanticview"
	workspace "swarm/internal/runtime/workspace"
)

func validateClaudeStartupConfig(cfg *config.Config, opts RuntimeOptions, source semanticview.Source) error {
	if !isClaudeCLIBackend(cfg) {
		return nil
	}
	hasAgents, err := workflowSourceDeclaresAgents(source)
	if err != nil {
		return err
	}
	if !hasAgents {
		return nil
	}
	return validateClaudeStartupRequirements(cfg, opts)
}

func validateClaudeStartupConfigForActiveAgents(cfg *config.Config, opts RuntimeOptions, source semanticview.Source, manager *runtimemanager.AgentManager) error {
	if !isClaudeCLIBackend(cfg) {
		return nil
	}
	hasAgents, err := workflowSourceOrManagerDeclaresAgents(source, manager)
	if err != nil {
		return err
	}
	if !hasAgents {
		return nil
	}
	return validateClaudeStartupRequirements(cfg, opts)
}

func validateClaudeStartupRequirements(cfg *config.Config, opts RuntimeOptions) error {
	if err := llm.ValidateClaudeCLIRuntimeConfig(cfg); err != nil {
		return err
	}
	if opts.WorkspaceLifecycle == nil {
		return fmt.Errorf("workspace lifecycle is required for claude cli runtime")
	}
	if !opts.EnableToolGateway {
		return fmt.Errorf("tool gateway must be enabled for claude cli runtime")
	}
	if strings.TrimSpace(opts.ToolGatewayToken) == "" {
		return fmt.Errorf("tool gateway token must be configured for claude cli runtime")
	}
	return nil
}

func workflowSourceDeclaresAgents(source semanticview.Source) (bool, error) {
	if source == nil {
		return false, fmt.Errorf("semantic source is required for claude cli runtime")
	}
	return len(source.AgentEntries()) > 0, nil
}

func workflowSourceOrManagerDeclaresAgents(source semanticview.Source, manager *runtimemanager.AgentManager) (bool, error) {
	hasAgents, err := workflowSourceDeclaresAgents(source)
	if err != nil {
		return false, err
	}
	if hasAgents {
		return true, nil
	}
	if manager == nil {
		return false, nil
	}
	return len(manager.ListAgentConfigs()) > 0, nil
}

func validateClaudeManagedAgentWorkspaces(ctx context.Context, cfg *config.Config, source semanticview.Source, workspaces workspace.Lifecycle, manager *runtimemanager.AgentManager) error {
	if !isClaudeCLIBackend(cfg) {
		return nil
	}
	hasAgents, err := workflowSourceOrManagerDeclaresAgents(source, manager)
	if err != nil {
		return err
	}
	if !hasAgents {
		return nil
	}
	if workspaces == nil {
		return fmt.Errorf("workspace lifecycle is required for claude cli runtime")
	}
	if manager == nil {
		return fmt.Errorf("agent manager is required for claude cli runtime")
	}
	for _, agentCfg := range manager.ListAgentConfigs() {
		target, err := workspaces.ResolveWorkspace(ctx, agentCfg)
		if err != nil {
			return fmt.Errorf("resolve workspace for agent %s: %w", strings.TrimSpace(agentCfg.ID), err)
		}
		if target == nil || !target.Enabled() {
			return fmt.Errorf("agent %s resolved no container workspace target", strings.TrimSpace(agentCfg.ID))
		}
	}
	return nil
}

type claudeStartupToolSource interface {
	ToolDefinitionsForActor(runtimeactors.AgentConfig) []llm.ToolDefinition
	ToolCapabilitiesForActor(runtimeactors.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set
}

type claudeStartupContextAwareToolSource interface {
	ToolDefinitionsForActorInContext(context.Context, runtimeactors.AgentConfig) []llm.ToolDefinition
	ToolCapabilitiesForActorInContext(context.Context, runtimeactors.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set
}

func validateClaudeMCPToolsForManagedAgents(ctx context.Context, cfg *config.Config, source semanticview.Source, startupProbe llm.StartupVisibleToolSurfaceProber, turnStore llm.MCPTurnContextStore, tools claudeStartupToolSource, manager *runtimemanager.AgentManager) error {
	if !isClaudeCLIBackend(cfg) {
		return nil
	}
	hasAgents, err := workflowSourceOrManagerDeclaresAgents(source, manager)
	if err != nil {
		return err
	}
	if !hasAgents {
		return nil
	}
	if turnStore == nil {
		return fmt.Errorf("mcp turn context store is required for claude cli runtime")
	}
	if tools == nil {
		return fmt.Errorf("tool executor is required for claude cli runtime")
	}
	if manager == nil {
		return fmt.Errorf("agent manager is required for claude cli runtime")
	}
	gatewayURL := strings.TrimSpace(runtimeConfiguredMCPGatewayURL())
	if gatewayURL == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_URL is required for claude cli runtime")
	}
	gatewayToken := strings.TrimSpace(runtimeConfiguredMCPGatewayToken())
	if gatewayToken == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_TOKEN is required for claude cli runtime")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for _, agentCfg := range manager.ListAgentConfigs() {
		agentID := strings.TrimSpace(agentCfg.ID)
		if agentID == "" {
			continue
		}
		sessionTools := tools.ToolDefinitionsForActor(agentCfg)
		if len(sessionTools) == 0 {
			return fmt.Errorf("managed-agent startup probe found no declared tools for agent %s", agentID)
		}
		if startupProbe == nil {
			return fmt.Errorf("claude cli startup probe is required for agent %s", agentID)
		}
		probeResp, err := startupProbe.ProbeStartupVisibleToolSurface(ctx, agentCfg, runtimemanager.ExtractSystemPromptFromConfig(agentCfg.Config), sessionTools)
		if err != nil {
			return fmt.Errorf("claude cli startup probe failed for agent %s: %w", agentID, err)
		}
		surface := llm.AgentVisibleToolSurfaceForActor(agentCfg, sessionTools)
		if len(surface.NativeBuiltinTools) > 0 {
			expectedVisible := llm.PlannedProviderNativeVisibleToolsForActor(agentCfg, sessionTools)
			actualVisible := llm.ObservedProviderNativeVisibleToolsForActor(agentCfg, sessionTools, probeResp)
			if !equalSortedStrings(actualVisible, expectedVisible) {
				return fmt.Errorf("provider-native startup probe returned unexpected visible tool surface for agent %s: expected [%s], got [%s]", agentID, strings.Join(expectedVisible, ", "), strings.Join(actualVisible, ", "))
			}
		}
		if len(surface.RuntimeToolNames) == 0 {
			continue
		}
		agentCtx := runtimeactors.WithActor(ctx, agentCfg)
		expectedNames, capabilities, err := expectedStartupMCPTools(agentCtx, agentCfg, tools, sessionTools)
		if err != nil {
			return fmt.Errorf("mcp startup probe expected tools for agent %s: %w", agentID, err)
		}
		binding, enabled, err := llm.BuildMCPHTTPBinding(agentCtx, cfg, turnStore, &llm.Session{
			AgentID: agentID,
			Tools:   sessionTools,
		}, gatewayURL, gatewayToken)
		if err != nil {
			return fmt.Errorf("build mcp startup probe transport for agent %s: %w", agentID, err)
		}
		if !enabled {
			return fmt.Errorf("mcp startup probe transport is disabled for agent %s", agentID)
		}
		if strings.TrimSpace(binding.ContextToken) == "" {
			return fmt.Errorf("mcp startup probe missing turn context token for agent %s", agentID)
		}
		actualTools, err := startupProbeMCPToolsList(agentCtx, client, binding)
		turnStore.UnregisterTurnContext(binding.ContextToken)
		if err != nil {
			return fmt.Errorf("mcp tools/list startup probe failed for agent %s: %w", agentID, err)
		}
		actualNames := startupToolNames(actualTools)
		if !equalSortedStrings(actualNames, expectedNames) {
			return fmt.Errorf("mcp tools/list returned unexpected tool surface for agent %s: expected [%s], got [%s]", agentID, strings.Join(expectedNames, ", "), strings.Join(actualNames, ", "))
		}
		probeName, ok, err := selectStartupCallableTool(actualTools, capabilities)
		if err != nil {
			return fmt.Errorf("mcp startup probe select callable tool for agent %s: %w", agentID, err)
		}
		if !ok {
			continue
		}
		binding, enabled, err = llm.BuildMCPHTTPBinding(agentCtx, cfg, turnStore, &llm.Session{
			AgentID: agentID,
			Tools:   sessionTools,
		}, gatewayURL, gatewayToken)
		if err != nil {
			return fmt.Errorf("build mcp startup call probe transport for agent %s: %w", agentID, err)
		}
		if !enabled || strings.TrimSpace(binding.ContextToken) == "" {
			return fmt.Errorf("mcp startup call probe missing transport binding for agent %s", agentID)
		}
		err = startupProbeMCPToolsCall(agentCtx, client, binding, probeName)
		turnStore.UnregisterTurnContext(binding.ContextToken)
		if err != nil {
			return fmt.Errorf("mcp tools/call startup probe failed for agent %s tool %s: %w", agentID, probeName, err)
		}
	}
	return nil
}

func isClaudeCLIBackend(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}
	profile, err := llmselection.ResolveActiveBackend(cfg.LLM.Backend)
	return err == nil && profile.ID == llmselection.BackendCLITest
}

func runtimeConfiguredMCPGatewayURL() string {
	return strings.TrimSpace(llm.RuntimeMCPGatewayURLForHostExecution())
}

func runtimeConfiguredMCPGatewayToken() string {
	return strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN"))
}

func expectedStartupMCPTools(ctx context.Context, agentCfg runtimeactors.AgentConfig, tools claudeStartupToolSource, sessionTools []llm.ToolDefinition) ([]string, toolcapabilities.Set, error) {
	staticNames := llm.AgentVisibleToolSurfaceForActor(agentCfg, sessionTools).RuntimeToolNames
	if len(staticNames) == 0 {
		return nil, toolcapabilities.Set{}, nil
	}
	allowed := make(map[string]struct{}, len(staticNames))
	for _, name := range staticNames {
		allowed[name] = struct{}{}
	}
	staticCaps := tools.ToolCapabilitiesForActor(agentCfg, staticNames, allowed)
	staticVisible, err := visibleStartupCapabilityNames(staticNames, staticCaps)
	if err != nil {
		return nil, toolcapabilities.Set{}, err
	}
	if len(staticVisible) == 0 {
		return nil, toolcapabilities.Set{}, fmt.Errorf("found no visible tools")
	}

	concreteTools := sessionTools
	if contextAware, ok := tools.(claudeStartupContextAwareToolSource); ok {
		concreteTools = contextAware.ToolDefinitionsForActorInContext(ctx, agentCfg)
	}
	concreteNames := llm.AgentVisibleToolSurfaceForActor(agentCfg, concreteTools).RuntimeToolNames
	var concreteCaps toolcapabilities.Set
	if contextAware, ok := tools.(claudeStartupContextAwareToolSource); ok {
		concreteCaps = contextAware.ToolCapabilitiesForActorInContext(ctx, agentCfg, concreteNames, allowed)
	} else {
		concreteCaps = tools.ToolCapabilitiesForActor(agentCfg, concreteNames, allowed)
	}
	visible, err := visibleStartupCapabilityNames(concreteNames, concreteCaps)
	if err != nil {
		return nil, toolcapabilities.Set{}, err
	}
	sort.Strings(visible)
	return visible, concreteCaps, nil
}

func visibleStartupCapabilityNames(names []string, caps toolcapabilities.Set) ([]string, error) {
	visible := make([]string, 0, len(names))
	for _, name := range names {
		capability, ok := caps.Capability(name)
		if !ok {
			return nil, fmt.Errorf("missing tool capability for %s", name)
		}
		if capability.Visible {
			visible = append(visible, name)
		}
	}
	return visible, nil
}

func startupSessionToolNames(tools []llm.ToolDefinition) []string {
	names := make([]string, 0, len(tools))
	seen := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func startupToolNames(tools []runtimemcp.ToolDef) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		if name := strings.TrimSpace(tool.Name); name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func equalSortedStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if strings.TrimSpace(left[i]) != strings.TrimSpace(right[i]) {
			return false
		}
	}
	return true
}

func selectStartupCallableTool(tools []runtimemcp.ToolDef, capabilities toolcapabilities.Set) (string, bool, error) {
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		capability, ok := capabilities.Capability(name)
		if !ok || !capability.Callable || capability.Kind == toolcapabilities.KindEmit {
			continue
		}
		if capability.StartupProbeMode != toolcapabilities.StartupProbeModeCallEmptyObject {
			continue
		}
		if !schemaAllowsEmptyObjectArguments(tool.InputSchema) {
			return "", false, fmt.Errorf("tool %s is marked startup-probe call_empty_object but its schema requires arguments", name)
		}
		return name, true, nil
	}
	return "", false, nil
}

func schemaAllowsEmptyObjectArguments(schema any) bool {
	typed, ok := schema.(map[string]any)
	if !ok {
		return false
	}
	if schemaType := strings.TrimSpace(asString(typed["type"])); schemaType != "" && schemaType != "object" {
		return false
	}
	return len(schemaRequiredFieldNames(typed["required"])) == 0
}

func schemaRequiredFieldNames(raw any) []string {
	switch typed := raw.(type) {
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if name := strings.TrimSpace(asString(item)); name != "" {
				out = append(out, name)
			}
		}
		return out
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if name := strings.TrimSpace(item); name != "" {
				out = append(out, name)
			}
		}
		return out
	default:
		return nil
	}
}

func startupProbeMCPToolsList(ctx context.Context, client *http.Client, binding llm.MCPHTTPBinding) ([]runtimemcp.ToolDef, error) {
	if _, err := startupCallMCP(ctx, client, binding, runtimemcp.RPCRequest{
		JSONRPC: "2.0",
		Method:  "initialize",
		Params: map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "swarm-startup",
				"version": "1.0.0",
			},
		},
		ID: "startup-initialize",
	}); err != nil {
		return nil, err
	}
	if _, err := startupCallMCP(ctx, client, binding, runtimemcp.RPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
		Params:  map[string]any{},
	}); err != nil {
		return nil, err
	}
	resp, err := startupCallMCP(ctx, client, binding, runtimemcp.RPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/list",
		Params:  map[string]any{},
		ID:      "startup-tools-list",
	})
	if err != nil {
		return nil, err
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("tools/list returned invalid result payload")
	}
	rawTools, ok := result["tools"]
	if !ok {
		return nil, fmt.Errorf("tools/list returned no tools array")
	}
	encoded, err := json.Marshal(rawTools)
	if err != nil {
		return nil, err
	}
	var tools []runtimemcp.ToolDef
	if err := json.Unmarshal(encoded, &tools); err != nil {
		return nil, fmt.Errorf("decode tools/list payload: %w", err)
	}
	return tools, nil
}

func startupProbeMCPToolsCall(ctx context.Context, client *http.Client, binding llm.MCPHTTPBinding, name string) error {
	resp, err := startupCallMCP(ctx, client, binding, runtimemcp.RPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/call",
		Params: map[string]any{
			"name":      strings.TrimSpace(name),
			"arguments": map[string]any{},
			"swarmProbe": map[string]any{
				"contract": runtimemcp.StartupProbeContractManagedAgentCallable,
			},
		},
		ID: "startup-tools-call",
	})
	if err != nil {
		return err
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		return fmt.Errorf("tools/call returned invalid result payload")
	}
	probeResult, err := runtimemcp.DecodeStartupProbeResult(result["swarmStartupProbe"])
	if err != nil {
		return fmt.Errorf("tools/call returned invalid startup probe result: %w", err)
	}
	if probeResult.ToolName != strings.TrimSpace(name) {
		return fmt.Errorf("tools/call returned startup probe result for unexpected tool %q", probeResult.ToolName)
	}
	isError, _ := result["isError"].(bool)
	switch probeResult.Outcome {
	case runtimemcp.StartupProbeOutcomeSuccess, runtimemcp.StartupProbeOutcomeValidationOnly:
		if probeResult.Outcome == runtimemcp.StartupProbeOutcomeSuccess && isError {
			return fmt.Errorf("tools/call returned inconsistent startup probe success result")
		}
		if probeResult.Outcome == runtimemcp.StartupProbeOutcomeValidationOnly && !isError {
			return fmt.Errorf("tools/call returned inconsistent startup probe validation-only result")
		}
		return nil
	case runtimemcp.StartupProbeOutcomeExecutionFailure:
		if !isError {
			return fmt.Errorf("tools/call returned inconsistent startup probe execution-failure result")
		}
		if message := strings.TrimSpace(startupMCPErrorText(result)); message != "" {
			return fmt.Errorf(message)
		}
		runtimeErr, err := runtimemcp.DecodeRuntimeErrorPayload(result["runtimeError"])
		if err == nil && runtimeErr != nil {
			if strings.TrimSpace(runtimeErr.Message) != "" {
				return fmt.Errorf("%s", strings.TrimSpace(runtimeErr.Message))
			}
			return fmt.Errorf("tools/call returned runtime error code=%s", strings.TrimSpace(runtimeErr.Code))
		}
		if code := strings.TrimSpace(probeResult.RuntimeErrorCode); code != "" {
			return fmt.Errorf("tools/call returned runtime error code=%s", code)
		}
		return fmt.Errorf("tools/call returned startup probe execution failure")
	default:
		return fmt.Errorf("tools/call returned unsupported startup probe outcome %q", probeResult.Outcome)
	}
}

func startupMCPErrorText(result map[string]any) string {
	content, ok := result["content"].([]any)
	if !ok {
		return ""
	}
	parts := make([]string, 0, len(content))
	for _, item := range content {
		typed, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if text := strings.TrimSpace(asString(typed["text"])); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, " | ")
}

func startupCallMCP(ctx context.Context, client *http.Client, binding llm.MCPHTTPBinding, req runtimemcp.RPCRequest) (runtimemcp.RPCResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return runtimemcp.RPCResponse{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSpace(binding.URL), bytes.NewReader(body))
	if err != nil {
		return runtimemcp.RPCResponse{}, err
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("mcp-protocol-version", "2025-03-26")
	for key, value := range binding.Headers {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		httpReq.Header.Set(key, value)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return runtimemcp.RPCResponse{}, err
	}
	defer resp.Body.Close()
	var decoded runtimemcp.RPCResponse
	if resp.StatusCode >= 400 {
		return runtimemcp.RPCResponse{}, fmt.Errorf("mcp transport returned status %d", resp.StatusCode)
	}
	if req.Method == "notifications/initialized" {
		return decoded, nil
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return runtimemcp.RPCResponse{}, err
	}
	if decoded.Error != nil {
		return runtimemcp.RPCResponse{}, fmt.Errorf(strings.TrimSpace(decoded.Error.Message))
	}
	return decoded, nil
}
