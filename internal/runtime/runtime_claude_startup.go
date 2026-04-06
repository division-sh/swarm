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
	runtimemanager "swarm/internal/runtime/manager"
	runtimemcp "swarm/internal/runtime/mcp"
	workspace "swarm/internal/runtime/workspace"
)

func validateClaudeStartupConfig(cfg *config.Config, opts RuntimeOptions) error {
	if err := llm.ValidateClaudeCLIRuntimeConfig(cfg); err != nil {
		return err
	}
	if cfg == nil || strings.TrimSpace(cfg.LLM.RuntimeMode) != "cli_test" {
		return nil
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

func validateClaudeManagedAgentWorkspaces(ctx context.Context, cfg *config.Config, workspaces workspace.Lifecycle, manager *runtimemanager.AgentManager) error {
	if cfg == nil || strings.TrimSpace(cfg.LLM.RuntimeMode) != "cli_test" {
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

func validateClaudeMCPToolsForManagedAgents(ctx context.Context, cfg *config.Config, turnStore llm.MCPTurnContextStore, tools claudeStartupToolSource, manager *runtimemanager.AgentManager) error {
	if cfg == nil || strings.TrimSpace(cfg.LLM.RuntimeMode) != "cli_test" {
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
		agentCtx := runtimeactors.WithActor(ctx, agentCfg)
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
		expectedNames, capabilities, err := expectedStartupMCPTools(agentCfg, tools, sessionTools)
		if err != nil {
			return fmt.Errorf("mcp startup probe expected tools for agent %s: %w", agentID, err)
		}
		if len(expectedNames) == 0 {
			return fmt.Errorf("mcp startup probe found no visible tools for agent %s", agentID)
		}
		actualNames := startupToolNames(actualTools)
		if !equalSortedStrings(actualNames, expectedNames) {
			return fmt.Errorf("mcp tools/list returned unexpected tool surface for agent %s: expected [%s], got [%s]", agentID, strings.Join(expectedNames, ", "), strings.Join(actualNames, ", "))
		}
		if probeName, ok := selectStartupCallableTool(actualTools, capabilities); ok {
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
	}
	return nil
}

func runtimeConfiguredMCPGatewayURL() string {
	return strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL"))
}

func runtimeConfiguredMCPGatewayToken() string {
	return strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN"))
}

func expectedStartupMCPTools(agentCfg runtimeactors.AgentConfig, tools claudeStartupToolSource, sessionTools []llm.ToolDefinition) ([]string, toolcapabilities.Set, error) {
	names := startupSessionToolNames(sessionTools)
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		allowed[name] = struct{}{}
	}
	caps := tools.ToolCapabilitiesForActor(agentCfg, names, allowed)
	if len(caps.ByName) == 0 {
		return nil, toolcapabilities.Set{}, fmt.Errorf("tool capability set is required")
	}
	visible := make([]string, 0, len(names))
	for _, name := range names {
		capability, ok := caps.Capability(name)
		if !ok {
			return nil, toolcapabilities.Set{}, fmt.Errorf("missing tool capability for %s", name)
		}
		if capability.Visible {
			visible = append(visible, name)
		}
	}
	sort.Strings(visible)
	return visible, caps, nil
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

func selectStartupCallableTool(tools []runtimemcp.ToolDef, capabilities toolcapabilities.Set) (string, bool) {
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		capability, ok := capabilities.Capability(name)
		if !ok || !capability.Callable || capability.Kind == toolcapabilities.KindEmit {
			continue
		}
		if !schemaRequiresObjectFields(tool.InputSchema) {
			continue
		}
		return name, true
	}
	return "", false
}

func schemaRequiresObjectFields(schema any) bool {
	typed, ok := schema.(map[string]any)
	if !ok {
		return false
	}
	if strings.TrimSpace(asString(typed["type"])) != "object" {
		return false
	}
	required, ok := typed["required"].([]any)
	if !ok || len(required) == 0 {
		return false
	}
	return true
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
	if isError, _ := result["isError"].(bool); !isError {
		return nil
	}
	message := strings.ToLower(strings.TrimSpace(startupMCPErrorText(result)))
	switch {
	case strings.Contains(message, "tool is not allowed"),
		strings.Contains(message, "tool executor unavailable"),
		strings.Contains(message, "tool name is required"),
		strings.Contains(message, "missing actor context"),
		strings.Contains(message, "missing authorization"):
		return fmt.Errorf(strings.TrimSpace(startupMCPErrorText(result)))
	default:
		return nil
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
