package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"

	"swarm/internal/config"
	llm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimetools "swarm/internal/runtime/tools"
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

func validateClaudeMCPToolsForManagedAgents(cfg *config.Config, gateway *runtimemcp.Gateway, toolGatewayToken string, manager *runtimemanager.AgentManager) error {
	if cfg == nil || strings.TrimSpace(cfg.LLM.RuntimeMode) != "cli_test" {
		return nil
	}
	if gateway == nil {
		return fmt.Errorf("tool gateway is required for claude cli runtime")
	}
	if manager == nil {
		return fmt.Errorf("agent manager is required for claude cli runtime")
	}
	token := strings.TrimSpace(toolGatewayToken)
	if token == "" {
		return fmt.Errorf("tool gateway token must be configured for claude cli runtime")
	}
	handler := gateway.Handler()
	for _, agentCfg := range manager.ListAgentConfigs() {
		agentID := strings.TrimSpace(agentCfg.ID)
		if agentID == "" {
			continue
		}
		reqBody, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      agentID,
			"method":  "tools/list",
		})
		if err != nil {
			return fmt.Errorf("encode mcp tools/list for agent %s: %w", agentID, err)
		}
		req := httptest.NewRequest(http.MethodPost, "/mcp?agent_id="+agentID, strings.NewReader(string(reqBody)))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return fmt.Errorf("mcp tools/list failed for agent %s: http %d", agentID, rec.Code)
		}
		var rpcResp struct {
			Result map[string]json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &rpcResp); err != nil {
			return fmt.Errorf("decode mcp tools/list for agent %s: %w", agentID, err)
		}
		if rpcResp.Error != nil {
			return fmt.Errorf("mcp tools/list failed for agent %s: %s", agentID, strings.TrimSpace(rpcResp.Error.Message))
		}
		rawTools, ok := rpcResp.Result["tools"]
		if !ok {
			return fmt.Errorf("mcp tools/list returned no tools payload for agent %s", agentID)
		}
		var tools []map[string]any
		if err := json.Unmarshal(rawTools, &tools); err != nil {
			return fmt.Errorf("decode mcp tools array for agent %s: %w", agentID, err)
		}
		if len(tools) == 0 {
			return fmt.Errorf("mcp tools/list returned no tools for agent %s", agentID)
		}
		toolNames := make(map[string]struct{}, len(tools))
		for _, tool := range tools {
			name := strings.TrimSpace(asString(tool["name"]))
			if name == "" {
				continue
			}
			toolNames[name] = struct{}{}
		}
		missingEmitTools := make([]string, 0)
		for _, eventType := range agentCfg.EmitEvents {
			name := runtimetools.EmitToolName(eventType)
			if _, ok := toolNames[name]; ok {
				continue
			}
			missingEmitTools = append(missingEmitTools, name)
		}
		if len(missingEmitTools) > 0 {
			sort.Strings(missingEmitTools)
			return fmt.Errorf("mcp tools/list missing required emit tools for agent %s: %s", agentID, strings.Join(missingEmitTools, ", "))
		}
	}
	return nil
}
