package llm

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

const (
	mcpContextTokenHeader = "X-SWARM-Context-Token"
)

func (r *ClaudeCLIRuntime) runWithPromptArg(ctx context.Context, args []string, target *workspace.Target, prompt string, meta MonitorTurnMeta) (*Response, error) {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return nil, errors.New("prompt argument fallback requires non-empty prompt")
	}
	runArgs := append(append([]string{}, args...), "--", prompt)
	return r.runWithInput(ctx, runArgs, target, "", meta)
}

type MCPHTTPBinding struct {
	URL          string
	Headers      map[string]string
	ContextToken string
	runtimeOwned *runtimeOwnedMCPHTTPBinding
}

type runtimeOwnedMCPHTTPBinding struct {
	url           string
	authorization string
	contextToken  string
}

type MCPGatewayEndpoint string

const (
	MCPGatewayHostEndpoint      MCPGatewayEndpoint = "host"
	MCPGatewayWorkspaceEndpoint MCPGatewayEndpoint = "workspace"
)

func (b MCPHTTPBinding) IsRuntimeOwned() bool {
	return b.runtimeOwned != nil &&
		b.URL == b.runtimeOwned.url &&
		b.Headers["Authorization"] == b.runtimeOwned.authorization &&
		b.Headers[mcpContextTokenHeader] == b.runtimeOwned.contextToken &&
		b.ContextToken == b.runtimeOwned.contextToken
}

func BuildMCPHTTPBinding(ctx context.Context, cfg *config.Config, turns MCPTurnContextStore, s *Session, gateway toolgateway.Binding, endpoint MCPGatewayEndpoint) (binding MCPHTTPBinding, enabled bool, err error) {
	if !shouldUseMCPBridge() || s == nil || len(s.Tools) == 0 {
		return MCPHTTPBinding{}, false, nil
	}
	actor, _ := models.ActorFromContext(ctx)
	if strings.TrimSpace(actor.ID) == "" {
		actor.ID = strings.TrimSpace(s.AgentID)
	}
	if strings.TrimSpace(actor.ID) == "" {
		return MCPHTTPBinding{}, false, nil
	}
	if turns == nil {
		return MCPHTTPBinding{}, false, errors.New("mcp turn context store is required for MCP bridge")
	}
	allowedTools := cliTurnContextAllowedToolsForActor(actor, s.Tools)
	if len(allowedTools) == 0 {
		return MCPHTTPBinding{}, false, nil
	}
	if err := gateway.Validate(); err != nil {
		return MCPHTTPBinding{}, false, err
	}
	if !gateway.IsRuntimeOwned() {
		return MCPHTTPBinding{}, false, errors.New("mcp bridge requires a runtime-owned tool gateway binding")
	}
	var serverURL string
	switch endpoint {
	case MCPGatewayHostEndpoint:
		serverURL = gateway.HostMCPURL()
	case MCPGatewayWorkspaceEndpoint:
		serverURL = gateway.WorkspaceMCPURL()
	default:
		return MCPHTTPBinding{}, false, errors.New("mcp bridge gateway endpoint is required")
	}
	if serverURL == "" {
		return MCPHTTPBinding{}, false, errors.New("mcp bridge gateway endpoint is invalid")
	}
	headers := map[string]string{"Authorization": "Bearer " + gateway.AuthToken()}
	contextToken := turns.RegisterTurnContextWithAllowedTools(ctx, mcpContextTokenTTLForConfig(ctx, cfg), allowedTools)
	if strings.TrimSpace(contextToken) == "" {
		return MCPHTTPBinding{}, false, errors.New("mcp turn context registration returned an empty token")
	}
	headers[mcpContextTokenHeader] = contextToken
	authorization := headers["Authorization"]
	return MCPHTTPBinding{
		URL:          serverURL,
		Headers:      headers,
		ContextToken: contextToken,
		runtimeOwned: &runtimeOwnedMCPHTTPBinding{
			url:           serverURL,
			authorization: authorization,
			contextToken:  contextToken,
		},
	}, true, nil
}

func (r *ClaudeCLIRuntime) buildMCPConfigArg(ctx context.Context, s *Session) (configJSON string, contextToken string, enabled bool, err error) {
	binding, enabled, err := BuildMCPHTTPBinding(ctx, r.cfg, r.mcpTurns, s, r.toolGateway, MCPGatewayWorkspaceEndpoint)
	if err != nil || !enabled {
		return "", "", enabled, err
	}
	contextToken = binding.ContextToken
	cfg := map[string]any{
		"mcpServers": map[string]any{
			"runtime-tools": map[string]any{
				"type":    "http",
				"url":     binding.URL,
				"headers": binding.Headers,
			},
		},
	}
	raw, marshalErr := json.Marshal(cfg)
	if marshalErr != nil {
		if contextToken != "" {
			r.mcpTurns.UnregisterTurnContext(contextToken)
			contextToken = ""
		}
		return "", "", false, marshalErr
	}
	return string(raw), contextToken, true, nil
}

func (r *ClaudeCLIRuntime) mcpContextTokenTTL(ctx context.Context) time.Duration {
	return mcpContextTokenTTLForConfig(ctx, r.cfg)
}

func mcpContextTokenTTLForConfig(ctx context.Context, cfg *config.Config) time.Duration {
	timeout := effectiveCLITimeoutForConfig(ctx, cfg)
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	ttl := timeout * 3
	const (
		minTTL = 45 * time.Minute
		maxTTL = 6 * time.Hour
	)
	if ttl < minTTL {
		ttl = minTTL
	}
	if ttl > maxTTL {
		ttl = maxTTL
	}
	return ttl
}

func shouldUseMCPBridge() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("SWARM_CLAUDE_USE_MCP")))
	if v == "" {
		return true
	}
	if v == "0" || v == "false" || v == "no" {
		return false
	}
	return v == "1" || v == "true" || v == "yes"
}

func (r *ClaudeCLIRuntime) runWithPromptTransportFallback(ctx context.Context, args []string, target *workspace.Target, prompt string, meta MonitorTurnMeta) (*Response, promptTransportFallback, error) {
	resp, err := r.runWithInput(ctx, args, target, prompt, meta)
	return resp, promptTransportFallback{}, err
}

func (r *ClaudeCLIRuntime) runWithPreparedPrompt(ctx context.Context, args []string, target *workspace.Target, prompt string, meta MonitorTurnMeta, attempt *runtimeeffects.Handle) (*Response, promptTransportFallback, error) {
	resp, err := r.runWithPreparedInput(ctx, args, target, prompt, meta, attempt)
	return resp, promptTransportFallback{}, err
}
