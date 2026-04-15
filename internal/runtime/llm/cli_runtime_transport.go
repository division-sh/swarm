package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"time"

	"swarm/internal/config"
	models "swarm/internal/runtime/core/actors"
	workspace "swarm/internal/runtime/workspace"
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
}

func BuildMCPHTTPBinding(ctx context.Context, cfg *config.Config, turns MCPTurnContextStore, s *Session, gatewayURL string, gatewayToken string) (binding MCPHTTPBinding, enabled bool, err error) {
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
	serverURL := normalizeMCPServerURL(gatewayURL)
	if serverURL == "" {
		return MCPHTTPBinding{}, false, nil
	}
	headers := map[string]string{}
	if token := strings.TrimSpace(gatewayToken); token != "" {
		headers["Authorization"] = "Bearer " + token
	}
	contextToken := turns.RegisterTurnContextWithAllowedTools(ctx, mcpContextTokenTTLForConfig(ctx, cfg), cliTurnContextAllowedToolsForActor(actor, s.Tools))
	if contextToken != "" {
		headers[mcpContextTokenHeader] = contextToken
	}
	return MCPHTTPBinding{
		URL:          serverURL,
		Headers:      headers,
		ContextToken: contextToken,
	}, true, nil
}

func (r *ClaudeCLIRuntime) buildMCPConfigArg(ctx context.Context, s *Session) (configJSON string, contextToken string, enabled bool, err error) {
	gatewayURL := runtimeMCPGatewayURLForContainerExecution()
	binding, enabled, err := BuildMCPHTTPBinding(ctx, r.cfg, r.mcpTurns, s, gatewayURL, strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")))
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

func normalizeMCPServerURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || strings.TrimSpace(u.Scheme) == "" || strings.TrimSpace(u.Host) == "" {
		return ""
	}
	path := strings.TrimSpace(u.Path)
	switch path {
	case "", "/":
		u.Path = "/mcp"
	case "/mcp":
	default:
		// Respect explicit path when operator already targets a specific endpoint.
	}
	return strings.TrimSpace(u.String())
}

func runtimeMCPGatewayURLForContainerExecution() string {
	raw := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL"))
	return normalizeMCPServerURL(raw)
}

func RuntimeMCPGatewayURLForHostExecution() string {
	raw := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL"))
	return normalizeMCPServerURL(raw)
}

func (r *ClaudeCLIRuntime) runWithPromptTransportFallback(ctx context.Context, args []string, target *workspace.Target, prompt string, meta MonitorTurnMeta) (*Response, promptTransportFallback, error) {
	resp, err := r.runWithInput(ctx, args, target, prompt, meta)
	if err == nil || !isPromptArgRequiredError(err) {
		return resp, promptTransportFallback{}, err
	}
	used := promptTransportFallback{Attempted: true}
	resp, err = r.runWithPromptArg(ctx, args, target, prompt, meta)
	if err == nil {
		used.Used = true
		logPublisherRuntime(ctx, r.events, "warn", "prompt_transport_fallback_used", "CLI prompt transport fallback was used", "", "", "", nil, nil)
	}
	return resp, used, err
}
