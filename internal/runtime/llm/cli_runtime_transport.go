package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"time"

	models "swarm/internal/runtime/core/actors"
	workspace "swarm/internal/runtime/workspace"
)

const (
	mcpActorIDHeader      = "X-SWARM-Agent-Id"
	mcpActorRoleHeader    = "X-SWARM-Agent-Role"
	mcpActorModeHeader    = "X-SWARM-Agent-Mode"
	mcpEntityIDHeader     = "X-SWARM-Entity-Id"
	mcpAllowedToolsHeader = "X-SWARM-Allowed-Tools"
	mcpContextTokenHeader = "X-SWARM-Context-Token"

	mcpActorIDQuery      = "agent_id"
	mcpActorRoleQuery    = "agent_role"
	mcpActorModeQuery    = "agent_mode"
	mcpEntityIDQuery     = "entity_id"
	mcpAllowedToolsQuery = "allowed_tools"
	mcpContextTokenQuery = "ctx_token"
)

func (r *ClaudeCLIRuntime) runWithPromptArg(ctx context.Context, args []string, target *workspace.Target, prompt string, meta MonitorTurnMeta) (*Response, error) {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return nil, errors.New("prompt argument fallback requires non-empty prompt")
	}
	runArgs := append(append([]string{}, args...), "--", prompt)
	return r.runWithInput(ctx, runArgs, target, "", meta)
}

func (r *ClaudeCLIRuntime) buildMCPConfigArg(ctx context.Context, s *Session) (configJSON string, contextToken string, enabled bool, err error) {
	if !shouldUseMCPBridge() || s == nil || len(s.Tools) == 0 {
		return "", "", false, nil
	}
	actor, _ := models.ActorFromContext(ctx)
	if strings.TrimSpace(actor.ID) == "" {
		actor.ID = strings.TrimSpace(s.AgentID)
	}
	if strings.TrimSpace(actor.Role) == "" {
		actor.Role = actor.ID
	}
	if strings.TrimSpace(actor.ID) == "" {
		return "", "", false, nil
	}
	if r.mcpTurns == nil {
		return "", "", false, errors.New("mcp turn context store is required for MCP bridge")
	}

	gatewayURL := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL"))
	if gatewayURL == "" {
		gatewayURL = "http://orchestrator:8090"
	}
	serverURL := normalizeMCPServerURL(gatewayURL)
	if serverURL == "" {
		return "", "", false, nil
	}
	allowedTools := toolNamesCSV(s.Tools)
	headers := map[string]string{
		mcpActorIDHeader:      strings.TrimSpace(actor.ID),
		mcpActorRoleHeader:    strings.TrimSpace(actor.Role),
		mcpEntityIDHeader:     strings.TrimSpace(actor.EffectiveEntityID()),
		mcpAllowedToolsHeader: allowedTools,
	}
	if mode := strings.TrimSpace(actor.Mode); mode != "" {
		headers[mcpActorModeHeader] = mode
	}
	if token := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")); token != "" {
		headers["Authorization"] = "Bearer " + token
	}
	contextToken = r.mcpTurns.RegisterTurnContextWithTTL(ctx, r.mcpContextTokenTTL(ctx))
	if contextToken != "" {
		headers[mcpContextTokenHeader] = contextToken
	}
	serverURL = withMCPContextQuery(serverURL, actor, contextToken, allowedTools)
	cfg := map[string]any{
		"mcpServers": map[string]any{
			"runtime-tools": map[string]any{
				"type":    "http",
				"url":     serverURL,
				"headers": headers,
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
	timeout := r.effectiveCLITimeout(ctx)
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

func withMCPContextQuery(rawURL string, actor models.AgentConfig, contextToken, allowedTools string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	q := u.Query()
	if v := strings.TrimSpace(contextToken); v != "" {
		q.Set(mcpContextTokenQuery, v)
	}
	if v := strings.TrimSpace(actor.ID); v != "" {
		q.Set(mcpActorIDQuery, v)
	}
	if v := strings.TrimSpace(actor.Role); v != "" {
		q.Set(mcpActorRoleQuery, v)
	}
	if v := strings.TrimSpace(actor.Mode); v != "" {
		q.Set(mcpActorModeQuery, v)
	}
	if v := strings.TrimSpace(actor.EffectiveEntityID()); v != "" {
		q.Set(mcpEntityIDQuery, v)
	}
	if v := strings.TrimSpace(allowedTools); v != "" {
		q.Set(mcpAllowedToolsQuery, v)
	}
	u.RawQuery = q.Encode()
	return strings.TrimSpace(u.String())
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
