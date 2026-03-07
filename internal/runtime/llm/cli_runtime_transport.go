package llm

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"empireai/internal/models"
	runtimeactor "empireai/internal/runtime/actorctx"
	workspace "empireai/internal/runtime/workspace"
)

func (r *ClaudeCLIRuntime) runWithPromptArg(ctx context.Context, args []string, target *workspace.Target, prompt string) (*Response, error) {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return nil, errors.New("prompt argument fallback requires non-empty prompt")
	}
	runArgs := append(append([]string{}, args...), "--", prompt)
	return r.runWithInput(ctx, runArgs, target, "")
}

func (r *ClaudeCLIRuntime) buildMCPConfigArg(ctx context.Context, s *Session) (configJSON string, contextToken string, enabled bool, err error) {
	if !shouldUseMCPBridge() || s == nil || len(s.Tools) == 0 {
		return "", "", false, nil
	}
	actor, _ := runtimeactor.ActorFromContext(ctx)
	if strings.TrimSpace(actor.ID) == "" {
		actor.ID = strings.TrimSpace(s.AgentID)
	}
	if strings.TrimSpace(actor.Mode) == "" {
		actor.Mode = "operating"
	}
	if strings.TrimSpace(actor.Role) == "" {
		actor.Role = actor.ID
	}
	if strings.TrimSpace(actor.ID) == "" {
		return "", "", false, nil
	}

	gatewayURL := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_URL"))
	if gatewayURL == "" {
		gatewayURL = "http://orchestrator:8090"
	}
	serverURL := normalizeMCPServerURL(gatewayURL)
	if serverURL == "" {
		return "", "", false, nil
	}
	allowedTools := toolNamesCSV(s.Tools)
	headers := map[string]string{
		"X-Empire-Agent-Id":      strings.TrimSpace(actor.ID),
		"X-Empire-Agent-Role":    strings.TrimSpace(actor.Role),
		"X-Empire-Agent-Mode":    strings.TrimSpace(actor.Mode),
		"X-Empire-Vertical-Id":   strings.TrimSpace(actor.VerticalID),
		"X-Empire-Allowed-Tools": allowedTools,
	}
	if token := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_TOKEN")); token != "" {
		headers["Authorization"] = "Bearer " + token
	}
	contextToken = mcpTurnContextRegister(ctx, r.mcpContextTokenTTL(ctx))
	traceID := strings.TrimSpace(contextToken)
	if contextToken != "" {
		headers["X-Empire-Context-Token"] = contextToken
	}
	if traceID != "" {
		headers["X-Empire-Trace-Id"] = traceID
	}
	serverURL = withMCPContextQuery(serverURL, actor, contextToken, allowedTools, traceID)
	cfg := map[string]any{
		"mcpServers": map[string]any{
			"empire-runtime": map[string]any{
				"type":    "http",
				"url":     serverURL,
				"headers": headers,
			},
		},
	}
	raw, marshalErr := json.Marshal(cfg)
	if marshalErr != nil {
		if contextToken != "" {
			mcpTurnContextUnregister(contextToken)
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
	v := strings.TrimSpace(strings.ToLower(os.Getenv("EMPIREAI_CLAUDE_USE_MCP")))
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

func withMCPContextQuery(rawURL string, actor models.AgentConfig, contextToken, allowedTools, traceID string) string {
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
		q.Set("empire_ctx_token", v)
	}
	if v := strings.TrimSpace(actor.ID); v != "" {
		q.Set("empire_agent_id", v)
	}
	if v := strings.TrimSpace(actor.Role); v != "" {
		q.Set("empire_agent_role", v)
	}
	if v := strings.TrimSpace(actor.Mode); v != "" {
		q.Set("empire_agent_mode", v)
	}
	if v := strings.TrimSpace(actor.VerticalID); v != "" {
		q.Set("empire_vertical_id", v)
	}
	if v := strings.TrimSpace(allowedTools); v != "" {
		q.Set("empire_allowed_tools", v)
	}
	if v := strings.TrimSpace(traceID); v != "" {
		q.Set("empire_trace_id", v)
	}
	u.RawQuery = q.Encode()
	return strings.TrimSpace(u.String())
}

func (r *ClaudeCLIRuntime) runWithPromptTransportFallback(ctx context.Context, args []string, target *workspace.Target, prompt string) (*Response, promptTransportFallback, error) {
	resp, err := r.runWithInput(ctx, args, target, prompt)
	if err == nil || !isPromptArgRequiredError(err) {
		return resp, promptTransportFallback{}, err
	}
	used := promptTransportFallback{Attempted: true}
	resp, err = r.runWithPromptArg(ctx, args, target, prompt)
	if err == nil {
		used.Used = true
		log.Printf("claude cli transport fallback: switched to prompt argument mode")
	}
	return resp, used, err
}
