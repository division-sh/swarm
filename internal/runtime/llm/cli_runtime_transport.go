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
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

const (
	mcpContextTokenHeader = "X-SWARM-Context-Token"
)

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
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		authority, sandbox := runtimeeffects.AuthorityFromContext(ctx)
		if !sandbox || authority.Kind != runtimeeffects.AuthorityConversationForkChat {
			return MCPHTTPBinding{}, false, errors.New("mcp bridge requires exact managed capability surface")
		}
		allowed := conversationForkSandboxTransportSurfaceForActor(actor, s.Tools).RuntimeToolNames
		if len(allowed) == 0 {
			return MCPHTTPBinding{}, false, nil
		}
		return buildConversationForkSandboxMCPHTTPBinding(ctx, cfg, turns, gateway, endpoint, allowed)
	}
	if surface.ActorID != strings.TrimSpace(actor.ID) {
		return MCPHTTPBinding{}, false, errors.New("mcp bridge capability surface actor mismatch")
	}
	if len(surface.BindingNames(managedcapabilities.BindingMCPTool)) == 0 {
		return MCPHTTPBinding{}, false, nil
	}
	if surface.Authority.Kind == managedcapabilities.AuthorityProviderTurn {
		authority, ok := runtimeeffects.AuthorityFromContext(ctx)
		if !ok || !runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(authority.Target, surface) {
			return MCPHTTPBinding{}, false, errors.New("mcp bridge requires exact provider-turn usage target")
		}
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
	contextToken := turns.RegisterTurnContextWithCapabilitySurface(ctx, mcpContextTokenTTLForConfig(ctx, cfg), surface)
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

func buildConversationForkSandboxMCPHTTPBinding(ctx context.Context, cfg *config.Config, turns MCPTurnContextStore, gateway toolgateway.Binding, endpoint MCPGatewayEndpoint, allowed []string) (MCPHTTPBinding, bool, error) {
	if err := gateway.Validate(); err != nil {
		return MCPHTTPBinding{}, false, err
	}
	if !gateway.IsRuntimeOwned() {
		return MCPHTTPBinding{}, false, errors.New("forkchat MCP bridge requires a runtime-owned tool gateway binding")
	}
	serverURL := gateway.WorkspaceMCPURL()
	if endpoint == MCPGatewayHostEndpoint {
		serverURL = gateway.HostMCPURL()
	}
	if serverURL == "" {
		return MCPHTTPBinding{}, false, errors.New("forkchat MCP bridge gateway endpoint is invalid")
	}
	token := turns.RegisterConversationForkSandboxTurnContext(ctx, mcpContextTokenTTLForConfig(ctx, cfg), allowed)
	if strings.TrimSpace(token) == "" {
		return MCPHTTPBinding{}, false, errors.New("forkchat MCP turn context registration returned an empty token")
	}
	headers := map[string]string{"Authorization": "Bearer " + gateway.AuthToken(), mcpContextTokenHeader: token}
	return MCPHTTPBinding{URL: serverURL, Headers: headers, ContextToken: token, runtimeOwned: &runtimeOwnedMCPHTTPBinding{
		url: serverURL, authorization: headers["Authorization"], contextToken: token,
	}}, true, nil
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

func (r *ClaudeCLIRuntime) runWithPreparedPrompt(ctx context.Context, args []string, target *workspace.Target, prompt string, meta MonitorTurnMeta, attempt *runtimeeffects.Handle) (*Response, promptTransportFallback, error) {
	resp, err := r.runWithPreparedInput(ctx, args, target, prompt, meta, attempt)
	return resp, promptTransportFallback{}, err
}
