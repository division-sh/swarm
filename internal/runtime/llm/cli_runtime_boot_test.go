package llm

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
)

func testProviderCredentialResolver(t *testing.T, key, value string) ProviderCredentialResolver {
	t.Helper()
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if strings.TrimSpace(value) != "" {
		if err := store.Set(context.Background(), key, value); err != nil {
			t.Fatalf("Set provider credential: %v", err)
		}
	}
	return NewProviderCredentialResolver(store)
}

func TestValidateClaudeCLIRuntimeConfig_RequiresToolGatewayBinding(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"

	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")

	err := ValidateClaudeCLIRuntimeConfig(context.Background(), cfg, testToolGatewayBinding("", "", ""), ProviderCredentialResolver{})
	if err == nil || !strings.Contains(err.Error(), "tool gateway binding") {
		t.Fatalf("expected missing gateway binding error, got %v", err)
	}
}

func TestValidateClaudeCLIRuntimeConfig_RejectsRetiredRuntimeMode(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"

	if err := ValidateClaudeCLIRuntimeConfig(context.Background(), cfg, toolgateway.Binding{}, ProviderCredentialResolver{}); err == nil || !strings.Contains(err.Error(), "llm.runtime_mode is retired") {
		t.Fatalf("ValidateClaudeCLIRuntimeConfig error = %v, want retired runtime mode rejection", err)
	}
}

func TestValidateClaudeCLIRuntimeConfig_RequiresMCPBridgeEnabled(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"

	t.Setenv("SWARM_CLAUDE_USE_MCP", "0")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	err := ValidateClaudeCLIRuntimeConfig(context.Background(), cfg, testToolGatewayBinding("http://127.0.0.1:8081", "http://host.docker.internal:8081", "gateway-token"), testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token"))
	if err == nil || !strings.Contains(err.Error(), "SWARM_CLAUDE_USE_MCP") {
		t.Fatalf("expected MCP enabled error, got %v", err)
	}
}

func TestValidateClaudeCLIRuntimeConfig_AcceptsExplicitConfig(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"

	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	if err := ValidateClaudeCLIRuntimeConfig(context.Background(), cfg, testToolGatewayBinding("http://127.0.0.1:8081", "http://host.docker.internal:8081", "gateway-token"), testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")); err != nil {
		t.Fatalf("ValidateClaudeCLIRuntimeConfig: %v", err)
	}
}

func TestValidateClaudeCLIRuntimeConfig_RequiresWorkspaceGatewayEndpoint(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"

	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	err := ValidateClaudeCLIRuntimeConfig(context.Background(), cfg, testToolGatewayBinding("http://127.0.0.1:8081", "", "gateway-token"), testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token"))
	if err == nil || !strings.Contains(err.Error(), "workspace endpoint") {
		t.Fatalf("expected missing workspace endpoint error, got %v", err)
	}
}
