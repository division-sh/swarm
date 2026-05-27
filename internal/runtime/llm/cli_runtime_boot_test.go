package llm

import (
	"strings"
	"testing"

	"swarm/internal/config"
)

func TestValidateClaudeCLIRuntimeConfig_RequiresExplicitBridgeEnv(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"

	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")

	err := ValidateClaudeCLIRuntimeConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "SWARM_TOOL_GATEWAY_URL") {
		t.Fatalf("expected missing gateway URL error, got %v", err)
	}
}

func TestValidateClaudeCLIRuntimeConfig_RejectsRetiredRuntimeMode(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"

	if err := ValidateClaudeCLIRuntimeConfig(cfg); err == nil || !strings.Contains(err.Error(), "llm.runtime_mode is retired") {
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

	err := ValidateClaudeCLIRuntimeConfig(cfg)
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

	if err := ValidateClaudeCLIRuntimeConfig(cfg); err != nil {
		t.Fatalf("ValidateClaudeCLIRuntimeConfig: %v", err)
	}
}

func TestValidateClaudeCLIRuntimeConfig_RequiresExplicitContainerGatewayEnv(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"

	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	err := ValidateClaudeCLIRuntimeConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "SWARM_TOOL_GATEWAY_CONTAINER_URL") {
		t.Fatalf("expected missing container gateway URL error, got %v", err)
	}
}
