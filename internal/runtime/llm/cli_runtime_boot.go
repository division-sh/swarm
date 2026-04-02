package llm

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"swarm/internal/config"
)

func ValidateClaudeCLIRuntimeConfig(cfg *config.Config) error {
	if cfg == nil || strings.TrimSpace(cfg.LLM.RuntimeMode) != "cli_test" {
		return nil
	}
	if !shouldUseMCPBridge() {
		return fmt.Errorf("SWARM_CLAUDE_USE_MCP must remain enabled for claude cli runtime")
	}
	gatewayURL := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL"))
	if gatewayURL == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_URL is required for claude cli runtime")
	}
	normalized := normalizeMCPServerURL(gatewayURL)
	if normalized == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_URL must be a valid http(s) MCP gateway URL: %s", gatewayURL)
	}
	parsed, err := url.Parse(normalized)
	if err != nil || strings.TrimSpace(parsed.Host) == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_URL must be a valid http(s) MCP gateway URL: %s", gatewayURL)
	}
	if strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")) == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_TOKEN is required for claude cli runtime")
	}
	if strings.TrimSpace(os.Getenv("CLAUDE_CODE_OAUTH_TOKEN")) == "" {
		return fmt.Errorf("CLAUDE_CODE_OAUTH_TOKEN is required for claude cli runtime")
	}
	return nil
}
