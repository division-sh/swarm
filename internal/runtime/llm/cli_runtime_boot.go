package llm

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"swarm/internal/config"
	llmselection "swarm/internal/runtime/llm/selection"
)

func ValidateClaudeCLIRuntimeConfig(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return err
	}
	if profile.ID != llmselection.BackendClaudeCLI {
		return nil
	}
	if !shouldUseMCPBridge() {
		return fmt.Errorf("SWARM_CLAUDE_USE_MCP must remain enabled for claude cli runtime")
	}
	hostGatewayURL := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL"))
	if hostGatewayURL == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_URL is required for claude cli runtime")
	}
	normalized := normalizeMCPServerURL(hostGatewayURL)
	if normalized == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_URL must be a valid http(s) MCP gateway URL: %s", hostGatewayURL)
	}
	parsed, err := url.Parse(normalized)
	if err != nil || strings.TrimSpace(parsed.Host) == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_URL must be a valid http(s) MCP gateway URL: %s", hostGatewayURL)
	}
	containerGatewayURL := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL"))
	if containerGatewayURL == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_CONTAINER_URL is required for claude cli runtime")
	}
	containerNormalized := normalizeMCPServerURL(containerGatewayURL)
	if containerNormalized == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_CONTAINER_URL must be a valid http(s) MCP gateway URL: %s", containerGatewayURL)
	}
	containerParsed, err := url.Parse(containerNormalized)
	if err != nil || strings.TrimSpace(containerParsed.Host) == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_CONTAINER_URL must be a valid http(s) MCP gateway URL: %s", containerGatewayURL)
	}
	if strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")) == "" {
		return fmt.Errorf("SWARM_TOOL_GATEWAY_TOKEN is required for claude cli runtime")
	}
	if err := llmselection.RequireCredential(profile, os.LookupEnv); err != nil {
		return err
	}
	return nil
}
