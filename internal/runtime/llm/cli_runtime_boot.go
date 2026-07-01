package llm

import (
	"fmt"
	"os"

	"github.com/division-sh/swarm/internal/config"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
)

func ValidateClaudeCLIRuntimeConfig(cfg *config.Config, binding toolgateway.Binding) error {
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
	if err := binding.Validate(); err != nil {
		return fmt.Errorf("claude cli tool gateway binding invalid: %w", err)
	}
	if err := llmselection.RequireCredential(profile, os.LookupEnv); err != nil {
		return err
	}
	return nil
}
