package llm

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/config"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
)

func ValidateClaudeCLIRuntimeConfig(ctx context.Context, cfg *config.Config, binding toolgateway.Binding, credentials ProviderCredentialResolver) error {
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
	if _, err := credentials.Resolve(ctx, profile); err != nil {
		return err
	}
	return nil
}
