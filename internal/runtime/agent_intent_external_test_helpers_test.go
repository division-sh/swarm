package runtime_test

import (
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func runtimeTestAgentConfig(t testing.TB, cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	t.Helper()
	if strings.TrimSpace(cfg.ResolvedLLMBackend) == "" {
		cfg.ResolvedLLMBackend = strings.TrimSpace(cfg.LLMBackend)
		if cfg.ResolvedLLMBackend == "" {
			cfg.ResolvedLLMBackend = "anthropic"
		}
	}
	if cfg.Intent.Empty() {
		resolved, err := runtimeagentintent.Resolve(
			runtimeagentintent.SourceInline,
			"inline",
			"test#agents."+strings.TrimSpace(cfg.ID)+".intent",
			"Perform the runtime supported-surface test agent's assigned work.",
		)
		if err != nil {
			t.Fatalf("resolve runtime supported-surface test intent: %v", err)
		}
		cfg.Intent = resolved
	}
	if strings.TrimSpace(cfg.SystemPrompt) == "" {
		cfg.SystemPrompt = cfg.Intent.Content
	}
	return cfg
}
