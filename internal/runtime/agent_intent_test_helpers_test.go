package runtime

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
			"agents.yaml#agents."+strings.TrimSpace(cfg.ID)+".intent",
			"Perform the runtime test agent's assigned work.",
		)
		if err != nil {
			t.Fatalf("resolve runtime test intent: %v", err)
		}
		cfg.Intent = resolved
	}
	if cfg.Prompt.Empty() {
		prompt, err := runtimeagentintent.IntentOnlyPrompt(cfg.Intent)
		if err != nil {
			t.Fatalf("derive runtime test prompt: %v", err)
		}
		cfg.Prompt = prompt
	}
	return cfg
}
