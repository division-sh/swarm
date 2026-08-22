package serveapp

import (
	"strings"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func serveTestAgentConfig(cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	if strings.TrimSpace(cfg.ResolvedLLMBackend) == "" {
		cfg.ResolvedLLMBackend = strings.TrimSpace(cfg.LLMBackend)
		if cfg.ResolvedLLMBackend == "" {
			cfg.ResolvedLLMBackend = "anthropic"
		}
	}
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+strings.TrimSpace(cfg.ID)+".intent",
		"Perform the serve-runtime test agent's assigned work.",
	)
	if err != nil {
		panic(err)
	}
	cfg.Intent = intent
	prompt, err := runtimeagentintent.IntentOnlyPrompt(intent)
	if err != nil {
		panic(err)
	}
	cfg.Prompt = prompt
	return cfg
}
