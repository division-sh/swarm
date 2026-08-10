package runtimepersistence

import (
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func withRuntimePersistenceTestIntent(t testing.TB, cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	t.Helper()
	return runtimePersistenceTestAgentConfig(cfg)
}

func runtimePersistenceTestAgentConfig(cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	if strings.TrimSpace(cfg.ResolvedLLMBackend) == "" {
		cfg.ResolvedLLMBackend = strings.TrimSpace(cfg.LLMBackend)
		if cfg.ResolvedLLMBackend == "" {
			cfg.ResolvedLLMBackend = "anthropic"
		}
	}
	content := "Test intent for " + cfg.ID + "."
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+cfg.ID+".intent",
		content,
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
