package serveapp

import (
	"strings"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func serveTestAgentConfig(cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	if strings.TrimSpace(cfg.ResolvedLLMBackend) == "" {
		backend := strings.TrimSpace(cfg.LLMBackend)
		if backend == "" {
			backend = "anthropic"
		}
		profile, err := selection.ResolveLiveBackend(backend)
		if err != nil {
			panic(err)
		}
		selected, err := runtimellm.ResolveAgentExecution(executionposture.Live, profile, nil, cfg)
		if err != nil {
			panic(err)
		}
		cfg = selected.Actor
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
