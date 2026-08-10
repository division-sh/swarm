package bus_test

import (
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func busTestAgentConfig(t testing.TB, cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	t.Helper()
	if strings.TrimSpace(cfg.ResolvedLLMBackend) == "" {
		cfg.ResolvedLLMBackend = "anthropic"
	}
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+strings.TrimSpace(cfg.ID)+".intent",
		"Process the event-bus test agent's assigned events.",
	)
	if err != nil {
		t.Fatalf("resolve event-bus test intent: %v", err)
	}
	cfg.Intent = intent
	prompt, err := runtimeagentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatalf("derive event-bus test prompt: %v", err)
	}
	cfg.Prompt = prompt
	return cfg
}
