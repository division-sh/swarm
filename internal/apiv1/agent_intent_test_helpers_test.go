package apiv1

import (
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func withAPITestIntent(t testing.TB, cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	t.Helper()
	backend := strings.TrimSpace(cfg.LLMBackend)
	if backend == "" {
		backend = llmselection.BackendAnthropic
	}
	profile, err := llmselection.ResolveLiveBackend(backend)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := runtimellm.ResolveAgentExecution(executionposture.Live, profile, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg = selected.Actor
	content := "Test intent for " + cfg.ID + "."
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+cfg.ID+".intent",
		content,
	)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Intent = intent
	prompt, err := runtimeagentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Prompt = prompt
	return cfg
}

func apiTestResolvedIntent(t testing.TB, agentID, content string) runtimeagentintent.Resolved {
	t.Helper()
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+agentID+".intent",
		content,
	)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}
