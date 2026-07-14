package manager

import (
	"context"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestAgentManagerDefaultsLLMBackendFromCanonicalProfile(t *testing.T) {
	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{LLMBackend: "openai_compatible"})
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{
		Config: models.AgentConfig{
			ID:    "agent-1",
			Role:  "reviewer",
			Model: "regular",
		},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}
	cfg, ok := am.GetAgentConfig("agent-1")
	if !ok {
		t.Fatal("spawned agent config is absent")
	}
	got := cfg.LLMBackend
	if got != "openai_compatible" {
		t.Fatalf("llm_backend = %q, want openai_compatible", got)
	}
}
