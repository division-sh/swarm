package manager

import (
	"context"
	"testing"

	models "swarm/internal/runtime/core/actors"
)

func TestAgentManagerDefaultsLLMBackendFromCanonicalProfile(t *testing.T) {
	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{LLMBackend: "cli_test"})
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{
		Config: models.AgentConfig{
			ID:               "agent-1",
			Role:             "reviewer",
			ModelTier:        "sonnet",
			ConversationMode: "task",
		},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}
	got := am.agentCfg["agent-1"].LLMBackend
	if got != "cli_test" {
		t.Fatalf("llm_backend = %q, want cli_test", got)
	}
}
