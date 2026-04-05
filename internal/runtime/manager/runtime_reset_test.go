package manager

import (
	"testing"
	"time"

	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemcp "swarm/internal/runtime/mcp"
)

func TestResetRuntimeState_OnlyResetsOwnedTurnContextRegistry(t *testing.T) {
	registryA := runtimemcp.NewTurnContextRegistry(nil)
	registryB := runtimemcp.NewTurnContextRegistry(nil)

	registryA.PutTurnContextForTest("ctx-shared", runtimemcp.TurnContext{
		Actor:     runtimeactors.AgentConfig{ID: "agent-a"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	registryB.PutTurnContextForTest("ctx-shared", runtimemcp.TurnContext{
		Actor:     runtimeactors.AgentConfig{ID: "agent-b"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{
		ResetRuntimeOwnedState: registryA.Reset,
	})
	if err := am.ResetRuntimeState(); err != nil {
		t.Fatalf("ResetRuntimeState: %v", err)
	}

	if _, ok := registryA.ResolveTurnContext("ctx-shared"); ok {
		t.Fatal("registryA should be empty after reset")
	}
	turn, ok := registryB.ResolveTurnContext("ctx-shared")
	if !ok {
		t.Fatal("registryB should retain its turn context")
	}
	if turn.Actor.ID != "agent-b" {
		t.Fatalf("registryB actor id = %q, want agent-b", turn.Actor.ID)
	}
}
