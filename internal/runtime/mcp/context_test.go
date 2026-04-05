package mcp

import (
	"testing"
	"time"

	models "swarm/internal/runtime/core/actors"
)

func TestTurnContextRegistry_ResetIsScopedToRegistry(t *testing.T) {
	registryA := NewTurnContextRegistry(nil)
	registryB := NewTurnContextRegistry(nil)

	registryA.PutTurnContextForTest("ctx-shared", TurnContext{
		Actor:     models.AgentConfig{ID: "agent-a"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	registryB.PutTurnContextForTest("ctx-shared", TurnContext{
		Actor:     models.AgentConfig{ID: "agent-b"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	registryA.Reset()

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
